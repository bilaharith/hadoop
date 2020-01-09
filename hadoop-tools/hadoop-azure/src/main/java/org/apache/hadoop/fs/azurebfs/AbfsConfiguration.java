/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.IntegerConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.LongConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.Base64StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.BooleanConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConfigurationPropertyNotFoundException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.diagnostics.Base64StringConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.BooleanConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.IntegerConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.LongConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.StringConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.extensions.AbfsAuthorizationException;
import org.apache.hadoop.fs.azurebfs.extensions.AbfsAuthorizer;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.CustomTokenProviderAdapter;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.RefreshTokenBasedTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.UserPasswordTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.KeyProvider;
import org.apache.hadoop.fs.azurebfs.services.SimpleKeyProvider;
import org.apache.hadoop.fs.azurebfs.utils.SSLSocketFactoryEx;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;

/**
 * Configuration for Azure Blob FileSystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AbfsConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsConfiguration.class);

  private final Configuration rawConfig;
  private final String accountName;
  private final boolean isSecure;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_WRITE_BUFFER_SIZE,
      MinValue = MIN_BUFFER_SIZE,
      MaxValue = MAX_BUFFER_SIZE,
      DefaultValue = DEFAULT_WRITE_BUFFER_SIZE)
  private int writeBufferSize;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_READ_BUFFER_SIZE,
      MinValue = MIN_BUFFER_SIZE,
      MaxValue = MAX_BUFFER_SIZE,
      DefaultValue = DEFAULT_READ_BUFFER_SIZE)
  private int readBufferSize;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_MIN_BACKOFF_INTERVAL,
      DefaultValue = DEFAULT_MIN_BACKOFF_INTERVAL)
  private int minBackoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_MAX_BACKOFF_INTERVAL,
      DefaultValue = DEFAULT_MAX_BACKOFF_INTERVAL)
  private int maxBackoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_BACKOFF_INTERVAL,
      DefaultValue = DEFAULT_BACKOFF_INTERVAL)
  private int backoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_MAX_IO_RETRIES,
      MinValue = 0,
      DefaultValue = DEFAULT_MAX_RETRY_ATTEMPTS)
  private int maxIoRetries;

  @LongConfigurationValidatorAnnotation(ConfigurationKey = AZURE_BLOCK_SIZE_PROPERTY_NAME,
      MinValue = 0,
      MaxValue = MAX_AZURE_BLOCK_SIZE,
      DefaultValue = MAX_AZURE_BLOCK_SIZE)
  private long azureBlockSize;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME,
      DefaultValue = AZURE_BLOCK_LOCATION_HOST_DEFAULT)
  private String azureBlockLocationHost;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_CONCURRENT_CONNECTION_VALUE_OUT,
      MinValue = 1,
      DefaultValue = MAX_CONCURRENT_WRITE_THREADS)
  private int maxConcurrentWriteThreads;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_CONCURRENT_CONNECTION_VALUE_IN,
      MinValue = 1,
      DefaultValue = MAX_CONCURRENT_READ_THREADS)
  private int maxConcurrentReadThreads;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = AZURE_TOLERATE_CONCURRENT_APPEND,
      DefaultValue = DEFAULT_READ_TOLERATE_CONCURRENT_APPEND)
  private boolean tolerateOobAppends;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ATOMIC_RENAME_KEY,
      DefaultValue = DEFAULT_FS_AZURE_ATOMIC_RENAME_DIRECTORIES)
  private String azureAtomicDirs;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
      DefaultValue = DEFAULT_AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION)
  private boolean createRemoteFileSystemDuringInitialization;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION,
      DefaultValue = DEFAULT_AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION)
  private boolean skipUserGroupMetadataDuringInitialization;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_READ_AHEAD_QUEUE_DEPTH,
      DefaultValue = DEFAULT_READ_AHEAD_QUEUE_DEPTH)
  private int readAheadQueueDepth;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_FLUSH,
      DefaultValue = DEFAULT_ENABLE_FLUSH)
  private boolean enableFlush;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_DISABLE_OUTPUTSTREAM_FLUSH,
      DefaultValue = DEFAULT_DISABLE_OUTPUTSTREAM_FLUSH)
  private boolean disableOutputStreamFlush;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_AUTOTHROTTLING,
      DefaultValue = DEFAULT_ENABLE_AUTOTHROTTLING)
  private boolean enableAutoThrottling;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_USER_AGENT_PREFIX_KEY,
      DefaultValue = "")
  private String userAgentId;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_DELEGATION_TOKEN,
      DefaultValue = DEFAULT_ENABLE_DELEGATION_TOKEN)
  private boolean enableDelegationToken;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = ABFS_EXTERNAL_AUTHORIZATION_CLASS,
      DefaultValue = "")
  private String abfsExternalAuthorizationClass;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ALWAYS_USE_HTTPS,
          DefaultValue = DEFAULT_ENABLE_HTTPS)
  private boolean alwaysUseHttps;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_USE_UPN,
      DefaultValue = DEFAULT_USE_UPN)
  private boolean useUpn;

  private Map<String, String> storageAccountKeys;

  public AbfsConfiguration(final Configuration rawConfig, String accountName)
      throws IllegalAccessException, InvalidConfigurationValueException, IOException {
    this.rawConfig = excludeIncompatibleCredentialProviders(
        rawConfig, AzureBlobFileSystem.class);
    this.accountName = accountName;
    this.isSecure = getBoolean(FS_AZURE_SECURE_MODE, false);

    validateStorageAccountKeys();
    Field[] fields = this.getClass().getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      if (field.isAnnotationPresent(IntegerConfigurationValidatorAnnotation.class)) {
        field.set(this, validateInt(field));
      } else if (field.isAnnotationPresent(LongConfigurationValidatorAnnotation.class)) {
        field.set(this, validateLong(field));
      } else if (field.isAnnotationPresent(StringConfigurationValidatorAnnotation.class)) {
        field.set(this, validateString(field));
      } else if (field.isAnnotationPresent(Base64StringConfigurationValidatorAnnotation.class)) {
        field.set(this, validateBase64String(field));
      } else if (field.isAnnotationPresent(BooleanConfigurationValidatorAnnotation.class)) {
        field.set(this, validateBoolean(field));
      }
    }
  }

  /**
   * There are certain integrations of the credential provider API in
   * which a recursive dependency between the provider and the hadoop
   * filesystem abstraction causes a problem. These integration points
   * need to leverage this utility method to remove problematic provider
   * types from the existing provider path within the configuration.
   *
   * @param config the existing configuration with provider path
   * @param fileSystemClass the class which providers must be compatible
   * @return Configuration clone with new provider path
   */
  private static Configuration excludeIncompatibleCredentialProviders(
      Configuration config, Class<? extends FileSystem> fileSystemClass)
      throws IOException {

    String providerPath = config.get(
        CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);

    if (providerPath == null) {
      return config;
    }
    StringBuffer newProviderPath = new StringBuffer();
    String[] providers = providerPath.split(",");
    Path path = null;
    for (String provider: providers) {
      try {
        path = ProviderUtils.unnestUri(new URI(provider));
        Class<? extends FileSystem> clazz = null;
        try {
          String scheme = path.toUri().getScheme();
          clazz = FileSystem.getFileSystemClass(scheme, config);
        } catch (IOException ioe) {
          // not all providers are filesystem based
          // for instance user:/// will not be able to
          // have a filesystem class associated with it.
          if (newProviderPath.length() > 0) {
            newProviderPath.append(",");
          }
          newProviderPath.append(provider);
        }
        if (clazz != null) {
          if (fileSystemClass.isAssignableFrom(clazz)) {
            LOG.debug("Filesystem based provider excluded from provider " +
                "path due to recursive dependency: {}", provider);
          } else {
            if (newProviderPath.length() > 0) {
              newProviderPath.append(",");
            }
            newProviderPath.append(provider);
          }
        }
      } catch (URISyntaxException e) {
        LOG.warn("Credential Provider URI is invalid." + provider);
      }
    }

    String effectivePath = newProviderPath.toString();
    if (effectivePath.equals(providerPath)) {
      return config;
    }

    Configuration conf = new Configuration(config);
    if (effectivePath.equals("")) {
      conf.unset(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
    } else {
      conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
          effectivePath);
    }
    return conf;
  }

  /**
   * Appends an account name to a configuration key yielding the
   * account-specific form.
   * @param key Account-agnostic configuration key
   * @return Account-specific configuration key
   */
  public String accountConf(String key) {
    return key + "." + accountName;
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value.
   * @param key Account-agnostic configuration key
   * @return value if one exists, else null
   */
  public String get(String key) {
    return rawConfig.get(accountConf(key), rawConfig.get(key));
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value.
   * @param key Account-agnostic configuration key
   * @return value if one exists, else the default value
   */
  public String getString(String key, String defaultValue) {
    return rawConfig.get(accountConf(key), rawConfig.get(key, defaultValue));
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value, and finally tries the default value.
   * @param key Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @return value if one exists, else the default value
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    return rawConfig.getBoolean(accountConf(key), rawConfig.getBoolean(key, defaultValue));
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value, and finally tries the default value.
   * @param key Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @return value if one exists, else the default value
   */
  public long getLong(String key, long defaultValue) {
    return rawConfig.getLong(accountConf(key), rawConfig.getLong(key, defaultValue));
  }

  /**
   * Returns the account-specific password in string form if it exists, then
   * looks for an account-agnostic value.
   * @param key Account-agnostic configuration key
   * @return value in String form if one exists, else null
   * @throws IOException
   */
  public String getPasswordString(String key) throws IOException {
    char[] passchars = rawConfig.getPassword(accountConf(key));
    if (passchars == null) {
      passchars = rawConfig.getPassword(key);
    }
    if (passchars != null) {
      return new String(passchars);
    }
    return null;
  }

  /**
   * Returns the account-specific Class if it exists, then looks for an
   * account-agnostic value, and finally tries the default value.
   * @param name Account-agnostic configuration key
   * @param defaultValue Class returned if none is configured
   * @param xface Interface shared by all possible values
   * @return Highest-precedence Class object that was found
   */
  public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface) {
    return rawConfig.getClass(accountConf(name),
        rawConfig.getClass(name, defaultValue, xface),
        xface);
  }

  /**
   * Returns the account-specific password in string form if it exists, then
   * looks for an account-agnostic value.
   * @param name Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @return value in String form if one exists, else null
   */
  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    return rawConfig.getEnum(accountConf(name),
        rawConfig.getEnum(name, defaultValue));
  }

  /**
   * Unsets parameter in the underlying Configuration object.
   * Provided only as a convenience; does not add any account logic.
   * @param key Configuration key
   */
  public void unset(String key) {
    rawConfig.unset(key);
  }

  /**
   * Sets String in the underlying Configuration object.
   * Provided only as a convenience; does not add any account logic.
   * @param key Configuration key
   * @param value Configuration value
   */
  public void set(String key, String value) {
    rawConfig.set(key, value);
  }

  /**
   * Sets boolean in the underlying Configuration object.
   * Provided only as a convenience; does not add any account logic.
   * @param key Configuration key
   * @param value Configuration value
   */
  public void setBoolean(String key, boolean value) {
    rawConfig.setBoolean(key, value);
  }

  public boolean isSecureMode() {
    return isSecure;
  }

  public String getStorageAccountKey() throws AzureBlobFileSystemException {
    String key;
    String keyProviderClass = get(AZURE_KEY_ACCOUNT_KEYPROVIDER);
    KeyProvider keyProvider;

    if (keyProviderClass == null) {
      // No key provider was provided so use the provided key as is.
      keyProvider = new SimpleKeyProvider();
    } else {
      // create an instance of the key provider class and verify it
      // implements KeyProvider
      Object keyProviderObject;
      try {
        Class<?> clazz = rawConfig.getClassByName(keyProviderClass);
        keyProviderObject = clazz.newInstance();
      } catch (Exception e) {
        throw new KeyProviderException("Unable to load key provider class.", e);
      }
      if (!(keyProviderObject instanceof KeyProvider)) {
        throw new KeyProviderException(keyProviderClass
                + " specified in config is not a valid KeyProvider class.");
      }
      keyProvider = (KeyProvider) keyProviderObject;
    }
    key = keyProvider.getStorageAccountKey(accountName, rawConfig);

    if (key == null) {
      throw new ConfigurationPropertyNotFoundException(accountName);
    }

    return key;
  }

  public Configuration getRawConfiguration() {
    return this.rawConfig;
  }

  public int getWriteBufferSize() {
    return this.writeBufferSize;
  }

  public int getReadBufferSize() {
    return this.readBufferSize;
  }

  public int getMinBackoffIntervalMilliseconds() {
    return this.minBackoffInterval;
  }

  public int getMaxBackoffIntervalMilliseconds() {
    return this.maxBackoffInterval;
  }

  public int getBackoffIntervalMilliseconds() {
    return this.backoffInterval;
  }

  public int getMaxIoRetries() {
    return this.maxIoRetries;
  }

  public long getAzureBlockSize() {
    return this.azureBlockSize;
  }

  public String getAzureBlockLocationHost() {
    return this.azureBlockLocationHost;
  }

  public int getMaxConcurrentWriteThreads() {
    return this.maxConcurrentWriteThreads;
  }

  public int getMaxConcurrentReadThreads() {
    return this.maxConcurrentReadThreads;
  }

  public boolean getTolerateOobAppends() {
    return this.tolerateOobAppends;
  }

  public String getAzureAtomicRenameDirs() {
    return this.azureAtomicDirs;
  }

  public boolean getCreateRemoteFileSystemDuringInitialization() {
    return this.createRemoteFileSystemDuringInitialization;
  }

  public boolean getSkipUserGroupMetadataDuringInitialization() {
    return this.skipUserGroupMetadataDuringInitialization;
  }

  public int getReadAheadQueueDepth() {
    return this.readAheadQueueDepth;
  }

  public boolean isFlushEnabled() {
    return this.enableFlush;
  }

  public boolean isOutputStreamFlushDisabled() {
    return this.disableOutputStreamFlush;
  }

  public boolean isAutoThrottlingEnabled() {
    return this.enableAutoThrottling;
  }

  public String getCustomUserAgentPrefix() {
    return this.userAgentId;
  }

  public SSLSocketFactoryEx.SSLChannelMode getPreferredSSLFactoryOption() {
    return getEnum(FS_AZURE_SSL_CHANNEL_MODE_KEY, DEFAULT_FS_AZURE_SSL_CHANNEL_MODE);
  }

  public AuthType getAuthType(String accountName) {
    return getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
  }

  public boolean isDelegationTokenManagerEnabled() {
    return enableDelegationToken;
  }

  public AbfsDelegationTokenManager getDelegationTokenManager() throws IOException {
    return new AbfsDelegationTokenManager(getRawConfiguration());
  }

  public boolean isHttpsAlwaysUsed() {
    return this.alwaysUseHttps;
  }

  public boolean isUpnUsed() {
    return this.useUpn;
  }

  public AccessTokenProvider getTokenProvider() throws TokenAccessProviderException {
    AuthType authType = getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
    if (authType == AuthType.OAuth) {
      try {
        Class<? extends AccessTokenProvider> tokenProviderClass =
                getClass(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME, null,
                        AccessTokenProvider.class);
        AccessTokenProvider tokenProvider = null;
        if (tokenProviderClass == ClientCredsTokenProvider.class) {
          String authEndpoint = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT);
          String clientId = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          String clientSecret = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET);
          tokenProvider = new ClientCredsTokenProvider(authEndpoint, clientId, clientSecret);
        } else if (tokenProviderClass == UserPasswordTokenProvider.class) {
          String authEndpoint = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT);
          String username = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_USER_NAME);
          String password = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_USER_PASSWORD);
          tokenProvider = new UserPasswordTokenProvider(authEndpoint, username, password);
        } else if (tokenProviderClass == MsiTokenProvider.class) {
          String tenantGuid = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT);
          String clientId = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          tokenProvider = new MsiTokenProvider(tenantGuid, clientId);
        } else if (tokenProviderClass == RefreshTokenBasedTokenProvider.class) {
          String refreshToken = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN);
          String clientId = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          tokenProvider = new RefreshTokenBasedTokenProvider(clientId, refreshToken);
        } else {
          throw new IllegalArgumentException("Failed to initialize " + tokenProviderClass);
        }
        return tokenProvider;
      } catch(IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw new TokenAccessProviderException("Unable to load key provider class.", e);
      }

    } else if (authType == AuthType.Custom) {
      try {
        String configKey = FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;
        Class<? extends CustomTokenProviderAdaptee> customTokenProviderClass =
                getClass(configKey, null, CustomTokenProviderAdaptee.class);
        if (customTokenProviderClass == null) {
          throw new IllegalArgumentException(
                  String.format("The configuration value for \"%s\" is invalid.", configKey));
        }
        CustomTokenProviderAdaptee azureTokenProvider = ReflectionUtils
                .newInstance(customTokenProviderClass, rawConfig);
        if (azureTokenProvider == null) {
          throw new IllegalArgumentException("Failed to initialize " + customTokenProviderClass);
        }
        azureTokenProvider.initialize(rawConfig, accountName);
        return new CustomTokenProviderAdapter(azureTokenProvider);
      } catch(IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw new TokenAccessProviderException("Unable to load custom token provider class.", e);
      }

    } else {
      throw new TokenAccessProviderException(String.format(
              "Invalid auth type: %s is being used, expecting OAuth", authType));
    }
  }

  public String getAbfsExternalAuthorizationClass() {
    return this.abfsExternalAuthorizationClass;
  }

  public AbfsAuthorizer getAbfsAuthorizer() throws IOException {
    String authClassName = getAbfsExternalAuthorizationClass();
    AbfsAuthorizer authorizer = null;

    try {
      if (authClassName != null && !authClassName.isEmpty()) {
        @SuppressWarnings("unchecked")
        Class<AbfsAuthorizer> authClass = (Class<AbfsAuthorizer>) rawConfig.getClassByName(authClassName);
        authorizer = authClass.getConstructor(new Class[] {Configuration.class}).newInstance(rawConfig);
        authorizer.init();
      }
    } catch (
        IllegalAccessException
        | InstantiationException
        | ClassNotFoundException
        | IllegalArgumentException
        | InvocationTargetException
        | NoSuchMethodException
        | SecurityException
        | AbfsAuthorizationException e) {
      throw new IOException(e);
    }
    return authorizer;
  }

  void validateStorageAccountKeys() throws InvalidConfigurationValueException {
    Base64StringConfigurationBasicValidator validator = new Base64StringConfigurationBasicValidator(
        FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, "", true);
    this.storageAccountKeys = rawConfig.getValByRegex(FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME_REGX);

    for (Map.Entry<String, String> account : storageAccountKeys.entrySet()) {
      validator.validate(account.getValue());
    }
  }

  int validateInt(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    IntegerConfigurationValidatorAnnotation validator = field.getAnnotation(IntegerConfigurationValidatorAnnotation.class);
    String value = get(validator.ConfigurationKey());

    // validate
    return new IntegerConfigurationBasicValidator(
        validator.MinValue(),
        validator.MaxValue(),
        validator.DefaultValue(),
        validator.ConfigurationKey(),
        validator.ThrowIfInvalid()).validate(value);
  }

  long validateLong(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    LongConfigurationValidatorAnnotation validator = field.getAnnotation(LongConfigurationValidatorAnnotation.class);
    String value = rawConfig.get(validator.ConfigurationKey());

    // validate
    return new LongConfigurationBasicValidator(
        validator.MinValue(),
        validator.MaxValue(),
        validator.DefaultValue(),
        validator.ConfigurationKey(),
        validator.ThrowIfInvalid()).validate(value);
  }

  String validateString(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    StringConfigurationValidatorAnnotation validator = field.getAnnotation(StringConfigurationValidatorAnnotation.class);
    String value = rawConfig.get(validator.ConfigurationKey());

    // validate
    return new StringConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  String validateBase64String(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    Base64StringConfigurationValidatorAnnotation validator = field.getAnnotation((Base64StringConfigurationValidatorAnnotation.class));
    String value = rawConfig.get(validator.ConfigurationKey());

    // validate
    return new Base64StringConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  boolean validateBoolean(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    BooleanConfigurationValidatorAnnotation validator = field.getAnnotation(BooleanConfigurationValidatorAnnotation.class);
    String value = rawConfig.get(validator.ConfigurationKey());

    // validate
    return new BooleanConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  @VisibleForTesting
  void setReadBufferSize(int bufferSize) {
    this.readBufferSize = bufferSize;
  }

  @VisibleForTesting
  void setWriteBufferSize(int bufferSize) {
    this.writeBufferSize = bufferSize;
  }

  @VisibleForTesting
  void setEnableFlush(boolean enableFlush) {
    this.enableFlush = enableFlush;
  }

  @VisibleForTesting
  void setDisableOutputStreamFlush(boolean disableOutputStreamFlush) {
    this.disableOutputStreamFlush = disableOutputStreamFlush;
  }

}

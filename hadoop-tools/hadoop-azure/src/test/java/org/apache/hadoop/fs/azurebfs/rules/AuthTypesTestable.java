package org.apache.hadoop.fs.azurebfs.rules;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import java.net.URISyntaxException;

public interface AuthTypesTestable {

  void setAuthType(AuthType authType);

  Configuration getInitialConfiguration();

  void initFSEndpointForNewFS() throws Exception;
}

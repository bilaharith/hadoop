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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;

import static org.apache.hadoop.io.IOUtils.wrapException;

/**
 * The BlobFsOutputStream for Rest AbfsClient.
 */
public class AbfsOutputStream extends OutputStream implements Syncable, StreamCapabilities {
  public static final Logger LOG = LoggerFactory.getLogger(AbfsOutputStream.class);
  private final AbfsClient client;
  private final String path;
  private long position;
  private boolean closed;
  private boolean supportFlush;
  private boolean disableOutputStreamFlush;
  private volatile IOException lastError;

  private long lastFlushOffset;
  private long lastTotalAppendOffset = 0;

  private final int bufferSize;
  private byte[] buffer;
  private int bufferIndex;

  private static int maxConcurrentRequestcount;
  private static int maxBufferCount;

  private ConcurrentLinkedDeque<WriteOperation> writeOperations;
  private static final Object INIT_LOCK = new Object();
  private static ThreadPoolExecutor threadExecutor;
  private static ExecutorCompletionService<Void> completionService;

  /**
   * Queue storing buffers with the size of the Azure block ready for
   * reuse. The pool allows reusing the blocks instead of allocating new
   * blocks. After the data is sent to the service, the buffer is returned
   * back to the queue
   */
  private static final ElasticByteBufferPool byteBufferPool
      = new ElasticByteBufferPool();
  private static AtomicInteger buffersToBeReturned = new AtomicInteger(0);

  public AbfsOutputStream(
      final AbfsClient client,
      final String path,
      final long position,
      final int bufferSize,
      final boolean supportFlush,
      final boolean disableOutputStreamFlush) {
    this.client = client;
    this.path = path;
    this.position = position;
    this.closed = false;
    this.supportFlush = supportFlush;
    this.disableOutputStreamFlush = disableOutputStreamFlush;
    this.lastError = null;
    this.lastFlushOffset = 0;
    this.bufferSize = bufferSize;
    this.buffer = byteBufferPool.getBuffer(false, bufferSize).array();
    this.bufferIndex = 0;
    this.writeOperations = new ConcurrentLinkedDeque<>();

    if (threadExecutor == null) {
      synchronized (INIT_LOCK) {
        if (threadExecutor == null) {
          int availableProcessors = Runtime.getRuntime().availableProcessors();
          maxConcurrentRequestcount = 4 * availableProcessors;
          maxBufferCount = maxConcurrentRequestcount + availableProcessors + 1;
          ThreadFactory deamonThreadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
              Thread deamonThread = Executors.defaultThreadFactory()
                  .newThread(runnable);
              deamonThread.setDaemon(true);
              return deamonThread;
            }
          };
          threadExecutor = new ThreadPoolExecutor(maxConcurrentRequestcount,
              maxConcurrentRequestcount, 10L, TimeUnit.SECONDS,
              new LinkedBlockingQueue<>(), deamonThreadFactory);
          completionService = new ExecutorCompletionService<>(threadExecutor);
        }
      }
    }
  }

  /**
   * Query the stream for a specific capability.
   *
   * @param capability string to query the stream support for.
   * @return true for hsync and hflush.
   */
  @Override
  public boolean hasCapability(String capability) {
    switch (capability.toLowerCase(Locale.ENGLISH)) {
      case StreamCapabilities.HSYNC:
      case StreamCapabilities.HFLUSH:
        return supportFlush;
      default:
        return false;
    }
  }

  /**
   * Writes the specified byte to this output stream. The general contract for
   * write is that one byte is written to the output stream. The byte to be
   * written is the eight low-order bits of the argument b. The 24 high-order
   * bits of b are ignored.
   *
   * @param byteVal the byteValue to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public void write(final int byteVal) throws IOException {
    write(new byte[]{(byte) (byteVal & 0xFF)});
  }

  /**
   * Writes length bytes from the specified byte array starting at off to
   * this output stream.
   *
   * @param data   the byte array to write.
   * @param off the start off in the data.
   * @param length the number of bytes to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public synchronized void write(final byte[] data, final int off, final int length)
      throws IOException {
    maybeThrowLastError();

    Preconditions.checkArgument(data != null, "null data");

    if (off < 0 || length < 0 || length > data.length - off) {
      throw new IndexOutOfBoundsException();
    }

    int currentOffset = off;
    int writableBytes = bufferSize - bufferIndex;
    int numberOfBytesToWrite = length;

    while (numberOfBytesToWrite > 0) {
      if (writableBytes <= numberOfBytesToWrite) {
        System.arraycopy(data, currentOffset, buffer, bufferIndex, writableBytes);
        bufferIndex += writableBytes;
        writeCurrentBufferToService();
        currentOffset += writableBytes;
        numberOfBytesToWrite = numberOfBytesToWrite - writableBytes;
      } else {
        System.arraycopy(data, currentOffset, buffer, bufferIndex, numberOfBytesToWrite);
        bufferIndex += numberOfBytesToWrite;
        numberOfBytesToWrite = 0;
      }

      writableBytes = bufferSize - bufferIndex;
    }
  }

  /**
   * Throw the last error recorded if not null.
   * After the stream is closed, this is always set to
   * an exception, so acts as a guard against method invocation once
   * closed.
   * @throws IOException if lastError is set
   */
  private void maybeThrowLastError() throws IOException {
    if (lastError != null) {
      throw lastError;
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be
   * written out. If any data remains in the payload it is committed to the
   * service. Data is queued for writing and forced out to the service
   * before the call returns.
   */
  @Override
  public void flush() throws IOException {
    if (!disableOutputStreamFlush) {
      flushInternalAsync();
    }
  }

  /** Similar to posix fsync, flush out the data in client's user buffer
   * all the way to the disk device (but the disk may have it in its cache).
   * @throws IOException if error occurs
   */
  @Override
  public void hsync() throws IOException {
    if (supportFlush) {
      flushInternal(false);
    }
  }

  /** Flush out the data in client's user buffer. After the return of
   * this call, new readers will see the data.
   * @throws IOException if any error occurs
   */
  @Override
  public void hflush() throws IOException {
    if (supportFlush) {
      flushInternal(false);
    }
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete. Close the access to the stream and
   * shutdown the upload thread pool.
   * If the blob was created, its lease will be released.
   * Any error encountered caught in threads and stored will be rethrown here
   * after cleanup.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      flushInternal(true);
    } catch (IOException e) {
      // Problems surface in try-with-resources clauses if
      // the exception thrown in a close == the one already thrown
      // -so we wrap any exception with a new one.
      // See HADOOP-16785
      throw wrapException(path, e.getMessage(), e);
    } finally {
      lastError = new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
      buffer = null;
      bufferIndex = 0;
      closed = true;
      writeOperations.clear();
    }
  }

  private synchronized void flushInternal(boolean isClose) throws IOException {
    maybeThrowLastError();
    writeCurrentBufferToService();
    flushWrittenBytesToService(isClose);
  }

  private synchronized void flushInternalAsync() throws IOException {
    maybeThrowLastError();
    writeCurrentBufferToService();
    flushWrittenBytesToServiceAsync();
  }

  private synchronized void writeCurrentBufferToService() throws IOException {
    if (bufferIndex == 0) {
      return;
    }

    final byte[] bytes = buffer;
    final int bytesLength = bufferIndex;
    bufferIndex = 0;
    final long offset = position;
    position += bytesLength;

    if (Runtime.getRuntime().freeMemory() < 100 * 1024 * 1024
        && buffersToBeReturned.get() > 0) {
      LOG.debug("Low memory");
      waitForTaskToComplete();
    }
    if (byteBufferPool.size(false) < 1
        && buffersToBeReturned.get() >= maxBufferCount) {
      waitForTaskToComplete();
    }

    buffer = byteBufferPool.getBuffer(false, bufferSize).array();
    buffersToBeReturned.incrementAndGet();

    final Future<Void> job = completionService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        AbfsPerfTracker tracker = client.getAbfsPerfTracker();
        try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker,
                "writeCurrentBufferToService", "append")) {
          AbfsRestOperation op = client.append(path, offset, bytes, 0,
                  bytesLength);
          perfInfo.registerResult(op.getResult());
          byteBufferPool.putBuffer(ByteBuffer.wrap(bytes));
          buffersToBeReturned.decrementAndGet();
          perfInfo.registerSuccess(true);
          return null;
        }
      }
    });

    writeOperations.add(new WriteOperation(job, offset, bytesLength));

    // Try to shrink the queue
    shrinkWriteOperationQueue();
  }

  private synchronized void flushWrittenBytesToService(boolean isClose) throws IOException {
    for (WriteOperation writeOperation : writeOperations) {
      try {
        writeOperation.task.get();
      } catch (Exception ex) {
        if (ex.getCause() instanceof AbfsRestOperationException) {
          if (((AbfsRestOperationException) ex.getCause()).getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new FileNotFoundException(ex.getMessage());
          }
        }

        if (ex.getCause() instanceof AzureBlobFileSystemException) {
          ex = (AzureBlobFileSystemException) ex.getCause();
        }
        lastError = new IOException(ex);
        throw lastError;
      }
    }
    flushWrittenBytesToServiceInternal(position, false, isClose);
  }

  private synchronized void flushWrittenBytesToServiceAsync() throws IOException {
    shrinkWriteOperationQueue();

    if (this.lastTotalAppendOffset > this.lastFlushOffset) {
      this.flushWrittenBytesToServiceInternal(this.lastTotalAppendOffset, true,
        false/*Async flush on close not permitted*/);
    }
  }

  private synchronized void flushWrittenBytesToServiceInternal(final long offset,
      final boolean retainUncommitedData, final boolean isClose) throws IOException {
    AbfsPerfTracker tracker = client.getAbfsPerfTracker();
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker,
            "flushWrittenBytesToServiceInternal", "flush")) {
      AbfsRestOperation op = client.flush(path, offset, retainUncommitedData, isClose);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    } catch (AzureBlobFileSystemException ex) {
      if (ex instanceof AbfsRestOperationException) {
        if (((AbfsRestOperationException) ex).getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new FileNotFoundException(ex.getMessage());
        }
      }
      throw new IOException(ex);
    }
    this.lastFlushOffset = offset;
  }

  /**
   * Try to remove the completed write operations from the beginning of write
   * operation FIFO queue.
   */
  private synchronized void shrinkWriteOperationQueue() throws IOException {
    try {
      while (writeOperations.peek() != null && writeOperations.peek().task.isDone()) {
        writeOperations.peek().task.get();
        lastTotalAppendOffset += writeOperations.peek().length;
        writeOperations.remove();
      }
    } catch (Exception e) {
      if (e.getCause() instanceof AzureBlobFileSystemException) {
        lastError = (AzureBlobFileSystemException) e.getCause();
      } else {
        lastError = new IOException(e);
      }
      throw lastError;
    }
  }

  private void waitForTaskToComplete() throws IOException {
    boolean completed;
    for (completed = false; completionService.poll() != null; completed = true) {
      // keep polling until there is no data
    }

    if (!completed) {
      try {
        completionService.take();
      } catch (InterruptedException e) {
        lastError = (IOException) new InterruptedIOException(e.toString()).initCause(e);
        throw lastError;
      }
    }
  }

  private static class WriteOperation {
    private final Future<Void> task;
    private final long startOffset;
    private final long length;

    WriteOperation(final Future<Void> task, final long startOffset, final long length) {
      Preconditions.checkNotNull(task, "task");
      Preconditions.checkArgument(startOffset >= 0, "startOffset");
      Preconditions.checkArgument(length >= 0, "length");

      this.task = task;
      this.startOffset = startOffset;
      this.length = length;
    }
  }

  @VisibleForTesting
  public synchronized void waitForPendingUploads()
      throws IOException, ExecutionException, InterruptedException {
      writeOperations.getFirst().task.get();
  }
}

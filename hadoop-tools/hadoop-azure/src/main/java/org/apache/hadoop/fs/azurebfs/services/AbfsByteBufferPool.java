/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE;

/**
 * Pool for byte[]
 */
public class AbfsByteBufferPool {

  /**
   * Queue holding the free buffers.
   */
  private ArrayBlockingQueue<byte[]> freeBuffers;
  /**
   * Count to track the buffers issued and yet to be returned.
   */
  private int numBuffersInUse;
  /**
   * Maximum number of buffers that can be in use.
   */
  private int maxBuffersInUse;
  private int bufferSize;

  /**
   * @param bufferSize                 Size of the byte[] to be returned.
   * @param maxConcurrentThreadCount   Maximum number of threads that will be
   *                                   using the pool.
   * @param maxWriteMemUsagePercentage Maximum percentage of memory that can
   *                                   be used by the pool from the max
   *                                   available memory.
   */
  public AbfsByteBufferPool(final int bufferSize,
      final int maxConcurrentThreadCount,
      final int maxWriteMemUsagePercentage) {
    Preconditions.checkArgument(maxWriteMemUsagePercentage
            >= MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE
            && maxWriteMemUsagePercentage
            <= MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
        "maxConcurrentThreadCount should be in range (%s - %s)",
        MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
        MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE);
    Preconditions.checkArgument(maxConcurrentThreadCount > 0,
        "maxConcurrentThreadCount cannot be < 1");
    this.bufferSize = bufferSize;
    this.numBuffersInUse = 0;
    freeBuffers = new ArrayBlockingQueue<>(maxConcurrentThreadCount + 1);

    double maxMemoryAllowedForPoolInMBs =
        Runtime.getRuntime().maxMemory()/(1024*1024) * maxWriteMemUsagePercentage / 100;
    double bufferCountByMemory = maxMemoryAllowedForPoolInMBs / bufferSize;
    double bufferCountByConcurrency =
        maxConcurrentThreadCount + Runtime.getRuntime().availableProcessors()
            + 1;
    maxBuffersInUse = (int) Math
        .ceil(Math.min(bufferCountByMemory, bufferCountByConcurrency));
    if (maxBuffersInUse < 2) {
      maxBuffersInUse = 2;
    }
  }

  /**
   * @return byte[] from the pool if available otherwise new byte[] is returned.
   * Waits if pool is empty and already maximum number of buffers are in use.
   */
  public synchronized byte[] get() {
    byte[] byteArray = freeBuffers.poll();
    if (byteArray == null) {
      if (numBuffersInUse < maxBuffersInUse) {
        byteArray = new byte[bufferSize];
      } else {
        try {
          byteArray = freeBuffers.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    numBuffersInUse++;
    return byteArray;
  }

  /**
   * @param byteArray The buffer to be offered back to the pool.
   */
  public synchronized void release(byte[] byteArray) {
    Preconditions.checkArgument(byteArray.length==bufferSize,"Buffer size has"
        + " to be %s", bufferSize);
    if (--numBuffersInUse < 0) {
      numBuffersInUse=0;
    }
    freeBuffers.offer(byteArray);
  }

  @VisibleForTesting
  public synchronized int getMaxBuffersInUse() {
    return this.maxBuffersInUse;
  }

  @VisibleForTesting
  public synchronized ArrayBlockingQueue<byte[]> getFreeBuffers() {
    return freeBuffers;
  }
}

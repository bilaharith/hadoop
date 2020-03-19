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

import java.util.LinkedList;
import java.util.Queue;

/**
 * Pool for byte[]
 */
public class AbfsBufferPool {

  /**
   * Queue holding the free byte[]s.
   */
  private Queue<byte[]> freeBuffers = new LinkedList<>();
  /**
   * Count to track the buffers issued and yet to be returned.
   */
  private int numBuffersInUse;
  /**
   * Maximum number of buffers that can be in use.
   */
  private int maxBuffersInUse;
  /**
   * Maximum number of free buffers that can be kept within the pool.
   */
  private int maxFreeBuffers;
  private int bufferSize;

  /**
   * @param bufferSize                 Size of the byte[] to be returned.
   * @param maxConcurrentThreadCount   Maximum number of threads that will be
   *                                   using the pool.
   * @param maxWriteMemUsagePercentage Maximum percentage of memory that can
   *                                   be used by the pool from the max
   *                                   available memory.
   */
  public AbfsBufferPool(final int bufferSize,
      final int maxConcurrentThreadCount,
      final int maxWriteMemUsagePercentage) {
    this.bufferSize = bufferSize;
    this.numBuffersInUse = 0;
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    this.maxFreeBuffers = maxConcurrentThreadCount + 1;

    double maxMemoryAllowedForPool =
        Runtime.getRuntime().maxMemory() * maxWriteMemUsagePercentage / 100;
    double bufferCountByMemory = maxMemoryAllowedForPool / bufferSize;
    double bufferCountByConcurrency =
        maxConcurrentThreadCount + availableProcessors + 1;
    maxBuffersInUse = (int) Math
        .ceil(Math.min(bufferCountByMemory, bufferCountByConcurrency));
  }

  /**
   * @return byte[] from the pool if available otherwise new byte[] is returned.
   * Waits if pool is empty and already maximum number of buffers are in use.
   */
  public synchronized byte[] get() {
    while (freeBuffers.isEmpty() && numBuffersInUse >= maxBuffersInUse) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    numBuffersInUse++;
    if (freeBuffers.isEmpty()) {
      return new byte[bufferSize];
    }
    return freeBuffers.remove();
  }

  /**
   * @param byteArray The buffer to be returned back to the pool. byteArray
   *                  is returned to the pool only if the pool contains less
   *                  than {@link #maxFreeBuffers}
   */
  public synchronized void release(byte[] byteArray) {
    numBuffersInUse--;
    if (numBuffersInUse < 0) {
      numBuffersInUse = 0;
    }
    if (byteArray.length == bufferSize && freeBuffers.size() < maxFreeBuffers) {
      freeBuffers.add(byteArray);
    }
    notifyAll();
  }
}

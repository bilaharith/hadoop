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

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsByteBufferPool;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamOld;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test create operation.
 */
public class ITestAzureBlobFileSystemCopyFromLocal
    extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemCopyFromLocal() throws Exception {
    super();
  }

  @Test
  public void testAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA() throws Exception {

    List<Integer> threadCounts = new ArrayList<>(Arrays.asList(4,8,16,32,40,
        64));

    for(int i=0;i<threadCounts.size();i++){
      int threadCount = threadCounts.get(i);
      for(int j=1;j<5;j++){
        try {
          test(threadCount, j * threadCount);
        }catch(Exception e){
          System.out.print("Thread count: "+threadCount+", Buffer count: "+j*threadCount);
          e.printStackTrace();
        }

        AbfsOutputStream.appendLatencies =
            Collections.synchronizedList(new ArrayList<>());

        AbfsOutputStream.getLatencies =  new ArrayList<>();;
      }
    }
  }

  //@Test
  public void test(int threadCount, int bufferCount) throws Exception {
    final AzureBlobFileSystem fsNew = getFileSystem();

    AbfsOutputStream.maxConcurrentThreadCountConf = threadCount;
    AbfsByteBufferPool.maxBuffersThatCanBeInUseConf = bufferCount;
    /*System.out.print("Thread count: "+threadCount+", Buffer count: "+bufferCount);
*/
    long start = System.currentTimeMillis();

    try {
      fsNew
          .copyFromLocalFile(new Path("/home/bith/Desktop/data-files/test.txt"),
              new Path("/tmp/abcABC1.java"));
      /*
      FileStatus[] status = fsNew.listStatus(new Path("/tmp"));
      for (int i = 0; i < status.length; i++) {
        System.out.print(status[i].getPath());
      }*/
    } catch (Exception e) {
      e.printStackTrace();
    }

    long end = System.currentTimeMillis();
    long timeTaken = end - start;

    System.out.print(timeTaken+", ");

    List<Long> appendList = AbfsOutputStream.appendLatencies;

    List<Long> getList = AbfsOutputStream.getLatencies;
    //List<Long> getList = AbfsOutputStreamOld.getLatencies;

    printAnalysis(getList,
        "BufferPool.get Latency, "+threadCount+", "+bufferCount);
    printAnalysis(appendList, ", Append thread Latency ");
    System.out.print("\n");
  }

  private void printAnalysis(List<Long> list, String heading) {

    Collections.sort(list);
    int sum = 0;
    for (int i = 0; i < list.size(); i++) {
      long latency = list.get(i);
      sum += latency;
      //System.out.print(latency);
    }

    int size = list.size();
    int pc50 = size * 50 / 100;
    int pc75 = size * 75 / 100;
    int pc90 = size * 90 / 100;
    int pc95 = size * 95 / 100;
    int pc99 = size * 99 / 100;

    double avg = 0.0;
    if (sum != 0) {
      avg = (sum / list.size());
    }

    System.out.print(heading.toUpperCase());
    //System.out.print("----------------------------");
    /*System.out.print("Number of calls : " + list.size());
    System.out.print("Min: " + list.get(0));
    System.out.print("Max: " + list.get(list.size() - 1));
    System.out.print("Avg: " + avg);
    System.out.print("50 percentile: " + list.get(pc50));
    System.out.print("75 percentile: " + list.get(pc75));
    System.out.print("90 percentile: " + list.get(pc90));
    System.out.print("95 percentile: " + list.get(pc95));
    System.out.print("99 percentile: " + list.get(pc99));*/


    System.out.print(", " + list.size());
    System.out.print(", " + list.get(0));
    System.out.print(", " + list.get(list.size() - 1));
    System.out.print(", " + avg);
    System.out.print(", " + list.get(pc50));
    System.out.print(", " + list.get(pc75));
    System.out.print(", " + list.get(pc90));
    System.out.print(", " + list.get(pc95));
    System.out.print(", " + list.get(pc99));
  }
}

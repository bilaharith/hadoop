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

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamOld;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test create operation.
 */
public class ITestAzureBlobFileSystemCopyFromLocal extends
    AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemCopyFromLocal() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileCreatedImmediately() throws Exception {
    final AzureBlobFileSystem fsNew = getFileSystem();


    long start = System.currentTimeMillis();



    try {
      fsNew.copyFromLocalFile( new Path("/home/bith/Desktop/data-files/test.txt"),
        new Path("/tmp/abcABC1.java"));
      FileStatus[] status = fsNew.listStatus(new Path("/tmp"));
      for(int i=0;i<status.length;i++){
        System.out.println(status[i].getPath());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }






    long end = System.currentTimeMillis();
    long timeTaken = end - start;

    System.out.println("Time taken: " + timeTaken);
    System.out.println("\n");

    List<Long> list = AbfsOutputStream.getLatencies;
    //List<Long> list = AbfsOutputStreamOld.getLatencies;

    Collections.sort(list);
    int sum = 0;
    for (int i = 0; i < list.size(); i++) {
      long latency = list.get(i);
      sum += latency;
      System.out.println(latency);
    }

    int size = list.size();
    int pc50 = size * 50/100;
    int pc75 = size * 75/100;
    int pc90 = size * 90/100;
    int pc95 = size * 95/100;
    int pc99 = size * 99/100;

    System.out.println("size : "+list.size());
    System.out.println("\n");

    double avg = 0.0;
    if(sum!=0) {
      avg = (sum / list.size());
    }
    System.out.println("Min: "+list.get(0)+", Max: "+list.get(list.size()-1)+
        ", Avg:"+avg);
    System.out.println("\n");
    System.out.println(list.get(pc50));
    System.out.println(list.get(pc75));
    System.out.println(list.get(pc90));
    System.out.println(list.get(pc95));
    System.out.println(list.get(pc99));

  }

}

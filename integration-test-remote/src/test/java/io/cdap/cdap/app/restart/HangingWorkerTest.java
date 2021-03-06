/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.app.restart;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.AudiTestBase;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkerManager;
import io.cdap.cdap.test.suite.category.SDKIncompatible;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test restarting non-responding program
 */
@Category({
  // we don't support stopping a hanging worker on CDAP SDK.
  SDKIncompatible.class
})
public class HangingWorkerTest extends AudiTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(HangingWorkerTest.class);

  @Test
  public void testRestart() throws Exception {
    ApplicationManager applicationManager = deployApplication(HangingWorkerApp.class);

    // start the worker
    WorkerManager workerManager = applicationManager.getWorkerManager(HangingWorkerApp.WORKER_NAME);
    startAndWaitForRun(workerManager, ProgramRunStatus.RUNNING);

    // the worker writes current time into workerDataset every HangingWorkerApp.WORKER_SLEEP_SECS secs
    final DataSetManager<KeyValueTable> workerDataset = getKVTableDataset(HangingWorkerApp.WORKER_DATASET_NAME);
    Tasks.waitFor(true, new Callable<Boolean>() {
      private final long currentTime = System.currentTimeMillis();

      @Override
      public Boolean call() throws Exception {
        byte[] bytes = workerDataset.get().read(HangingWorker.getKey(0));
        LOG.info("Got time = {}", bytes == null ? null : Bytes.toLong(bytes));
        return bytes != null && Bytes.toLong(bytes) > currentTime;
      }
    }, 60, TimeUnit.SECONDS, 2, TimeUnit.SECONDS, "Worker 0 is not running");

    Tasks.waitFor(true, new Callable<Boolean>() {
      private final long currentTime = System.currentTimeMillis();

      @Override
      public Boolean call() throws Exception {
        byte[] bytes = workerDataset.get().read(HangingWorker.getKey(1));
        LOG.info("Got time = {}", bytes == null ? null : Bytes.toLong(bytes));
        return bytes != null && Bytes.toLong(bytes) > currentTime;
      }
    }, 60, TimeUnit.SECONDS, 2, TimeUnit.SECONDS, "Worker 1 is not running");

    LOG.info("Both instances of workers have started");

    LOG.info("Scale down the worker instance to 1");
    workerManager.setInstances(1);

    // Worker with instance id 1 should stop now
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        byte[] bytes = workerDataset.get().read(HangingWorker.getKey(1));
        LOG.info("Got time = {}", bytes == null ? null : Bytes.toLong(bytes));
        return bytes != null &&
          (System.currentTimeMillis() - Bytes.toLong(bytes)) >
            TimeUnit.MILLISECONDS.convert(HangingWorkerApp.WORKER_SLEEP_SECS + 10, TimeUnit.SECONDS);
      }
    }, 180, TimeUnit.SECONDS, 5, TimeUnit.SECONDS, "Worker 1 has not stopped");

    // However worker with instance id 0 should still be running
    Tasks.waitFor(true, new Callable<Boolean>() {
      private final long currentTime = System.currentTimeMillis();

      @Override
      public Boolean call() throws Exception {
        byte[] bytes = workerDataset.get().read(HangingWorker.getKey(0));
        LOG.info("Got time = {}", bytes == null ? null : Bytes.toLong(bytes));
        return bytes != null && Bytes.toLong(bytes) > currentTime;
      }
    }, 60, TimeUnit.SECONDS, 2, TimeUnit.SECONDS, "Worker 0 is not running");
  }
}

/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.security;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.examples.helloworld.HelloWorld;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.examples.purchase.PurchaseHistoryStore;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProtoConstraint;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.remote.dataset.AbstractDatasetApp;
import co.cask.cdap.remote.dataset.table.TableDatasetApp;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Basic authorization test base for apps
 */
public class BasicAppAuthorizationTest extends AuthorizationTestBase {

  // namespace for app1 for cross namespace test. For tests without cross namespace, use testNamespace in
  // AuthorizationTestBase instead.
  protected NamespaceMeta crossNsTest1 = getNamespaceMeta(new NamespaceId("authorization11"), null, null,
                                                          null, null, null, null);
  // namespace for app2 for cross namespace test
  protected NamespaceMeta crossNsTest2 = getNamespaceMeta(new NamespaceId("authorization22"), null, null,
                                                          null, null, null, null);

  // owner of app1 deployed in crossNs1
  // Ideally this should be null but since any app without impersonation will be run as user cdap,
  // the privileges on other user will be useless in the test, app impersonation is required.
  // To deploy without app impersonation, set this to null.
  protected String appOwner1 = ALICE;
  // owner of app2 deployed in crossNs2
  protected String appOwner2 = BOB;
  // app owner for all other apps
  protected String appOwner = null;

  /**
   * Test deploy app under authorization.
   */
  @Test
  public void testDeployApp() throws Exception {

    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    NamespaceId namespaceId = testNamespace.getNamespaceId();
    ApplicationId appId = namespaceId.app(PurchaseApp.APP_NAME);

    // pre-grant all required privileges
    // admin user will be able to create namespace and retrieve the status of programs(needed for teardown)
    String workflowName = "PurchaseHistoryWorkflow";
    ImmutableMap.Builder<EntityId, Set<Action>> adminPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(namespaceId, EnumSet.of(Action.ADMIN));

    // Privileges needed to create datasets and streams
    Map<EntityId, Set<Action>> dsStreamCreationPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(namespaceId.dataset("frequentCustomers"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.stream("purchaseStream"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.dataset("userProfiles"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.dataset("history"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.dataset("purchases"), EnumSet.of(Action.ADMIN))
      .put(namespaceId.datasetModule(PurchaseHistoryStore.class.getName()), EnumSet.of(Action.ADMIN))
      .put(namespaceId.datasetType(PurchaseHistoryStore.class.getName()), EnumSet.of(Action.ADMIN))
      .build();

    Map<EntityId, Set<Action>> dsStreamReadWritePrvileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(namespaceId.dataset("history"), EnumSet.of(Action.WRITE))
      .put(namespaceId.dataset("purchases"), EnumSet.of(Action.READ))
      .build();

    // carol will be able to deploy the purchase app
    ImmutableMap.Builder<EntityId, Set<Action>> appDeployPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(appId, EnumSet.of(Action.ADMIN))
      // use some random artifact version to test version is not enforced
      .put(namespaceId.artifact(PurchaseApp.class.getSimpleName(),
                                String.format("1.0.0-SNAPSHOT-%d", System.currentTimeMillis())),
           EnumSet.of(Action.ADMIN));
    String namespacePrincipal = testNamespace.getConfig().getPrincipal();
    String appEffectiveOwner = appOwner == null ? namespacePrincipal : appOwner;
    if (namespacePrincipal != null) {
      // this is needed to create an impersonated namespace
      adminPrivileges.put(new KerberosPrincipalId(namespacePrincipal), EnumSet.of(Action.ADMIN));
    }
    if (appEffectiveOwner != null) {
      // this is needed to create an impersonated app
      appDeployPrivileges.put(new KerberosPrincipalId(appEffectiveOwner), EnumSet.of(Action.ADMIN));
      // if impersonation is involved, impersonated user will be responsible to create the dataset
      setUpPrivileges(appEffectiveOwner, dsStreamCreationPrivileges);
      // impersonated user will run the program, so it needs privilege on the dataset and stream
      setUpPrivileges(appEffectiveOwner, dsStreamReadWritePrvileges);
    } else {
      // else the requesting user will need the privileges
      appDeployPrivileges.putAll(dsStreamCreationPrivileges);
      setUpPrivileges(CDAP_USER, dsStreamCreationPrivileges);
      // if no impersonation, CDAP will run the program, so it needs privilege on the dataset and stream
      setUpPrivileges(CDAP_USER, dsStreamReadWritePrvileges);
    }
    setUpPrivileges(ADMIN_USER, adminPrivileges.build());
    setUpPrivileges(CAROL, appDeployPrivileges.build());
    authorizationTestClient.grant(ALICE, appId.workflow(workflowName), Action.EXECUTE);
    // this is needed to add the schedule
    authorizationTestClient.grant(ALICE, appId, Action.ADMIN);
    authorizationTestClient.waitForAuthzCacheTimeout();

    createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    TestManager testManager = getTestManager(carolConfig, carolClient);
    testManager.addAppArtifact(namespaceId.artifact(PurchaseApp.class.getSimpleName(), "1.0.0"), PurchaseApp.class);
    ArtifactSummary appArtifactSummary = new ArtifactSummary(PurchaseApp.class.getSimpleName(), "1.0.0");
    ApplicationManager appManager =
      testManager.deployApplication(appId, new AppRequest<>(appArtifactSummary, null, appOwner));

    ScheduleId scheduleId = new ScheduleId(appId.getNamespace(), appId.getApplication(), "testSchedule");
    ScheduleDetail scheduleDetail = new ScheduleDetail(
      scheduleId.getSchedule(), null,
      new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, workflowName), null,
      new ProtoTrigger.TimeTrigger("0/1 * * * *"), ImmutableList.of(new ProtoConstraint.ConcurrencyConstraint(1)),
      null);

    ClientConfig bobConfig = getClientConfig(fetchAccessToken(BOB, BOB + PASSWORD_SUFFIX));
    RESTClient bobClient = new RESTClient(bobConfig);
    bobClient.addListener(createRestClientListener());
    ScheduleClient scheduleBobClient = new ScheduleClient(bobConfig, bobClient);
    try {
      scheduleBobClient.add(scheduleId, scheduleDetail);
      Assert.fail();
    } catch (UnauthorizedException e) {
      // expected
    }

    ClientConfig aliceConfig = getClientConfig(fetchAccessToken(ALICE, ALICE + PASSWORD_SUFFIX));
    RESTClient aliceClient = new RESTClient(aliceConfig);
    aliceClient.addListener(createRestClientListener());

    ScheduleClient scheduleAliceClient = new ScheduleClient(aliceConfig, aliceClient);
    scheduleAliceClient.add(scheduleId, scheduleDetail);
    scheduleAliceClient.resume(scheduleId);

    WorkflowManager workflowAliceManager =
      getTestManager(aliceConfig, aliceClient).getApplicationManager(appId).getWorkflowManager(workflowName);
    workflowAliceManager.waitForRun(ProgramRunStatus.RUNNING, 90, TimeUnit.SECONDS);
    scheduleAliceClient.suspend(scheduleId);
    workflowAliceManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
  }

  /**
   * Test user cannot deploy app with insufficient privilege.
   */
  @Test
  public void testDeployAppUnauthorized() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());
    NamespaceId namespaceId = testNamespace.getNamespaceId();

    authorizationTestClient.grant(ADMIN_USER, namespaceId, Action.ADMIN);
    String principal = testNamespace.getConfig().getPrincipal();
    if (principal != null) {
      authorizationTestClient.grant(ADMIN_USER, new KerberosPrincipalId(principal), Action.ADMIN);
    }
    authorizationTestClient.waitForAuthzCacheTimeout();

    createAndRegisterNamespace(testNamespace, adminConfig, adminClient);

    ClientConfig carolConfig = getClientConfig(fetchAccessToken(CAROL, CAROL + PASSWORD_SUFFIX));
    RESTClient carolClient = new RESTClient(carolConfig);
    carolClient.addListener(createRestClientListener());

    try {
      getTestManager(carolConfig, carolClient).deployApplication(namespaceId, PurchaseApp.class);
      Assert.fail();
    } catch (Exception ex) {
      // expected
      Assert.assertTrue(ex.getMessage().toLowerCase().contains(NOT_VISIBLE_MSG.toLowerCase()));
    }
  }

  /**
   * Test dataset read/write in a program. Two apps are deployed in different namespaces owned by user1 and user2
   * user1 will have write on its dataset, and user2 will have read on user1's dataset(dataset1),
   * In the first program run, test user1 is able to write to the dataset and user2 is able to read when program is
   * running. So for user2 only Read operation should succeed. In the second program run, user2 is able to read and
   * write on dataset1, so Read, Write, IncrementAndGet should work. In last run, user2 is only able to write, so
   * Read and IncrementAndGet will not work.
   *
   * Note that this test can ONLY be used when impersonation is enabled, since currently we do not have
   * endpoint enforcement
   */
  @Test
  public void testDatasetInProgram() throws Exception {
    ClientConfig adminConfig = getClientConfig(fetchAccessToken(ADMIN_USER, ADMIN_USER));
    RESTClient adminClient = new RESTClient(adminConfig);
    adminClient.addListener(createRestClientListener());

    String datasetName = "testReadDataset";

    NamespaceId testNs1 = crossNsTest1.getNamespaceId();
    NamespaceId testNs2 = crossNsTest2.getNamespaceId();
    ApplicationId appId1 = testNs1.app(TableDatasetApp.class.getSimpleName());
    ApplicationId appId2 = testNs2.app(TableDatasetApp.class.getSimpleName());

    // todo: remove this once we support endpoint enforcement
    String nsPrincipal1 = crossNsTest1.getConfig().getPrincipal();
    String nsPrincipal2 = crossNsTest2.getConfig().getPrincipal();
    String user1 = appOwner1 == null ? nsPrincipal1 : appOwner1;
    String user2 = appOwner2 == null ? nsPrincipal2 : appOwner2;
    if (user1 == null || user2 == null) {
      Assert.fail("This test can only be used when impersonation is enabled");
    }

    // pre-grant all required privileges
    // admin user will be able to create namespace and retrieve the status of programs(needed for teardown)
    ImmutableMap.Builder<EntityId, Set<Action>> adminPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      // privilege to create namespace
      .put(testNs1, EnumSet.of(Action.ADMIN))
      .put(testNs2, EnumSet.of(Action.ADMIN))
      // these privileges are for program running and retrieve service url, not for deploying the app
      .put(appId1.service("DatasetService"), EnumSet.of(Action.EXECUTE, Action.ADMIN))
      .put(appId2.service("DatasetService"), EnumSet.of(Action.EXECUTE, Action.ADMIN))
      // privilege to deploy app1
      .put(appId1, EnumSet.of(Action.ADMIN))
      // use some random artifact version to test version is not enforced
      .put(testNs1.artifact(TableDatasetApp.class.getSimpleName(),
                            String.format("1.0.0-SNAPSHOT-%d", System.currentTimeMillis())), EnumSet.of(Action.ADMIN))
      .put(new KerberosPrincipalId(user1), EnumSet.of(Action.ADMIN))
      // privilege to deploy app2
      .put(appId2, EnumSet.of(Action.ADMIN))
      // use some random artifact version to test version is not enforced
      .put(testNs2.artifact(TableDatasetApp.class.getSimpleName(),
                            String.format("1.0.0-SNAPSHOT-%d", System.currentTimeMillis())), EnumSet.of(Action.ADMIN))
      .put(new KerberosPrincipalId(user2), EnumSet.of(Action.ADMIN));
    // this is to create impersonated ns
    if (nsPrincipal1 != null && !nsPrincipal1.equals(user1)) {
      adminPrivileges.put(new KerberosPrincipalId(nsPrincipal1), EnumSet.of(Action.ADMIN));
    }
    if (nsPrincipal2 != null && !nsPrincipal2.equals(user2)) {
      adminPrivileges.put(new KerberosPrincipalId(nsPrincipal2), EnumSet.of(Action.ADMIN));
    }
    setUpPrivileges(ADMIN_USER, adminPrivileges.build());
    // since impersonation is involved, grant privileges to create the dataset and also let user1 be able to
    // write the dataset
    DatasetId dataset1 = testNs1.dataset(datasetName);
    DatasetId dataset2 = testNs2.dataset(datasetName);
    authorizationTestClient.grant(user1, dataset1, Action.ADMIN);
    authorizationTestClient.grant(user1, dataset1, Action.WRITE);
    // grant user2 the read access to dataset in ns1
    authorizationTestClient.grant(user2, dataset2, Action.ADMIN);
    authorizationTestClient.grant(user2, dataset1, Action.READ);
    authorizationTestClient.waitForAuthzCacheTimeout();

    createAndRegisterNamespace(crossNsTest1, adminConfig, adminClient);
    createAndRegisterNamespace(crossNsTest2, adminConfig, adminClient);

    // initialize clients and configs for users user1 and user2
    ClientConfig user1Config = getClientConfig(fetchAccessToken(user1, user1 + PASSWORD_SUFFIX));
    RESTClient user1Client = new RESTClient(user1Config);

    ClientConfig user2Config = getClientConfig(fetchAccessToken(user2, user2 + PASSWORD_SUFFIX));
    RESTClient user2Client = new RESTClient(user2Config);

    user1Client.addListener(createRestClientListener());
    user2Client.addListener(createRestClientListener());

    ServiceManager user1ServiceManager =
      setupAppStartAndGetService(testNs1, adminConfig, adminClient, datasetName, appOwner1);

    try {
      // user1 writes an entry to the dataset
      URL serviceURL = user1ServiceManager.getServiceURL();
      Put put = new Put(Bytes.toBytes("row"), Bytes.toBytes("col"), 100L);
      HttpResponse httpResponse = user1Client.execute(
        HttpRequest.post(serviceURL.toURI().resolve("put").toURL()).withBody(GSON.toJson(put)).build(),
        user1Config.getAccessToken());
      Assert.assertEquals(200, httpResponse.getResponseCode());
    } finally {
      user1ServiceManager.stop();
      user1ServiceManager.waitForRun(ProgramRunStatus.KILLED,
                                     PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    ServiceManager user2ServiceManager =
      setupAppStartAndGetService(testNs2, adminConfig, adminClient, datasetName, appOwner2);

    int runs = 0;
    try {
      URL serviceURL = user2ServiceManager.getServiceURL();

      // try to get the entry written by user2 for the dataset owned by user1
      // user2 has read access on it, so read should succeed
      String getJson = GSON.toJson(new Get(Bytes.toBytes("row"), Bytes.toBytes("col")));
      String putJson = GSON.toJson(new Put(Bytes.toBytes("row"), Bytes.toBytes("col2"), Bytes.toBytes("val2")));
      String incrementJson = GSON.toJson(new Increment(Bytes.toBytes("row"), Bytes.toBytes("col"), 1));
      HttpResponse response = executeDatasetCommand(serviceURL, user2Client, user2Config,
                                                    testNs1, datasetName, getJson, "get");
      Assert.assertEquals(200, response.getResponseCode());

      try {
        // put should fail since user2 does not have write privilege on the dataset
        executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, putJson, "put");
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_ACCESS_MSG.toLowerCase()));
      }

      try {
        // incrementAndGet should fail since user2 does not have both READ and WRITE privilege on the dataset
        executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, incrementJson,
                              "incrementAndGet");
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_ACCESS_MSG.toLowerCase()));
      }

      // grant user WRITE privilege on the dataset
      authorizationTestClient.grant(user2, dataset1, Action.WRITE);
      authorizationTestClient.waitForAuthzCacheTimeout();

      user2ServiceManager.stop();
      user2ServiceManager.waitForRuns(ProgramRunStatus.KILLED, ++runs, PROGRAM_START_STOP_TIMEOUT_SECONDS,
                                      TimeUnit.SECONDS);
      user2ServiceManager.start();
      user2ServiceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      serviceURL = user2ServiceManager.getServiceURL();

      // put should be successful this time
      response = executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, putJson, "put");
      Assert.assertEquals(200, response.getResponseCode());

      // incrementAndGet should be successful this time since user2 has both READ and WRITE privilege on the dataset
      response = executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, incrementJson,
                                       "incrementAndGet");
      Assert.assertEquals(200, response.getResponseCode());

      // revoke privileges from user2
      authorizationTestClient.revoke(user2, dataset1, Action.READ);
      authorizationTestClient.waitForAuthzCacheTimeout();

      user2ServiceManager.stop();
      user2ServiceManager.waitForRuns(ProgramRunStatus.KILLED, ++runs,
                                      PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      user2ServiceManager.start();
      user2ServiceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      serviceURL = user2ServiceManager.getServiceURL();

      // put should be successful since we only revoke READ from user2
      response = executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, putJson, "put");
      Assert.assertEquals(200, response.getResponseCode());

      try {
        // get should fail since user2 does not have READ privilege now
        executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, getJson, "get");
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_ACCESS_MSG.toLowerCase()));
      }

      try {
        // increment should fail since user2 does not have both READ and WRITE privilege on the dataset
        executeDatasetCommand(serviceURL, user2Client, user2Config, testNs1, datasetName, incrementJson,
                              "incrementAndGet");
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().toLowerCase().contains(NO_ACCESS_MSG.toLowerCase()));
      }
    } finally {
      user2ServiceManager.stop();
      user2ServiceManager.waitForRuns(ProgramRunStatus.KILLED, ++runs,
                                      PROGRAM_FIRST_PROCESSED_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  private ServiceManager setupAppStartAndGetService(NamespaceId namespaceId, ClientConfig clientConfig,
                                                    RESTClient restClient, String datasetName,
                                                    @Nullable String ownerPrincipal) throws Exception {
    ArtifactId tableDatasetApp = namespaceId.artifact(TableDatasetApp.class.getSimpleName(), VERSION);
    TestManager testManager = getTestManager(clientConfig, restClient);

    testManager.addAppArtifact(tableDatasetApp, TableDatasetApp.class);

    ArtifactSummary appSummary = new ArtifactSummary(TableDatasetApp.class.getSimpleName(), VERSION);
    ApplicationId applicationId = namespaceId.app(TableDatasetApp.class.getSimpleName());

    TableDatasetApp.DatasetConfig config = new AbstractDatasetApp.DatasetConfig(datasetName);

    ApplicationManager applicationManager =
      testManager.deployApplication(applicationId, new AppRequest<>(appSummary, config, ownerPrincipal));

    ServiceManager serviceManager =
      applicationManager.getServiceManager(TableDatasetApp.DatasetService.class.getSimpleName());

    serviceManager.start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // i have noticed this take longer than 60 seconds on CM cluster
    serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

    return serviceManager;
  }

  private HttpResponse executeDatasetCommand(URL serviceURL, RESTClient client, ClientConfig config,
                                             NamespaceId namespaceId, String datasetName,
                                             String jsonString, String method) throws Exception {
    String path = String.format("namespaces/%s/datasets/%s/%s", namespaceId.getNamespace(), datasetName, method);
    return client.execute(
      HttpRequest.post(serviceURL.toURI().resolve(path).toURL()).withBody(jsonString).build(),
      config.getAccessToken());
  }

  private void startAndKillProgram(ProgramManager programManager, int runs) throws Exception {
    programManager.start();
    programManager.waitForRun(ProgramRunStatus.RUNNING, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    programManager.stop();
    programManager.waitForRuns(ProgramRunStatus.KILLED, runs, PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
}

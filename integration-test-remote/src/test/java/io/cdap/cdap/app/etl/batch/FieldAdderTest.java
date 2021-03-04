/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.app.etl.ETLTestBase;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.security.authentication.client.AccessToken;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.plugin.common.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FieldAdderTest extends ETLTestBase {

  private static final ArtifactSelectorConfig CORE_PLUGINS_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "core-plugins", "[0.0.0, 100.0.0)");
  private static final ArtifactSelectorConfig FIELD_ADDER_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "field-adder", "[0.0.0, 100.0.0)");

  private static final String FILE_SOURCE_PLUGIN_NAME = "File";
  private static final String MULTI_FIELD_ADDER_PLUGIN_NAME = "MultiFieldAdder";
  private static final String TABLE_SINK_PLUGIN_NAME = "Table";
  private static final String SINK_TABLE_NAME = "field-adder-sink";

  private static final String NON_MACRO_FIELD_VALUE = "key1:value1,key2:value2";
  private static final String MACRO_FIELD_VALUE = "key1:${value1},${key2}:value2";
  private static final String WHOLE_MACRO_FIELD_VALUE = "${fieldValue}";

  private String sourcePath;

  private static final Schema INPUT_SCHEMA = Schema.recordOf("record",
                                                             Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                                             Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                                             Schema.Field.of("surname", Schema.of(Schema.Type.STRING)));
  private static final Schema OUTPUT_SCHEMA =
    Schema.recordOf("output",
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("surname", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("key1", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("key2", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  @Before
  public void testSetup() throws Exception {
    ApplicationManager applicationManager = deployApplication(UploadFile.class);
    String fileSetName = UploadFile.FileSetService.class.getSimpleName();
    ServiceManager serviceManager = applicationManager.getServiceManager(fileSetName);
    startAndWaitForRun(serviceManager, ProgramRunStatus.RUNNING);
    URL serviceURL = serviceManager.getServiceURL(PROGRAM_START_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    URL url = new URL(serviceURL, "fieldadder/create");
    //POST request to create a new file set with name fieldadder.csv.
    HttpResponse response = getRestClient().execute(HttpMethod.POST, url, getClientConfig().getAccessToken());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    url = new URL(serviceURL, "fieldadder?path=fieldadder.csv");
    //PUT request to upload the fieldadder.csv file, sent in the request body
    getRestClient().execute(HttpRequest.put(url).withBody(new File("src/test/resources/fieldadder.csv"))
                              .build(), getClientConfig().getAccessToken());

    URL pathServiceUrl = new URL(serviceURL, "fieldadder?path");
    AccessToken accessToken = getClientConfig().getAccessToken();
    HttpResponse sourceResponse = getRestClient().execute(HttpMethod.GET, pathServiceUrl, accessToken);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, sourceResponse.getResponseCode());
    sourcePath = sourceResponse.getResponseBodyAsString();
  }

  @Test
  public void testMultiFieldAdderNonMacroValues() throws Exception {
    testMultiFieldAdderNonMacroValues(Engine.SPARK);
    testMultiFieldAdderNonMacroValues(Engine.MAPREDUCE);
  }

  private void testMultiFieldAdderNonMacroValues(Engine engine) throws Exception {
    Map<String, String> args = new HashMap<>();
    args.put("schema", OUTPUT_SCHEMA.toString());

    multiFieldAdderConfig(engine, NON_MACRO_FIELD_VALUE, args);
    assertValues();
  }

  @Test
  public void testMultiFieldAdderMacroValues() throws Exception {
    testMultiFieldAdderMacroValues(Engine.SPARK);
    testMultiFieldAdderMacroValues(Engine.MAPREDUCE);
  }

  private void testMultiFieldAdderMacroValues(Engine engine) throws Exception {
    Map<String, String> args = new HashMap<>();
    args.put("schema", OUTPUT_SCHEMA.toString());
    args.put("value1", "value1");
    args.put("key2", "key2");

    multiFieldAdderConfig(engine, MACRO_FIELD_VALUE, args);
    assertValues();
  }

  @Test
  public void testMultiFieldAdderWholeMacroValue() throws Exception {
    testMultiFieldAdderWholeMacroValue(Engine.SPARK);
    testMultiFieldAdderWholeMacroValue(Engine.MAPREDUCE);
  }

  private void testMultiFieldAdderWholeMacroValue(Engine engine) throws Exception {
    Map<String, String> args = new HashMap<>();
    args.put("schema", OUTPUT_SCHEMA.toString());
    args.put("fieldValue", NON_MACRO_FIELD_VALUE);

    multiFieldAdderConfig(engine, WHOLE_MACRO_FIELD_VALUE, args);
    assertValues();
  }

  private void multiFieldAdderConfig(Engine engine, String fieldValue, Map<String, String> args) throws Exception {
    ETLStage source = getSourceStage();
    ETLStage transform = getTransformStage(fieldValue);
    ETLStage sink = getTableSink();
    ETLBatchConfig.Builder pipelineConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName());
    pipelineConfig.setEngine(engine);

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(pipelineConfig.build());
    ApplicationId appId = TEST_NAMESPACE.app("MultiFieldAdder");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    startAndWaitForRun(workflowManager, ProgramRunStatus.COMPLETED, args);
  }

  private void assertValues() throws Exception {
    DataSetManager<Table> outputManager = getTableDataset(SINK_TABLE_NAME);
    Table outputTable = outputManager.get();
    Row row = outputTable.get(Bytes.toBytes("1"));
    Assert.assertEquals(4, row.getColumns().size());
    Assert.assertNotNull(row.getString("key1"));
    Assert.assertEquals("value1", row.getString("key1"));
    Assert.assertNotNull(row.getString("key2"));
    Assert.assertEquals("value2", row.getString("key2"));
  }

  private ETLStage getSourceStage() {
    return new ETLStage("source",
                        new ETLPlugin(
                          FILE_SOURCE_PLUGIN_NAME,
                          BatchSource.PLUGIN_TYPE,
                          ImmutableMap.of(
                            "referenceName", "file_source",
                            "schema", INPUT_SCHEMA.toString(),
                            "format", "csv",
                            "skipHeader", "true",
                            "path", sourcePath),
                          CORE_PLUGINS_ARTIFACT));

  }

  private ETLStage getTransformStage(String fieldValue) {
    return new ETLStage("transform",
                        new ETLPlugin(
                          MULTI_FIELD_ADDER_PLUGIN_NAME,
                          Transform.PLUGIN_TYPE,
                          ImmutableMap.of(
                            "fieldValue", fieldValue,
                            "inputSchema", INPUT_SCHEMA.toString()),
                          FIELD_ADDER_ARTIFACT));
  }

  private ETLStage getTableSink() {
    return new ETLStage(
      "TableSink",
      new ETLPlugin(
        TABLE_SINK_PLUGIN_NAME,
        BatchSink.PLUGIN_TYPE,
        ImmutableMap.of(
          Properties.BatchReadableWritable.NAME, SINK_TABLE_NAME,
          Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "id",
          Properties.Table.PROPERTY_SCHEMA, "${schema}"
        ),
        CORE_PLUGINS_ARTIFACT));
  }
}

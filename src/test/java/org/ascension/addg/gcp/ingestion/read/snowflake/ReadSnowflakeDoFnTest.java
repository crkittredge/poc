package org.ascension.addg.gcp.ingestion.read.snowflake;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.read.snowflake.mocks.FakeSnowflakeBasicDataSource;
import org.ascension.addg.gcp.ingestion.read.snowflake.mocks.FakeSnowflakeDatabase;
import org.ascension.addg.gcp.ingestion.read.snowflake.mocks.FakeSnowflakeServicesImpl;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.*;

/**
 * Tests Snowflake read
 * Mocks adapted from:
 * https://github.com/apache/beam/blob/master/sdks/java/io/snowflake/
 * src/test/java/org/apache/beam/sdk/io/snowflake/test/unit/read/SnowflakeIOReadTest.java
 */
@RunWith(Parameterized.class)
public class ReadSnowflakeDoFnTest extends IngestionTest.Setup {
    private final String confFile;
    private final List<Row> expectedRows;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "snowflake-positive.conf" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     */
    public ReadSnowflakeDoFnTest(String confFile) {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, ReadSnowflakeDoFn.class);

        this.confFile = confFile;
        this.expectedRows = new ArrayList<>();
        var schema = Schema.builder()
                .addNullableStringField("field_1")
                .addNullableStringField("field_2")
                .addNullableStringField("field_3")
                .build();

        this.expectedRows.add(Row.withSchema(schema).addValues("1", "2", "3").build());
        this.expectedRows.add(Row.withSchema(schema).addValues("4", "5", "6").build());

        FakeSnowflakeDatabase.createTableWithElements("some_table", List.of("1,2,3", "4,5,6"));
    }

    @Override
    public void tearDown() {
        super.tearDown();

        // remove 'staging bucket' folder
        try (var stream = Files.walk(Paths.get("snowflake_staging/"))) {
            stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            //do nothing
        }
    }

    @Override
    public void executeTest() {
        var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
        var stepConfig = (ReadSnowflakeStep) config.getSteps().get(0);

        if (this.confFile.equals("snowflake-positive.conf")) {
            assertEquals("ReadSnowflake", stepConfig.getType());
            assertEquals("some_username_secret", stepConfig.getUsernameSecret());
            assertEquals("some_password_secret", stepConfig.getPasswordSecret());
            assertEquals("some_server.snowflakecomputing.com", stepConfig.getServerName());
            assertEquals("some_warehouse", stepConfig.getWarehouseName());
            assertEquals("some_database", stepConfig.getDatabaseName());
            assertEquals("some_schema", stepConfig.getSchemaName());
            assertEquals("SELECT * FROM some_table", stepConfig.getQuery());
            assertEquals("gs://some_gcs_bucket/", stepConfig.getStagingBucketName());
            assertEquals("gcs_integration_name", stepConfig.getStorageIntegrationName());
            assertEquals("_", stepConfig.getReplaceHeaderSpecialCharactersWith());
            assertEquals("PUBLIC", stepConfig.getRole());

        }

        var pipelineSpy = spy(this.pipeline.begin());

        stepConfig.setStagingBucketName("snowflake_staging/");

        var fakeDatasource = SnowflakeIO.<Row>read(new FakeSnowflakeServicesImpl())
                .withDataSourceConfiguration(SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
                        .withServerName(stepConfig.getServerName()));

        try (var credStaticMock = mockStatic(Utils.CredentialsHelper.class);
             var sfioStaticMock = mockStatic(SnowflakeIO.class, CALLS_REAL_METHODS)) {
            var sfioReadMock = spy(SnowflakeIO.Read.class);

            credStaticMock.when(() -> Utils.CredentialsHelper.getSecret(any(), any()))
                    .thenReturn("test_username").thenReturn("test_password");

            doReturn(fakeDatasource).when(sfioReadMock).withDataSourceConfiguration(any());

            sfioStaticMock.when(SnowflakeIO::read).thenReturn(sfioReadMock);

            var reader = Reader.of(stepConfig, this.pipelineOptions);
            var result = pipelineSpy.apply("ReadSource", reader);

            // check good records against expected
            PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

            // for snowflake we won't have bad records
            PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(List.of());

            this.pipeline.run();
        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}

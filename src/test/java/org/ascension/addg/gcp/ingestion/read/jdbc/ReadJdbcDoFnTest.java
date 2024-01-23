package org.ascension.addg.gcp.ingestion.read.jdbc;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.VariableString;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.joda.time.Instant;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mockStatic;

/**
 * Tests row-level scenarios for reading data from a JDBC source
 */
@RunWith(Parameterized.class)
public class ReadJdbcDoFnTest extends IngestionTest.Setup {
    private final String confFile;
    private final List<Row> expectedRows;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "jdbc-positive-partitions.conf" },
                { "jdbc-positive-no-partitions.conf" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     */
    public ReadJdbcDoFnTest(String confFile) throws SQLException {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, ReadJdbcDoFn.class);

        var datasource = JdbcIO.DataSourceConfiguration.create("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:testDB;create=true").buildDatasource();
        this.confFile = confFile;
        this.expectedRows = new ArrayList<>();

        var expectedSchema = Schema.builder()
                .addNullableInt32Field("id")
                .addNullableLogicalTypeField("name", VariableString.of(10))
                .addNullableBooleanField("boolean_value")
                .addNullableDoubleField("float_value")
                .addNullableInt64Field("long_value")
                .addNullableDateTimeField("timestamp_value")
                .build();

        for (int i = 0; i < 20; i++) {
            this.expectedRows.add(Row.withSchema(expectedSchema)
                    .addValues(i, "value_" + i, true, 5.5D, i * 800000L,
                            Instant.ofEpochMilli(LocalDateTime.of(2023, 1, 2, 12, 5, 0).toInstant(ZoneOffset.UTC).toEpochMilli()))
                    .build());
        }

        DatabaseTestHelper.configureProps(datasource, "test_username", "test_password");
        DatabaseTestHelper.createTable(datasource, "test_username.SCHEMA_TEST");
        DatabaseTestHelper.addInitialData(datasource, "test_username.SCHEMA_TEST",20);

    }

    @Override
    public void tearDown() {
        super.tearDown();

        //drop derby database
        try {
            DatabaseTestHelper.dropDatabase();
        } catch (SQLException e) {
            // ignore
        }
    }

    @Override
    public void executeTest() {
        var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
        var stepConfig = (ReadJdbcStep) config.getSteps().get(0);

        try (var credStaticMock = mockStatic(Utils.CredentialsHelper.class)) {
            credStaticMock.when(() -> Utils.CredentialsHelper.getSecret(any(), any()))
                    .thenReturn("test_username").thenReturn("test_password");

            try {
                var reader = Reader.of(stepConfig, this.pipelineOptions);

                var result = this.pipeline.apply("ReadSource", reader);

                // check good records against expected
                PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

                // for jdbc we won't have bad records
                PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(List.of());

                this.pipeline.run();
            } catch (ReflectiveOperationException e) {
                fail(e.getMessage());
            }
        }
    }
}

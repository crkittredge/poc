package org.ascension.addg.gcp.ingestion.write.bigquery;

import com.typesafe.config.Optional;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.write.WriteStep;

import java.io.Serializable;

@StepMap(name = "WriteBigQuery")
public class WriteBigQueryStep extends WriteStep {
    @Optional private BigQueryIO.Write.CreateDisposition createDisposition;
    @Optional private BigQueryIO.Write.WriteDisposition writeDisposition;
    @Optional private BigQueryIO.Write.Method writeMethod;
    @Optional private PartitionSpec partitionBy;
    @Optional private String bqLandProject;
    @Optional private String bqLandDataset;
    @Optional private String bqLandTable;
    @Optional private String bqErrorTableSuffix;

    public WriteBigQueryStep() {
        super(null, BigQueryWriter.class);
        this.createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
        this.writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
        this.writeMethod = BigQueryIO.Write.Method.FILE_LOADS;
        this.bqErrorTableSuffix = "_error";
    }

    public static class PartitionSpec implements Serializable {
        private String columnName;
        private String dataType;

        public PartitionSpec() {
            //do nothing
        }

        public final String getColumnName() {
            return this.columnName;
        }

        public final void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public final String getDataType() {
            return this.dataType;
        }

        public final void setDataType(String dataType) {
            this.dataType = dataType;
        }
    }

    public final BigQueryIO.Write.CreateDisposition getCreateDisposition() {
        return this.createDisposition;
    }

    public final void setCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
        this.createDisposition = createDisposition;
    }

    public final BigQueryIO.Write.WriteDisposition getWriteDisposition() {
        return this.writeDisposition;
    }

    public final void setWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
        this.writeDisposition = writeDisposition;
    }

    public final BigQueryIO.Write.Method getWriteMethod() {
        return this.writeMethod;
    }

    public final void setWriteMethod(BigQueryIO.Write.Method writeMethod) {
        this.writeMethod = writeMethod;
    }

    public final PartitionSpec getPartitionBy() {
        return this.partitionBy;
    }

    public final void setPartitionBy(PartitionSpec partitionBy) {
        this.partitionBy = partitionBy;
    }

    public final String getBqLandProject() {
        return this.bqLandProject;
    }

    public final void setBqLandProject(String bqLandProject) {
        this.bqLandProject = bqLandProject;
    }

    public final String getBqLandDataset() {
        return this.bqLandDataset;
    }

    public final void setBqLandDataset(String bqLandDataset) {
        this.bqLandDataset = bqLandDataset;
    }

    public final String getBqLandTable() {
        return this.bqLandTable;
    }

    public final void setBqLandTable(String bqLandTable) {
        this.bqLandTable = bqLandTable;
    }

    public String getBqErrorTableSuffix() {
        return this.bqErrorTableSuffix;
    }

    public void setBqErrorTableSuffix(String bqErrorTableSuffix) {
        this.bqErrorTableSuffix = bqErrorTableSuffix;
    }
}

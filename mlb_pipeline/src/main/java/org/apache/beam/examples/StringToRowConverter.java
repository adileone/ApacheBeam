package org.apache.beam.examples;

import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.transforms.DoFn;

public class StringToRowConverter extends DoFn<String, TableRow> {

    private static final long serialVersionUID = 1L;

    private TableSchema schema;


    public StringToRowConverter(TableSchema schema) {
        schema=this.schema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = new TableRow();
        
        String[] parts = c.element().split(",");
        List<TableFieldSchema> fields = schema.getFields();
        
        for (int i = 0; i < parts.length; i++) {
            row.set(fields.get(i).toString(), parts[i]);
        }
        c.output(row);
    }
};
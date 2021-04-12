package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.transforms.DoFn;

public class StringToRowConverter extends DoFn<String, TableRow> {

    private static final long serialVersionUID = 1L;

    private String header;

    public StringToRowConverter(String header) {
        this.header=header;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        
        TableRow row = new TableRow();
        
        String[] parts = c.element().split(",");
        String[] columnNames = header.split(",");
        
        for (int i = 0; i < parts.length; i++) {
                row.set(columnNames[i], parts[i]);
            }
            c.output(row);
        }
    };
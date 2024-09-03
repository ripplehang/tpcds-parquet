package com.querifylabs.tools.tpcds.parquet;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@SuppressWarnings("unused")
public class Main implements Callable<Void> {

    @CommandLine.Option(names = {"--dir"}, required = true, description = "Path to TPC-DS data.")
    private String dir;

    @CommandLine.Option(names = {"--table"}, required = true, description = "Table name.")
    private String tableName;

    @CommandLine.Option(names = {"--partitioning"}, description = "Partitioning column (optional).")
    private String partitioning;

    @CommandLine.Option(names = {"--format"}, required = true, description = "Data format.")
    private String format;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Void call() throws Exception {
        Runner.run(dir, tableName, partitioning, Format.resolve(format));
        return null;
    }
}

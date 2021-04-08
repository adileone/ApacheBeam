package org.apache.beam.examples;

import java.util.HashMap;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;

// mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.MlbPipeline -Dexec.args="--runner=DataflowRunner --project=exemplary-works-305313 --stagingLocation=gs://beambinaries/staging --templateLocation=gs://beambinaries/templates/customTemplate1 --region=europe-west6 --input=default"

public class MlbPipeline {

  public interface MyOptions extends DataflowPipelineOptions {

    ValueProvider<String> getInput();

    void setInput(ValueProvider<String> value);
  }

  public static void main(String[] args) {
    runPipeline(args);
  }

  public static void runPipeline(String[] args) {

    HashMap<String, String> names = new HashMap<>();
    names.put("BAL", "Orioles");
    names.put("CWS", "White Sox");
    names.put("ANA", "Angels");
    names.put("BOS", "Red Sox");
    names.put("CLE", "Indians");
    names.put("OAK", "Athletics");
    names.put("NYY", "Yankees");
    names.put("DET", "Tigers");
    names.put("SEA", "Mariners");
    names.put("TB", "Rays");
    names.put("KC", "Royals");
    names.put("TEX", "Rangers");
    names.put("TOR", "Blue Jays");
    names.put("MIN", "Twins");
    names.put("ATL", "Braves");
    names.put("CHC", "Cubs");
    names.put("ARZ", "Diamondbacks");
    names.put("FLA", "Marlins");
    names.put("CIN", "Reds");
    names.put("COL", "Rockies");
    names.put("NYM", "Mets");
    names.put("HOU", "Astros");
    names.put("LA", "Dodgers");
    names.put("PHI", "Phillies");
    names.put("MLW", "Brewers");
    names.put("SD", "Padres");
    names.put("WAS", "Nationals");
    names.put("PIT", "Pirates");
    names.put("SF", "Giants");
    names.put("STL", "Cardinals");

    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    Pipeline p = Pipeline.create(options);

    // Create and set your PipelineOptions.
    // DataflowPipelineOptions options =
    // PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    // For Cloud execution, set the Cloud Platform project, staging location,
    // and specify DataflowRunner.
    // options.setProject("exemplary-works-305313");
    // options.setStagingLocation("gs://beambinaries/binaries");
    // options.setRunner(DataflowRunner.class);
    // options.setRegion("europe-west6");
    // options.setGcpTempLocation("gs://beambinaries/binaries");

    // // For DirectRunner
    // PipelineOptions options = PipelineOptionsFactory.create();

    // Create the Pipeline with the specified options.
    // Pipeline p = Pipeline.create(options);

    // p.apply("Read Players from CSV in
    // bucket",TextIO.read().from("gs://mls-bucket/mlb_players.csv"))
    p.apply("Read Players from CSV in bucket", TextIO.read().from(options.getInput().get()))
        .apply("Remove header row", Filter.by((String row) -> !(row.startsWith("\"Name\","))))
        .apply("Remove empty rows", Filter.by((new SerializableFunction<String, Boolean>() {
          private static final long serialVersionUID = 1L;

          @Override
          public Boolean apply(String input) {

            String[] array = input.replace("\"", "").split(",");
            return (array.length == 6);
          }
        }

        ))).apply("Prepare Join Player", ParDo.of(new DoFn<String, String>() {
          private static final long serialVersionUID = 1L;

          @ProcessElement
          public void processElement(ProcessContext ctx) {

            String[] split = ctx.element().replace("\"", "").trim().split(",");

            String name = split[0];
            String team = names.get(split[1].trim());
            String position = split[2];
            Double height = parseDouble(split[3]);
            Double weight = parseDouble(split[4]);
            Double age = parseDouble(split[5]);

            Player player = new Player(name, team, position, height, weight, age);
            ctx.output(player.getTeam() + "---->" + player.toString());
          }
        })).apply("Write Result1", TextIO.write().to("gs://mlb_results1"));

    // Check pipeline status
    p.run().waitUntilFinish();
  }

  private static double parseDouble(String s) {
    try {
      return Double.valueOf(s);
    } catch (NumberFormatException e) {
      return 0.0;
    }
  }
}

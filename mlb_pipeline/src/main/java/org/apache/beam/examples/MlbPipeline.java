package org.apache.beam.examples;

import java.sql.ResultSet;
import java.util.HashMap;

// import org.apache.beam.runners.dataflow.DataflowRunner;
// import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class MlbPipeline {

  public static void main(String[] args) {

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

    // // Create and set your PipelineOptions.
    // DataflowPipelineOptions options =
    // PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    // // For Cloud execution, set the Cloud Platform project, staging location,
    // // and specify DataflowRunner.
    // options.setProject("exemplary-works-305313");
    // options.setStagingLocation("gs://beambinaries/binaries");
    // options.setRunner(DataflowRunner.class);

    PipelineOptions options = PipelineOptionsFactory.create();

    // Create the Pipeline with the specified options.
    Pipeline p = Pipeline.create(options);

    DataSourceConfiguration mysqlConfig = JdbcIO.DataSourceConfiguration
        .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://35.204.50.197:3306/mls_db").withUsername("root")
        .withPassword("root");

    PCollection<KV<String, Team>> teams = p

        .apply(JdbcIO.<KV<String, Team>>read().withDataSourceConfiguration(mysqlConfig)
            .withQuery("SELECT * FROM mlb_teams").withCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Team.class)))
            .withRowMapper(new JdbcIO.RowMapper<KV<String, Team>>() {
              private static final long serialVersionUID = 1L;

              public KV<String, Team> mapRow(ResultSet resultSet) throws Exception {
                Team team = new Team(resultSet.getString(1), resultSet.getDouble(2), resultSet.getDouble(3));
                return KV.of(team.getName(), team);
              }
            }));

    PCollection<KV<String, Player>> players = p

        .apply(TextIO.read().from("gs://mls-bucket/mlb_players.csv"))
        .apply("Remove header row", Filter.by((String row) -> !(row.startsWith("\"Name\","))))
        .apply("Remove empty rows", Filter.by((new SerializableFunction<String, Boolean>() {
          private static final long serialVersionUID = 1L;

          @Override
          public Boolean apply(String input) {

            String[] array = input.replace("\"", "").split(",");
            return (array.length == 6);
          }
        }

        ))).apply(ParDo.of(new DoFn<String, KV<String, Player>>() {
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
            ctx.output(KV.of(player.getTeam(), player));
          }
        }));

    final TupleTag<Team> teamsTag = new TupleTag<>();
    final TupleTag<Player> playersTag = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple.of(playersTag, players).and(teamsTag, teams)
        .apply(CoGroupByKey.create());
    
    PDone res1 = joined.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {

      private static final long serialVersionUID = 1L;

      @ProcessElement
      public void processElement(ProcessContext ctx) {

        Iterable<Player> players = ctx.element().getValue().getAll(playersTag);
        Team team = ctx.element().getValue().getOnly(teamsTag);

        for (Player p : players) {
          ctx.output(p.toString() + " -> " + team.toString());
        }
      }
    }))
    .apply(TextIO.write().to("/media/alessandro/storage/EsercizioBeam/output.txt"));

    PDone res2 = joined.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, Double>>() {

      private static final long serialVersionUID = 1L;

      @ProcessElement
      public void processElement(ProcessContext ctx) {

        Iterable<Player> players = ctx.element().getValue().getAll(playersTag);
        Team team = ctx.element().getValue().getOnly(teamsTag);

        for (Player p : players) {
          ctx.output(KV.of(p.getTeam(), p.getHeight()));
        }
      }
    }))
    .apply(Mean.perKey())
    .apply(ParDo.of(new DoFn<KV<String, Double>, String>() {

      private static final long serialVersionUID = 1L;

      @ProcessElement
      public void processElement(ProcessContext ctx) {

          ctx.output(ctx.element().getKey()+"-->"+ctx.element().getValue());
        }
      }))
    .apply(TextIO.write().to("/media/alessandro/storage/EsercizioBeam/output2.txt"));

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

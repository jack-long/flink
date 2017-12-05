package regression;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class LinearRegressionTest {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.getRequired("input");
        final String output = params.getRequired("output");

        final int dimension = params.getInt("dimention", 1);

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // operate in Event-time
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        // DataStream<ArrayList<Double>> dataStream = env.readTextFile(input)
        //        .map(new VectorExtractor());

        DataStream<RegressionData> dataStream = env.addSource(new streamFromFile(input))
                .map(new PrepareData(dimension)); //.assignTimestampsAndWatermarks(new TimestampExtractor());

        // remove the original timestamp at the fist position.
        // DataStream<List<Double>> preparedDataStream = dataStream.map(new GetValues());

        // dataStream.writeAsText("output", OVERWRITE).setParallelism(1);

        // Reference FlinkML & Spark MLlib
        LinearRegression lr = new LinearRegression(dimension);

        // LinearRegression.fit(input) => Tuple3<input, model, error_score>
        DataStream<Tuple2<RegressionData, RegressionModel>> inputWithModel = lr.fit(dataStream);

        // write to file
        inputWithModel.writeAsText(output, OVERWRITE); //.setParallelism(1);
        // print
        // inputWithModel.print();

        // LinearRegression.predict(Tuple2<input, model>) => prediction
        // DataStream<Tuple2<Double, Double>> predictions = lr.predict(inputWithModel);
        // predictions.writeAsText("output_prediction", OVERWRITE);

        // Another approach.
        // DataStream<Double> predictions = lr.fitPredict(dataStream);

        // predictions.print();

        // run the prediction pipeline
        env.execute("Linear predictor");
    }


    /**
     * Generate a stream line by line from a file.
     */
    public static class streamFromFile implements SourceFunction<String> {
        boolean isRunning;
        String inputFile;

        streamFromFile(String inputFile) throws FileNotFoundException {
            isRunning = true;
            this.inputFile = inputFile;
        }

        /**
         * Starts the source. Implementations can use the {@link SourceContext} emit
         * elements.
         *
         * @param ctx The context to emit elements to and for accessing locks.
         */
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            FileReader fr;
            BufferedReader br;
            fr = new FileReader(inputFile);
            br = new BufferedReader(fr);
            String line;
            while (isRunning) {
                if ((line = br.readLine()) != null) {
                    ctx.collect(line);
                } else {
                    break;
                }
                Thread.sleep(10);
            }
            fr.close();
            fr = null;
            br.close();
            br = null;
        }

        /**
         * Cancels the source.
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * Assign tiemstamp and watermarks.
     * <p>
     * Get timestamp at the first value.
     */
    public static class TimestampExtractor implements AssignerWithPeriodicWatermarks<ArrayList<Double>> {
        private final long maxOutOfOrderness = 3000; // 3 seconds

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(ArrayList<Double> data, long previousElementTimestamp) {
            long newTimestamp = data.get(0).longValue();
            if (newTimestamp > currentMaxTimestamp) {
                currentMaxTimestamp = newTimestamp;
            }
            return newTimestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }

    public static class PrepareData implements MapFunction<String, RegressionData> {
        static long id = 0;
        int dimension;

        PrepareData (int dimension){
            this.dimension = dimension;
        }

        @Override
        public RegressionData map(String s) throws Exception {
            Double timestamp;
            List<Double> values = new ArrayList<>(dimension);
            Double label;
            int index = 0;

            String[] elements = s.split(",");

            id ++;
            timestamp = new Double(id);
            // timestamp = Calendar.getTime().getTime();

            for (int i = 0; i < dimension; i++){
                values.add(new Double(elements[index++]));
            }

            label = new Double(elements[index]);

            return new RegressionData(id, values, label, timestamp);
        }
    }

    /**
     * Get the valid data values.
     * <p>
     * Remove original timestamples at the beginning of the List.
     */
    public static class GetValues implements MapFunction<List<Double>, List<Double>> {
        @Override
        public List<Double> map(List<Double> input) throws Exception {
            return new ArrayList<>(input.subList(1, input.size()));
        }
    }

}

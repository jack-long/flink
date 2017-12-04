package regression;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Usage
 *
 * LinearRegression lr = new LinearRegression()  // with default value for parameters
 *              .setLearningRate(0.1)  // customize parameter
 *              .setDimentions(2)
 *
 * val trainingDS: DataStream[LabeledVector] = env.addSource().map();
 * val testingDS: DataStream[Vector] = env.addSource().map();
 *
 * model = lr.fit(trainingDS);
 *
 * val predictions = lr.predict(testingDS, model)
 */

public class LinearRegression {
    private Double learningRate;
    private int dimensions;
    private int batchSize = 10;
    /** Used to assign key to the values */
    private int partition = 4;

    private long sampleCount = 0;

    /**
     * With default learningRate = 0.0002
     * @param dimensions
     */
    public LinearRegression(int dimensions){
        this(dimensions, 0.0002);
    }

    public LinearRegression(int dimensions, Double learningRate){
        this.dimensions = dimensions;
        this.learningRate = learningRate;
    }

    /**
     * Take a stream of input data and output a stream of input data with the model appended.
     * This is supervised learning, the label is supposed to be at the last position.
     *
     * @param trainingData
     * @return
     * @throws Exception
     */
    public DataStream<Tuple3<RegressionData, List<Double>, Double>> fit(DataStream<RegressionData> trainingData) throws Exception {
        return trainingData.map(new AssignKey(partition)).keyBy(0).map(new PartialModelBuilder(learningRate, dimensions, batchSize));
    }

    // @Override
    public DataStream<Tuple2<Double, Double>> predict(DataStream<ArrayList<Double>> testingData,
                                                      DataStream<ArrayList<Double>> model)
            throws Exception {
                ConnectedStreams<ArrayList<Double>, ArrayList<Double>> combinedStream = testingData.connect(model);
                DataStream<Tuple2<Double, Double>> prediction = combinedStream
                        .flatMap(new PredictionModel(dimensions)).setParallelism(1);
                return prediction;
            }


    public static class PredictionModel extends RichCoFlatMapFunction<ArrayList<Double>, ArrayList<Double>, Tuple2<Double, Double>> {
        int dimensions;

        public PredictionModel(int dimensions) {
            this.dimensions = dimensions;
        }

        private transient ValueState<ArrayList<Double>> modelState;

        @Override
        public void flatMap1(ArrayList<Double> value, Collector<Tuple2<Double, Double>> out) throws Exception {
            ArrayList<Double> model = modelState.value();

            assert value.size() - 1 == model.size();

            Double prediction = .0;
            int i = 0;
            while( i < model.size()){
                prediction += value.get(i) * model.get(i);
                i++;
            }

            out.collect(new Tuple2<>(prediction, value.get(i)));
        }

        @Override
        public void flatMap2(ArrayList<Double> model, Collector<Tuple2<Double, Double>> out) throws Exception {
            modelState.update(model);
        }

        @Override
        public void open(Configuration config) {
            // obtain key-value state for prediction model
            ValueStateDescriptor<ArrayList<Double>> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "linearRegressionModel",
                            // type information of state
                            TypeInformation.of(new TypeHint<ArrayList<Double>>() {}),
                            // default value of state
                            new ArrayList<>(Collections.nCopies(dimensions, 0.0)));
            modelState = getRuntimeContext().getState(descriptor);
        }
    }

    public static class PartialModelBuilder extends RichMapFunction<Tuple2<Integer, RegressionData>, Tuple3<RegressionData, List<Double>, Double>> {

        private Double learningRate;
        private int dimensions;
        private int batchSize;

        private int applyCount = 0;

        private static final long serialVersionUID = 1L;

        // This is where we store our persistent state
        // the model, the mini-batch data, batchSize
        private transient ValueState<Tuple2<List<Double>, List<List<Double>>>> modelState;


        public PartialModelBuilder(Double learningRate, int dimensions, int batchSize) {
            this.learningRate = learningRate;
            this.dimensions = dimensions;
            this.batchSize = batchSize;
        }

        // Here we initialize our state by creating a model as a weight vector of zeros
        @Override
        public void open(Configuration config) {
            List<Double> allZeroes = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
            // obtain key-value state for prediction model
            // TODO: Do random assignment of weights instead of all zeros?
            ValueStateDescriptor<Tuple2<List<Double>, List<List<Double>>>> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "modelState",
                            // type information of state
                            TypeInformation.of(new TypeHint<Tuple2<List<Double>, List<List<Double>>>>() {}),
                            // default value of state
                            new Tuple2<>(allZeroes, new ArrayList<>()));
            modelState = getRuntimeContext().getState(descriptor);
        }

        private Double squaredError(Double truth, Double prediction) {
            return 0.5 * (prediction - truth) * (prediction - truth);
        }

        /**
         * This is where the model update happens.
         *
         * It takes a batch of input data to update model.
         */
        private Tuple2<List<Double>, Double> buildPartialModel(List<Double> trainingData) throws Exception{
            int batchSize = 0;

            // Get the old model and stored samples
            Tuple2<List<Double>, List<List<Double>>> storedData = modelState.value();
            List<Double> regressionModel = storedData.f0;
            List<List<Double>> trainingBatch = storedData.f1;

            trainingBatch.add(trainingData);

            // If training data is not enough for updating model,
            // just add into collection and return original model.
            if(trainingBatch.size() < this.batchSize){
                // add new data into collection
                modelState.update(new Tuple2<>(regressionModel, trainingBatch));
                // return model and it's precision(-1 if not updated)
                return new Tuple2<>(regressionModel, new Double(-1));
            }

            ArrayList<Double> gradientSum = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
            Double error = .0;

            for (List<Double> sample : trainingBatch) {
                // For each example in the batch, find it's error derivative
                batchSize++;
                Double truth = sample.get(sample.size() - 1);
                Double prediction = predict(regressionModel, sample);
                error += squaredError(truth, prediction);
                Double derivative = prediction - truth;
                for (int i = 0; i < regressionModel.size(); i++) {
                    Double weightGradient = derivative * sample.get(i);
                    Double currentSum = gradientSum.get(i);
                    gradientSum.set(i, currentSum + weightGradient);
                }
            }
            for (int i = 0; i < regressionModel.size(); i++) {
                Double oldWeight = regressionModel.get(i);
                // Double currentLR = Math.sqrt(applyCount);
                Double change = learningRate * (gradientSum.get(i) / batchSize);
                regressionModel.set(i, oldWeight - change);
            }

            // update state
            modelState.update(new Tuple2<>(regressionModel, new ArrayList<>()));

            // return regressionModel;
            return new Tuple2<>(regressionModel, error / batchSize);
        }

        private Double predict(List<Double> model, List<Double> sample){
            Double prediction = .0;
            for(int i = 0; i < model.size(); i++){
                prediction += model.get(i) * sample.get(i);
            }
            return prediction;
        }

        // The apply function calculates the updated model according to the new batch and updates the state
        @Override
        public Tuple3<RegressionData, List<Double>, Double> map(Tuple2<Integer, RegressionData> value) throws Exception {
            List<Double> trainingData = new ArrayList<>(value.f1.values);
            trainingData.add(value.f1.label);
            Tuple2<List<Double>, Double> updateValues = buildPartialModel(trainingData);
            return new Tuple3<>(value.f1, updateValues.f0, updateValues.f1);
        }
    }

    public static class AssignKey implements MapFunction<RegressionData, Tuple2<Integer, RegressionData>> {
        int partition;

        AssignKey(int partition){
            this.partition = partition;
        }

        @Override
        public Tuple2<Integer, RegressionData> map(RegressionData value) throws Exception{
            return new Tuple2<>(new Random().nextInt(partition), value);
        }
    }
}


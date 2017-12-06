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
 * <p>
 * LinearRegression lr = new LinearRegression()  // with default value for parameters
 * .setLearningRate(0.1)  // customize parameter
 * .setDimentions(2)
 * <p>
 * val trainingDS: DataStream[LabeledVector] = env.addSource().map();
 * val testingDS: DataStream[Vector] = env.addSource().map();
 * <p>
 * model = lr.fit(trainingDS);
 * <p>
 * val predictions = lr.predict(testingDS, model)
 */

public class LinearRegression {
    private Double learningRate;
    private int dimensions;
    private int batchSize;
    /**
     * Used to assign key to the values
     */
    private int partition = 4;

    private long sampleCount = 0;

    /**
     * With default learningRate = 0.0002
     *
     * @param dimensions
     */
    public LinearRegression(int dimensions) {
        this(dimensions, 0.0002, 10);
    }

    public LinearRegression(int dimensions, Double learningRate, int batchSize) {
        this.dimensions = dimensions;
        this.learningRate = learningRate;
        this.batchSize = batchSize;
    }

    /**
     * Take a stream of input data and output a stream of input data with the model appended.
     * This is supervised learning, the label is supposed to be at the last position.
     *
     * @param trainingData
     * @return
     * @throws Exception
     */
    public DataStream<Tuple2<RegressionData, RegressionModel>> fit(DataStream<RegressionData> trainingData) throws Exception {
        return trainingData.map(new AssignKey(partition)).keyBy(0).map(new PartialModelBuilder(learningRate, dimensions, batchSize));
    }

    /**
     * Take a stream of input data, if label exist, update model, else predict.
     *
     * @param trainingData
     * @return
     * @throws Exception
     */
    public DataStream<Tuple3<RegressionData, RegressionModel, Double>> fitAndPredict(DataStream<RegressionData> trainingData) throws Exception {
        return trainingData.map(new AssignKey(partition)).keyBy(0).map(new FitAndPredict(learningRate, dimensions, batchSize));
    }

    /**
     * Just do prediction // TODO
     * @param testingData
     * @param model
     * @return
     * @throws Exception
     */
    public DataStream<Tuple2<Double, Double>> predict(DataStream<ArrayList<Double>> testingData,
                                                      DataStream<ArrayList<Double>> model)
            throws Exception {
        ConnectedStreams<ArrayList<Double>, ArrayList<Double>> combinedStream = testingData.connect(model);
        DataStream<Tuple2<Double, Double>> prediction = combinedStream
                .flatMap(new PredictionModel(dimensions)).setParallelism(1);
        return prediction;
    }

    /**
     * Save the model locally. // TODO
     *
     * Overwrite a local file when new model is produced.
     *
     * @param dataAndModel
     * @param output
     */
    public void saveModel(DataStream<Tuple2<RegressionData, RegressionModel>> dataAndModel, String output){
        dataAndModel.writeAsText(output);
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
            while (i < model.size()) {
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

    public static class PartialModelBuilder extends RichMapFunction<Tuple2<Integer, RegressionData>, Tuple2<RegressionData, RegressionModel>> {

        private Double learningRate;
        private int dimensions;
        private int batchSize;

        private int applyCount = 0;

        private static final long serialVersionUID = 1L;

        /**
         * The persistent state stores the model and the mini-batch data.
         */
        private transient ValueState<Tuple2<RegressionModel, List<List<Double>>>> modelState;


        public PartialModelBuilder(Double learningRate, int dimensions, int batchSize) {
            this.learningRate = learningRate;
            this.dimensions = dimensions;
            this.batchSize = batchSize;
        }

        @Override
        public void open(Configuration config) {
            RegressionModel initialModel = new RegressionModel(dimensions);
            // obtain key-value state for prediction model
            // TODO: Do random assignment of weights instead of all zeros?
            ValueStateDescriptor<Tuple2<RegressionModel, List<List<Double>>>> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "modelState",
                            // type information of state
                            TypeInformation.of(new TypeHint<Tuple2<RegressionModel, List<List<Double>>>>() {}),
                            // default value of state
                            new Tuple2<>(initialModel, new ArrayList<>()));
            modelState = getRuntimeContext().getState(descriptor);
        }

        private Double squaredError(Double truth, Double prediction) {
            return 0.5 * (prediction - truth) * (prediction - truth);
        }

        /**
         * This is where the model update happens.
         * It takes a batch of input data to update model.
         */
        private RegressionModel buildPartialModel(List<Double> trainingData) throws Exception {

            // Get the old model and stored samples
            Tuple2<RegressionModel, List<List<Double>>> storedData = modelState.value();
            RegressionModel currentModel = storedData.f0;
            List<List<Double>> miniBatch = storedData.f1;

            miniBatch.add(trainingData);

            // If training data is not enough for updating model,
            // just add new data into collection and return original model.
            if (miniBatch.size() < batchSize) {
                // add new data into collection
                modelState.update(new Tuple2<>(currentModel, miniBatch));
                return currentModel;
            }

            List<Double> gradientSum = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
            Double error = .0;
            List<Double> weights = currentModel.weights;

            for (List<Double> sample : miniBatch) {
                // For each example in the batch, find it's error derivative
                batchSize++;
                Double truth = sample.get(sample.size() - 1);
                Double prediction = predict(weights, sample);
                error += squaredError(truth, prediction);
                Double derivative = prediction - truth;
                for (int i = 0; i < weights.size(); i++) {
                    Double weightGradient = derivative * sample.get(i);
                    Double currentSum = gradientSum.get(i);
                    gradientSum.set(i, currentSum + weightGradient);
                }
            }
            for (int i = 0; i < weights.size(); i++) {
                Double oldWeight = weights.get(i);
                // Double currentLR = Math.sqrt(applyCount);
                Double change = learningRate * (gradientSum.get(i) / batchSize);
                weights.set(i, oldWeight - change);
            }

            RegressionModel newModel = new RegressionModel(weights, error / batchSize);
            // update state
            modelState.update(new Tuple2<>(newModel, new ArrayList<>()));

            return newModel;
        }

        private Double predict(List<Double> weights, List<Double> sample) {
            Double prediction = .0;
            for (int i = 0; i < weights.size(); i++) {
                prediction += weights.get(i) * sample.get(i);
            }
            return prediction;
        }

        /**
         * Calculates the updated model according to the new batch and updates the state.
         */
        @Override
        public Tuple2<RegressionData, RegressionModel> map(Tuple2<Integer, RegressionData> value) throws Exception {
            List<Double> trainingData = new ArrayList<>(value.f1.values);
            trainingData.add(value.f1.label);
            RegressionModel newModel = buildPartialModel(trainingData);
            return new Tuple2<>(value.f1, newModel);
        }
    }

    public static class FitAndPredict extends RichMapFunction<Tuple2<Integer, RegressionData>,
            Tuple3<RegressionData, RegressionModel, Double>> {

        private Double learningRate;
        private int dimensions;
        private int batchSize;

        private int applyCount = 0;

        private static final long serialVersionUID = 1L;

        /**
         * The persistent state stores the model and the mini-batch data.
         */
        private transient ValueState<Tuple2<RegressionModel, List<List<Double>>>> modelState;


        public FitAndPredict(Double learningRate, int dimensions, int batchSize) {
            this.learningRate = learningRate;
            this.dimensions = dimensions;
            this.batchSize = batchSize;
        }

        /**
         * Calculates the updated model according to the new batch and updates the state.
         */
        @Override
        public Tuple3<RegressionData, RegressionModel, Double> map(Tuple2<Integer, RegressionData> value) throws Exception {
            RegressionModel latestModel;

            List<Double> sample = new ArrayList<>(value.f1.values); //TODO: necessary?

            if(value.f1.label == null){
                latestModel = modelState.value().f0;
            } else {
                sample.add(value.f1.label);
                latestModel = buildPartialModel(sample);
            }
            Double prediction = predict(latestModel.weights, sample);
            return new Tuple3<>(value.f1, latestModel, prediction);
        }

        @Override
        public void open(Configuration config) {
            RegressionModel initialModel = new RegressionModel(dimensions);
            // obtain key-value state for prediction model
            // TODO: Do random assignment of weights instead of all zeros?
            ValueStateDescriptor<Tuple2<RegressionModel, List<List<Double>>>> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "modelState",
                            // type information of state
                            TypeInformation.of(new TypeHint<Tuple2<RegressionModel, List<List<Double>>>>() {}),
                            // default value of state
                            new Tuple2<>(initialModel, new ArrayList<>()));
            modelState = getRuntimeContext().getState(descriptor);
        }

        private Double squaredError(Double truth, Double prediction) {
            return 0.5 * (prediction - truth) * (prediction - truth);
        }

        /**
         * This is where the model update happens.
         * It takes a batch of input data to update model. So if there is not enough samples, update will not happen.
         * Label is assumed to be placed at the last position.
         */
        private RegressionModel buildPartialModel(List<Double> trainingData) throws Exception {

            // Get the old model and stored samples
            Tuple2<RegressionModel, List<List<Double>>> storedData = modelState.value();
            RegressionModel currentModel = storedData.f0;
            List<List<Double>> miniBatch = storedData.f1;

            miniBatch.add(trainingData);

            // If training data is not enough for updating model,
            // just add new data into collection and return original model.
            if (miniBatch.size() < batchSize) {
                // add new data into collection
                modelState.update(new Tuple2<>(currentModel, miniBatch));
                return currentModel;
            }

            List<Double> gradientSum = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
            Double error = .0;
            List<Double> weights = currentModel.weights;

            for (List<Double> sample : miniBatch) {
                // For each example in the batch, find it's error derivative
                batchSize++;
                Double truth = sample.get(sample.size() - 1);
                Double prediction = predict(weights, sample);
                error += squaredError(truth, prediction);
                Double derivative = prediction - truth;
                for (int i = 0; i < weights.size(); i++) {
                    Double weightGradient = derivative * sample.get(i);
                    Double currentSum = gradientSum.get(i);
                    gradientSum.set(i, currentSum + weightGradient);
                }
            }
            for (int i = 0; i < weights.size(); i++) {
                Double oldWeight = weights.get(i);
                // Double currentLR = Math.sqrt(applyCount);
                Double change = learningRate * (gradientSum.get(i) / batchSize);
                weights.set(i, oldWeight - change);
            }

            RegressionModel newModel = new RegressionModel(weights, error / batchSize);
            // update state
            modelState.update(new Tuple2<>(newModel, new ArrayList<>()));

            return newModel;
        }

        private Double predict(List<Double> weights, List<Double> sample) {
            Double prediction = .0;
            for (int i = 0; i < weights.size(); i++) {
                prediction += weights.get(i) * sample.get(i);
            }
            return prediction;
        }


    }

    public static class AssignKey implements MapFunction<RegressionData, Tuple2<Integer, RegressionData>> {
        int partition;

        AssignKey(int partition) {
            this.partition = partition;
        }

        @Override
        public Tuple2<Integer, RegressionData> map(RegressionData value) throws Exception {
            return new Tuple2<>(new Random().nextInt(partition), value);
        }
    }
}


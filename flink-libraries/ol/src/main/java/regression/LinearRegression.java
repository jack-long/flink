package regression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import common.Predictor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

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
// TODO: add noise in data
// TODO: ? How to implement/extend Predictor

public class LinearRegression {
    private Double learningRate;
    private int dimensions;

    private long sampleCount = 0;

    public LinearRegression(int dimensions){
        this.dimensions = dimensions;
        this.learningRate = new Double(0.0002);
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
    public DataStream<Tuple2<ArrayList<Double>, Double>> fit(DataStream<ArrayList<Double>> trainingData) throws Exception {
        return trainingData.countWindowAll(10).apply(new PartialModelBuilder(learningRate, dimensions)); //.setParallelism(1);
    }

//    @Override
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

    public static class PartialModelBuilder extends RichAllWindowFunction<ArrayList<Double>, Tuple2<ArrayList<Double>, Double>, GlobalWindow> {

        public PartialModelBuilder(Double learningRate, int dimensions) {
            this.learningRate = learningRate;
            this.dimensions = dimensions;
        }

        private Double learningRate;
        private int dimensions;

        private int applyCount = 0;

        private static final long serialVersionUID = 1L;

        // This is where we store our persistent state, which is the model.
        private transient ValueState<ArrayList<Double>> modelState;

        // Here we initialize our state by creating a model as a weight vector of zeros
        @Override
        public void open(Configuration config) {
            ArrayList<Double> allZeroes = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
            // obtain key-value state for prediction model
            // TODO: Do random assignment of weights instead of all zeros?
            ValueStateDescriptor<ArrayList<Double>> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "modelState",
                            // type information of state
                            TypeInformation.of(new TypeHint<ArrayList<Double>>() {}),
                            // default value of state
                            allZeroes);
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
        private Tuple2<ArrayList<Double>, Double> buildPartialModel(Iterable<ArrayList<Double>> trainingBatch) throws Exception{
            int batchSize = 0;
            // Get the old model
            ArrayList<Double> regressionModel = modelState.value();
            ArrayList<Double> gradientSum = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
            Double error = .0;

            for (ArrayList<Double> sample : trainingBatch) {
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
//                Double currentLR = Math.sqrt(applyCount);
                Double change = learningRate * (gradientSum.get(i) / batchSize);
                regressionModel.set(i, oldWeight - change);
            }
//            return regressionModel;
            return new Tuple2<>(regressionModel, error / batchSize);
        }

        private Double predict(ArrayList<Double> model, ArrayList<Double> sample){
            Double prediction = .0;
            for(int i = 0; i < model.size(); i++){
                prediction += model.get(i) * sample.get(i);
            }
            return prediction;
        }

        // The apply function calculates the updated model according to the new batch and updates the state
        @Override
        public void apply(GlobalWindow window, Iterable<ArrayList<Double>> values, Collector<Tuple2<ArrayList<Double>, Double>> out) throws Exception {
            this.applyCount++;

            Tuple2<ArrayList<Double>, Double> updateValues = buildPartialModel(values);
            ArrayList<Double> updatedModel = updateValues.f0;
            modelState.update(updatedModel);
            out.collect(updateValues);
        }
    }
}


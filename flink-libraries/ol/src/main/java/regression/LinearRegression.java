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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

/**
 * Usage
 *
 * LinearRegression lr = new LinearRegression()
 *              .setLearningRate(0.1)
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
// TODO: ? static not allowed here -- what is a static class?
// TODO: ? How to implement/extend Predictor
//public class LinearRegression implements Predictor<DataStream<ArrayList<Double>>, DataStream<Double>>{
public class LinearRegression {
    private Double learningRate;
    private int dimensions;

    private long sampleCount = 0;

    public LinearRegression(Double learningRate, int dimensions){
        this.learningRate = learningRate;
        this.dimensions = dimensions;
    }

    public DataStream<ArrayList<Double>> fit(DataStream<Tuple2<ArrayList<Double>, Double>> trainingData) throws Exception {
        return trainingData.flatMap(new PartialModelBuilder(learningRate, dimensions));
    }

//    @Override
    public DataStream<Double> predict(DataStream<ArrayList<Double>> value) throws Exception {
        return null;
    }

    public static class PartialModelBuilder extends RichFlatMapFunction<Tuple2<ArrayList<Double>, Double>, ArrayList<Double>> {
        private Double learningRate;
        private int dimensions;

        public PartialModelBuilder(Double learningRate, int dimensions){
            this.learningRate = learningRate;
            this.dimensions = dimensions;
        }

        private transient ValueState<RegressionModel> modelState;

        /**
         * Error Message:
         * State key serializer has not been configured in the config.
         * This operation cannot use partitioned state.
         */
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(Tuple2<ArrayList<Double>, Double> sample, Collector<ArrayList<Double>> out) throws Exception {

            Double label = sample.f1;
            ArrayList<Double> features = sample.f0;
            Double prediction = .0;

            RegressionModel model = modelState.value();

            for(int i = 0; i < model.weights.size(); i++){
                prediction += model.weights.get(i) * features.get(i);
            }
            // TODO: ? error is not used
            Double error = squaredError(label, prediction);

            Double derivative = prediction - label;

            for (int i = 0; i < model.weights.size(); i++) {
                Double oldWeight = model.weights.get(i);
                // For each weight in our model use the example's error derivative to get the weight gradient
                // and update the gradient sum for that weight
                Double weightGradient = derivative * sample.f0.get(i);
                // Double currentLR = learningRate / Math.sqrt(applyCount);  // todo: ? dynamic learningRate
                Double change = learningRate * weightGradient;

                model.weights.set(i, oldWeight - change);
            }

            modelState.update(model);

            out.collect(model.weights);

        }

        // TODO: loss function check
        private Double squaredError(Double truth, Double prediction) {
            return 0.5 * (prediction - truth) * (prediction - truth);
        }

        @Override
        public void open(Configuration config) {
//            ArrayList<Double> initWeights = ); //
//        ArrayList<Double> initWeights = new ArrayList<>(); //Collections.nCopies(dimensions, 0.0)
//        Random random = new Random();
//        for(int i = 0; i < dimensions; i++){
//            initWeights.add(new Double(random.nextDouble()));  // uniformly distributed from 0.0 to 1.0
//        }

            // obtain key-value state for prediction model
            ValueStateDescriptor<RegressionModel> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "regressionModel",
                            // type information of state
                            TypeInformation.of(new TypeHint<RegressionModel>() {}),
                            // default value of state
                            new RegressionModel(dimensions));
            modelState = getRuntimeContext().getState(descriptor);
        }
    }

}

class RegressionModel  implements Serializable {
    public ArrayList<Double> weights;

    public RegressionModel(int dimensions) {
        this.weights = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
    }
}

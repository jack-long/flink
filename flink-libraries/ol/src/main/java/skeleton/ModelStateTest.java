package skeleton;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Simple example to show the state is consistent between multiple instances of an operator.
 * The model is only an increasing counter.
 * The result shows increasing values without a duplication,
 * so the counter is increased by each instance.
 */
public class ModelStateTest {

    // *************************************************************************
    // PROGRAM
    // Usage: the arguments needed are:
    // --training The path to the training file
    // --batchsize The number of items to include in one batch
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // To simplify we make the assumption that the last element in each line is the dependent variable
        DataStream<ArrayList<Double>> trainingData = env.readTextFile(params.get("training"))
                .map(new VectorExtractor());
        // Build the model
        DataStream<Tuple2<Integer, Integer>>model = trainingData
                .countWindowAll(Integer.parseInt(params.get("batchsize")))
                .apply(new PartialModelBuilder());

//        model.print();
        model.writeAsText("output/model_state_test", FileSystem.WriteMode.OVERWRITE);
        // execute program
        env.execute("State test");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************
    /**
     * Builds up-to-date partial models on new training data.
     */
    public static class PartialModelBuilder extends RichAllWindowFunction<ArrayList<Double>, Tuple2<Integer, Integer>, GlobalWindow> {

        public PartialModelBuilder() {}

        // This keeps track of the number of batches we have passed.
        private int applyCount = 0;

        private static final long serialVersionUID = 1L;

        // This is where we store our persistent state, which is the model.
        private transient ValueState<Integer> modelState;

        // Here we initialize our state by creating a model as a weight vector of zeros
        @Override
        public void open(Configuration config) {

            // obtain key-value state for prediction model
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "modelState",
                            // type information of state
                            TypeInformation.of(new TypeHint<Integer>() {}),
                            // default value of state
                            0);
            modelState = getRuntimeContext().getState(descriptor);
        }

        // The apply function calculates the updated model according to the new batch and updates the state
        @Override
        public void apply(GlobalWindow window, Iterable<ArrayList<Double>> values, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            this.applyCount++;

            Integer oldModel = modelState.value();
            Integer updatedModel = oldModel + 1;
            modelState.update(updatedModel);
            out.collect(new Tuple2<>(updatedModel, this.applyCount));
        }
    }

    // Takes a String CSV line of numbers and converts it to an ArrayList<Double>
    public static class VectorExtractor implements MapFunction<String, ArrayList<Double>> {
        @Override
        public ArrayList<Double> map(String s) throws Exception {
            String[] elements = s.split(",");
            ArrayList<Double> doubleElements = new ArrayList<>(elements.length);
            for (int i = 0; i < elements.length; i++) {
                doubleElements.add(new Double(elements[i]));
            }
            return doubleElements;
        }
    }

}

package skeleton;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package se.sics.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Simple example of a streaming mini-batch SGD implementation
 */
public class IncrementalLearning {


    // *************************************************************************
    // PROGRAM
    // Usage: the arguments needed are:
    // --training The path to the training file
    // --batchsize The number of items to include in one batch
    // --dimensions The dimensionality of our data (number of features)
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        Double learningRate = params.has("learningRate") ?  new Double(params.get("learningRate")) : 0.001;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // To simplify we make the assumption that the last element in each line is the dependent variable
        DataStream<ArrayList<Double>> trainingData = env.readTextFile(params.get("training"))
                .map(new VectorExtractor());
        // Build the model

        DataStream<ArrayList<Double>> model = trainingData
                .countWindowAll(Integer.parseInt(params.get("batchsize")))
                .apply(new PartialModelBuilder(learningRate, Integer.parseInt(params.get("dimensions"))));

        model.print();


        // execute program
        env.execute("Streaming SGD");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************


    /**
     * Builds up-to-date partial models on new training data.
     */
    public static class PartialModelBuilder extends RichAllWindowFunction<ArrayList<Double>, ArrayList<Double>, GlobalWindow> {

        public PartialModelBuilder(Double learningRate, int dimensions) {
            this.learningRate = learningRate;
            this.dimensions = dimensions;
        }

        private Double learningRate;
        private int dimensions;
        // This keeps track of the number of batches we have passed. Normally this would only update once per iteration
        // but this is a crude POC ;)
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

        // The loss function. Not really used currently, but useful to understand why the
        // error derivative is simply (prediction - truth)
        private Double squaredError(Double truth, Double prediction) {
            return 0.5 * (prediction - truth) * (prediction - truth);
        }

        // This is where the model update happens. Our input is one batch of training points (as an Iterator)
        // and the output is the updated model (weight vector)
        private ArrayList<Double> buildPartialModel(Iterable<ArrayList<Double>> trainingBatch) throws Exception{
            int batchSize = 0;
            // Get the old model
            ArrayList<Double> regressionModel = modelState.value();
            ArrayList<Double> gradientSum = new ArrayList<>(Collections.nCopies(dimensions, 0.0));

            for (ArrayList<Double> sample : trainingBatch) {
                // For each example in the batch, find it's error derivative
                batchSize++;
                Double truth = sample.get(sample.size() - 1);
                Double prediction = predict(regressionModel, sample);
                Double error = squaredError(truth, prediction);
                Double derivative = prediction - truth;
                for (int i = 0; i < regressionModel.size(); i++) {
                    // For each weight in our model use the example's error derivative to get the weight gradient
                    // and update the gradient sum for that weight
                    Double weightGradient = derivative * sample.get(i);
                    Double currentSum = gradientSum.get(i);
                    gradientSum.set(i, currentSum + weightGradient);
                }
            }
            for (int i = 0; i < regressionModel.size(); i++) {
                // Update each weight in the model by the sum of the gradients divided by the batchsize
                // and multiply by the learning rate
                Double oldWeight = regressionModel.get(i);
                Double currentLR = learningRate / Math.sqrt(applyCount);
                Double change = currentLR * (gradientSum.get(i) / batchSize);
                regressionModel.set(i, oldWeight - change);
            }
            return regressionModel;
        }

        private Double predict(ArrayList<Double> model, ArrayList<Double> sample){
//            System.out.println(model);
            Double prediction = .0;
            for(int i = 0; i < model.size(); i++){
                prediction += model.get(i) * sample.get(i);
            }
            return prediction;
        }

        // The apply function calculates the updated model according to the new batch and updates the state
        @Override
        public void apply(GlobalWindow window, Iterable<ArrayList<Double>> values, Collector<ArrayList<Double>> out) throws Exception {
            this.applyCount++;

            ArrayList<Double> updatedModel = buildPartialModel(values);
            modelState.update(updatedModel);
            out.collect(updatedModel);
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

package regression;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RegressionModel {
    public List<Double> weights;
    public Double cost;

    /**
     * Required no-arg constructor to be as POJO.
     */
    public RegressionModel() {}

    /**
     * Initialize weights with all zeros.
     *
     * @param dimensions
     */
    public RegressionModel(int dimensions) {
        weights = new ArrayList<>(Collections.nCopies(dimensions, 0.0));
        cost = new Double(-1); // TODO: what's the initial value ?
    }

    public RegressionModel(List<Double> weights, Double cost) {
        this.weights = weights;
        this.cost = cost;
    }

    @Override
    public String toString() {
        return "weights: " + weights + ", cost: " + cost;
    }
}

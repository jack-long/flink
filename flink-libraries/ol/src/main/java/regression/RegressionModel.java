package regression;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RegressionModel implements Serializable {
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
        weights = new ArrayList<>(Collections.nCopies(dimensions, 0.0)); //TODO: better initial values?
        cost = null;
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

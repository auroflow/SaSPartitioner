package cn.edu.zju.daily.metricflux.partitioner.monitor;

import org.junit.jupiter.api.Test;
import smile.data.DataFrame;
import smile.data.formula.Formula;
import smile.regression.DataFrameRegression;
import smile.regression.LASSO;

public class RandomForestTest {

    @Test
    void test() {
        double[][] raw = {
            {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}, {8, 9}, {9, 10}, {10, 11},
            {11, 12}, {12, 13}, {13, 14}, {14, 15}, {15, 16}, {16, 17}, {17, 18}, {18, 19},
            {19, 20}, {20, 21}
        };

        DataFrameRegression rf = LASSO.fit(Formula.lhs("y"), DataFrame.of(raw, "x", "y"));
        System.out.println(rf.online());

        double[][] test = {
            {1.5, 2.5},
            {2.5, 3.5},
            {3.5, 4.5},
            {4.5, 5.5},
            {5.5, 6.5},
            {6.5, 7.5},
            {7.5, 8.5},
            {8.5, 9.5},
            {9.5, 10.5},
            {10.5, 11.5},
            {11.5, 12.5},
            {12.5, 13.5},
            {13.5, 14.5},
            {14.5, 15.5},
            {15.5, 16.5},
            {16.5, 17.5},
            {17.5, 18.5},
            {18.5, 19.5},
            {19.5, 20.5},
            {20.5, 21.5}
        };
        double[] predictions = rf.predict(DataFrame.of(test, "x", "y"));
        for (double prediction : predictions) {
            System.out.println(prediction);
        }
    }
}

package cn.edu.zju.daily.metricflux.partitioner.learning;

import static java.lang.Math.abs;

import java.util.function.DoubleBinaryOperator;
import lombok.extern.slf4j.Slf4j;

/**
 * Calculate rewards given imbalance. Each episode should use a separated instance of this class.
 */
@Slf4j
class RewardCalculator implements DoubleBinaryOperator {

    public RewardCalculator(double initialPerf) {
        this.initialPerf = initialPerf;
        this.prevPerf = initialPerf;
        this.useFromPrev = false;
    }

    public RewardCalculator() {
        this.initialPerf = Double.NaN;
        this.prevPerf = Double.NaN;
        this.useFromPrev = true;
    }

    private double initialPerf;
    private double prevPerf;
    private final boolean useFromPrev;

    @Override
    public synchronized double applyAsDouble(double imbalance, double nmad) {
        double perf = (imbalance + nmad) / 2;

        if (Double.isNaN(initialPerf)) {
            LOG.info("Initial imbalance and nmad: {} {}", imbalance, nmad);
            initialPerf = perf;
            prevPerf = perf;
        }

        double fromInitial = (initialPerf - perf) / initialPerf;
        double fromPrev = (prevPerf - perf) / prevPerf;
        double reward;
        if (fromInitial > 0) {
            reward =
                    ((1 + fromInitial) * (1 + fromInitial) - 1)
                            * (useFromPrev ? abs(1 + fromPrev) : 1);
        } else {
            reward =
                    -((1 - fromInitial) * (1 - fromInitial) - 1)
                            * (useFromPrev ? abs(1 - fromPrev) : 1);
        }
        prevPerf = perf;
        return reward;
    }
}

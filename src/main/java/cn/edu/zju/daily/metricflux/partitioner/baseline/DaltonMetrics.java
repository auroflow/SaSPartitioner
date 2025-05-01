/*Copyright (c) 2022 Data Intensive Applications and Systems Laboratory (DIAS)
                   Ecole Polytechnique Federale de Lausanne
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/

package cn.edu.zju.daily.metricflux.partitioner.baseline;

import cn.edu.zju.daily.metricflux.core.data.Record;
import cn.edu.zju.daily.metricflux.partitioner.MetricPartitioner;
import cn.edu.zju.daily.metricflux.partitioner.container.ContextualBandits;
import cn.edu.zju.daily.metricflux.partitioner.learning.ExternalEnv.NextActionResponse;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Original dalton implementation */
@Slf4j
public class DaltonMetrics<R extends Record<Integer>> extends MetricPartitioner<R>
        implements CheckpointedFunction {

    ContextualBandits cbandit;

    private ListState<ContextualBandits> cbandit_chk;
    private volatile double[] nonIdleTimes;

    public DaltonMetrics(
            int metricCollectorPort,
            int numWorkers,
            int metricNumSlides,
            int numMetricsPerWorker,
            int episodeLength,
            int slide, // milliseconds
            int size, // milliseconds
            int numOfKeys,
            double initialPerf) {

        super(
                metricCollectorPort,
                numWorkers,
                metricNumSlides,
                numMetricsPerWorker,
                episodeLength,
                initialPerf,
                false);

        cbandit = new ContextualBandits(numWorkers, slide, size, numOfKeys);
        nonIdleTimes = new double[numWorkers];

        Arrays.fill(nonIdleTimes, 1000D);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
    }

    /**
     * Main function Receives a record
     *
     * @param r the newly arrived record
     * @param out Outputs <Worker, Record>
     */
    @Override
    protected void processElementInternal(
            R r,
            ProcessFunction<R, Tuple2<Integer, R>>.Context ctx,
            Collector<Tuple2<Integer, R>> out)
            throws Exception {
        boolean isHot;
        int worker;

        // checks if it is hot and does maintenance work for hot keys
        isHot = cbandit.isHot(r);

        // once every slide iterate workers, const. cost per tuple --> O(n)
        cbandit.expireState(r, isHot);

        // partition: iterate workers, const. cost per tuple --> O(n)
        worker = cbandit.partition(r, isHot);
        r.setHot(isHot);

        out.collect(new Tuple2<>(worker, r));

        cbandit.updateState(r, worker); // --> O(n)
        // cbandit.updateQtable(r, isHot, worker); // --> O(n)
        if (isHot) {
            double reward = reward(worker);
            cbandit.update(r.getKey(), worker, reward);
        }

        applyEffect(r.getKey(), worker, isHot);
    }

    private synchronized double reward(int worker) {
        double L = nonIdleTimes[worker];
        double avg = Arrays.stream(nonIdleTimes).average().orElse(1000D);
        return (avg - L) / Math.max(avg, L); // smaller load -> bigger reward
    }

    @Override
    protected synchronized void onNewSlide(NextActionResponse response) {
        super.onNewSlide(response);
        this.nonIdleTimes = response.getMetrics();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        cbandit_chk.clear();
        cbandit_chk.add(cbandit);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {
        cbandit_chk =
                functionInitializationContext
                        .getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>("cbanditChk", ContextualBandits.class));
        for (ContextualBandits q : cbandit_chk.get()) {
            cbandit = q;
        }
    }

    // used for debugging
    public ContextualBandits getCbandit() {
        return cbandit;
    }
}

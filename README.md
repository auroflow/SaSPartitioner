# SaSPartitioner

SaSPartitioner is a self-adaptive stream partitioning framework that leverages Deep Reinforcement Learning based on real
running metrics.

## System requirements

- Java 11
- Apache Flink 1.20
- Ray 2.40.0

We use a modified version of Flink 1.20 with the ability to collect metrics at custom intervals. You should compile and deploy
[this modified version](https://github.com/auroflow/flink-1.20) of Flink on every machine in your cluster.

The system contains two main components: the Flink partitioner written in Java, and the reinforcement learning agent
implemented with Ray RLlib.

- For Java code, point `flink.source.path` in `pom.xml` to our modified Flink, then compile with `mvn package`. 
- For the RL agent, install the required Python packages in `scripts/rl/requirements.txt`.

## Run

The parameters are configured in `src/main/resources/params.yaml` and `scripts/rl/configurations.py` respectively. An example 
of the yaml and Python configuration file is provided in `params/` and `scripts/rl/configuration_pool/`.

### Offline training

1. Set the `learningPartitioner` in `params.yaml` to `dalton-offline`.
2. Offline data collection: run the Java class `cn.edu.zju.daily.metricflux.task.wordcount.WordCountStaticDistRouteTrainingExperiment`
   to collect the offline data.
3. Configure the `log_folder` and `data_path` in `configurations.py`. Change `run_mode` to `offline`.
4. Run `scripts/rl/offline_online_train_remote_n.py` to obtain the pre-trained model.

### Online training

1. Set the `learningPartitioner` in `params.yaml` to `saspartitioner`.
2. Set the `checkpoint_path` in `configurations.py` to point to the offline model.
3. Run `scripts/rl/offline_online_train_remote_n.py` to start the RL agent server.
4. Run the Java class `cn.edu.zju.daily.metricflux.task.wordcount.WordCountStaticDistRouteTrainingExperiment` to start the
   online training process.

### Throughput test

To test the maximum throughput of the system:

1. Set the `checkpoint_path` in `configurations.py` to point to the online model.
2. Run `scripts/rl/offline_online_train_remote_n.py` to start the RL agent server.
3. Set the `partitioner` in `params.yaml` to `saspartitioner`.
4. Run the Java class `cn.edu.zju.daily.metricflux.task.wordcount.WordCountThroughputExperimentV2` to start testing. The source data
   rate will gradually increase until backpressure is detected, and the maximum throughput will be logged.

#### Baselines

You can run the following baselines to compare with SaSPartitioner by setting the `partitioner` to these values:

- Hash: `hash`
- cAM: `cam`
- DAGreedy: `dagreedy`
- Dalton: `dalton-original`
- Dalton with collected metrics as observations: `dalton-metrics`

There are some bash scripts in `bin/` to facilitate batch experiments. You can refer to these scripts for the TDigest task.

from utils import get_mask_indexes_heuristic


class RtEnvConfig:
    num_steps_lifetime = 5000  # 200000
    checkpoint_path = None

    class OfflineConfig:
        data_path = "/home/user/ray_results/CQL_2024-12-30_22-13-36/CQL_RemoteEnv_4445b_00000_0_2024-12-30_22-13-39/checkpoint_000000"
        offline_iters = 10

    offline = OfflineConfig()


class RemoteEnvConfig:
    socket_host = "0.0.0.0"
    socket_port = 49985
    initial_perf = 0.6
    # Features: for each worker, 1-dim features + workload
    feature_dim = 1 + 1
    checkpoint_path = None

    class RemoteOfflineConfig:
        log_folder = None
        data_path = "/home/user/data/offline_data/log/dalton-cold"
        include_workload_features = True
        num_steps_lifetime = 1e7
        actor_lr = 1e-5
        critic_lr = 1e-5  # will it work?
        alpha_lr = 1e-3
        tau = 1e-5
        target_entropy = -73
        initial_alpha = 1e-2
        cql_temperature = 1.0
        cql_min_q_weight = 0.05

    class RemoteOnlineConfig:
        # checkpoint_path = "/home/user/ray_results/SAC_2025-04-24_16-16-17/SAC_RemoteEnv_652b4_00000_0_2025-04-24_16-16-17/checkpoint_000001"
        checkpoint_path = None
        evaluate_only = False
        simulation = False
        num_iters = 500
        actor_lr = 1e-5
        critic_lr = 1e-3
        alpha_lr = 1e-3
        tau = 1e-5
        initial_alpha = 0.002

    offline = RemoteOfflineConfig()
    online = RemoteOnlineConfig()
    run_mode = "online"


class Config:
    # parameters
    gamma = 0.99
    num_features_per_worker = 3
    max_steps = 128
    train_batch_size = 128
    training_intensity = 128
    actor_lr = 1e-5
    critic_lr = 1e-3
    alpha_lr = 1e-3  # 1e
    tau = 5e-4
    initial_alpha = 1e-2

    model = "default"
    num_gpus = 0
    num_cpus = 2
    api = "new"
    num_steps_lifetime = 20000
    num_steps_sampled_before_learning_starts = 100

    # Important ones
    parallelism = 128
    zipf = 1.5
    ensemble_size = 2
    num_hot_keys = 10
    num_true_hot_keys = 10
    spread_keys = 0
    distribution_file = None
    # If distribution file is None, zipf is used to determine the distribution. Otherwise, distribution file is used.
    mask_indexes = get_mask_indexes_heuristic(
        distribution_file=distribution_file,
        alpha=zipf,
        parallelism=parallelism,
        spread_keys=spread_keys,
        num_hot_keys=num_hot_keys,
        num_true_hot_keys=num_true_hot_keys,
    )
    if num_hot_keys == -1:
        num_hot_keys = 10

    simulator_env = RtEnvConfig()
    remote_env = RemoteEnvConfig()

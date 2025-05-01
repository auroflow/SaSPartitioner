import pandas as pd
from ray.air import CheckpointConfig
from ray.rllib.algorithms import CQLConfig, CQL, SACConfig, SAC, BCConfig, BC
from ray.rllib.core.rl_module import RLModuleSpec
from ray.tune import Tuner
from ray.train import RunConfig

from callback import CustomMetricsCallback
from configurations import Config
from environment.remote_env import RemoteEnv

import os

from offline.offline_prelearner import UpdatedOfflinePreLearner


def offline_train_remote_cql():
    conf = Config()
    action_dim = sum([len(item) for item in conf.mask_indexes])

    print("Action dim:", action_dim)

    env_config = {
        "feature_dim": conf.remote_env.feature_dim,
        "action_dim": action_dim,
        "socket_host": conf.remote_env.socket_host,
        "socket_port": conf.remote_env.socket_port,
        "parallelism": conf.parallelism,
        "offline": True,
    }
    env_class = RemoteEnv

    # ========================
    # Offline training -- CQL
    # ========================
    offline_config = (
        CQLConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(env_class, env_config=env_config)
        .framework(framework="torch")
        .resources(num_gpus=conf.num_gpus)
        .learners(num_learners=0, num_gpus_per_learner=conf.num_gpus)
        .env_runners(
            num_env_runners=1,
            num_envs_per_env_runner=1,
            num_cpus_per_env_runner=conf.num_cpus - 1,
            num_gpus_per_env_runner=0,
            sample_timeout_s=300,
            # batch_mode="complete_episodes",
        )
        .training(
            lr=None,
            gamma=conf.gamma,
            train_batch_size_per_learner=conf.train_batch_size,
            training_intensity=conf.training_intensity,
            actor_lr=conf.actor_lr,
            critic_lr=conf.critic_lr,
            alpha_lr=conf.alpha_lr,
            initial_alpha=conf.initial_alpha,
            store_buffer_in_checkpoints=True,
            tau=conf.tau,
            replay_buffer_config=dict(
                _enable_replay_buffer_api=True,
                type="PrioritizedEpisodeReplayBuffer",
                capacity=100000,
            ),
            num_steps_sampled_before_learning_starts=conf.num_steps_sampled_before_learning_starts,
        )
        .reporting(metrics_num_episodes_for_smoothing=1)
        .callbacks(CustomMetricsCallback)
        .offline_data(
            input_=conf.remote_env.offline.data_path,
            input_compress_columns=[],
            offline_sampling=True,
            dataset_num_iters_per_learner=1,
            prelearner_class=UpdatedOfflinePreLearner,
            input_read_episodes=False,
            output=None,
            output_write_episodes=False,
        )
        .evaluation(evaluation_interval=0)
    )

    offline_tuner = Tuner(
        CQL,
        param_space=offline_config,
        run_config=RunConfig(
            checkpoint_config=CheckpointConfig(
                checkpoint_frequency=10,
                checkpoint_at_end=True,
            ),
            stop={
                "learners/__all_modules__/num_env_steps_trained_lifetime": conf.remote_env.offline.num_steps_lifetime
            },
        ),
    )

    results = offline_tuner.fit()
    best_result = results.get_best_result()
    print(best_result)


def online_training_remote():
    conf = Config()
    action_dim = sum([len(item) for item in conf.mask_indexes])

    print("Action dim:", action_dim)

    env_config = {
        "feature_dim": conf.remote_env.feature_dim,
        "action_dim": action_dim,
        "socket_host": conf.remote_env.socket_host,
        "socket_port": conf.remote_env.socket_port,
        "parallelism": conf.parallelism,
    }
    env_class = RemoteEnv

    # ================
    # Online training
    # ================
    checkpoint_path = (
        os.path.join(
            conf.remote_env.online.checkpoint_path,
            "learner_group",
            "learner",
            "rl_module",
            "default_policy",
        )
        if conf.remote_env.online.checkpoint_path
        else None
    )
    print("Checkpoint path:", checkpoint_path)

    online_config = (
        SACConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(env_class, env_config=env_config)
        .framework(framework="torch")
        .resources(num_gpus=conf.num_gpus)
        .learners(num_learners=0, num_gpus_per_learner=0)
        .env_runners(
            num_env_runners=0,
            num_envs_per_env_runner=1,
            num_cpus_per_env_runner=conf.num_cpus - 1,
            num_gpus_per_env_runner=0,
            sample_timeout_s=300,
            # batch_mode="complete_episodes",
        )
        .training(
            lr=None,
            gamma=conf.gamma,
            train_batch_size_per_learner=conf.train_batch_size,
            training_intensity=conf.training_intensity,
            actor_lr=conf.remote_env.online.actor_lr,
            critic_lr=conf.remote_env.online.critic_lr,
            alpha_lr=conf.remote_env.online.alpha_lr,
            initial_alpha=conf.remote_env.online.initial_alpha,
            store_buffer_in_checkpoints=True,
            tau=conf.remote_env.online.tau,
            replay_buffer_config=dict(
                _enable_replay_buffer_api=True,
                type="PrioritizedEpisodeReplayBuffer",
                capacity=100000,
            ),
            num_steps_sampled_before_learning_starts=(
                0 if checkpoint_path else conf.num_steps_sampled_before_learning_starts
            ),
        )
        .evaluation(evaluation_interval=0)
        .reporting(metrics_num_episodes_for_smoothing=1)
        .callbacks(CustomMetricsCallback)
        .rl_module(
            rl_module_spec=RLModuleSpec(
                load_state_path=checkpoint_path,
            ),
        )
    )

    online_tuner = Tuner(
        SAC,
        param_space=online_config,
        run_config=RunConfig(
            checkpoint_config=CheckpointConfig(
                checkpoint_frequency=10,
                checkpoint_at_end=True,
            ),
            stop={
                "env_runners/negative_imbalance": -0.01,
                "num_env_steps_sampled_lifetime": conf.remote_env.online.num_steps_lifetime,
            },
        ),
    )

    results = online_tuner.fit()
    best_result = results.get_best_result()
    print("Best result:", best_result)
    for i, result in enumerate(results):
        if result.error:
            print(f"Trial #{i} had an error:", result.error)
            continue
        print(
            f"Trial #{i} finished successfully with an imbalance of:",
            result.metrics["env_runners"]["imbalance"],
        )

    results_df = results.get_dataframe()
    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.precision",
        4,
    ):
        print(
            results_df[
                [
                    "config/actor_lr",
                    "config/critic_lr",
                    "config/alpha_lr",
                    "config/tau",
                    "env_runners/imbalance",
                ]
            ]
        )


if __name__ == "__main__":
    os.environ["CUDA_VISIBLE_DEVICES"] = "1"
    offline_train_remote_cql()
    # online_training_remote()

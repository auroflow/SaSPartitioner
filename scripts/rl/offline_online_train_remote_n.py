import pandas as pd
import ray
import torch.cuda
from ray import tune, train
from ray.air import CheckpointConfig
from ray.rllib.algorithms import CQLConfig, CQL, SACConfig, SAC, BCConfig, BC
from ray.rllib.core.rl_module import RLModuleSpec
from ray.tune import Tuner
from ray.train import RunConfig

from callback import CustomMetricsCallback
from configurations import Config
from environment.remote_env import RemoteEnv

import os

from environment.remote_sim_env import RemoteSimEnv
from models.sac_n.catalog import SACNCatalog
from models.sac_n.cql_learner import CQLNTorchLearner
from models.sac_n.cql_module import CQLNTorchRLModule
from models.sac_n.sac_learner import SACNTorchLearner
from models.sac_n.sac_module import SACNTorchRLModule
from offline.offline_prelearner import UpdatedOfflinePreLearner
from utils import get_masks_from_mask_indexes

conf = Config()


def offline_train_remote_cql_n():
    action_dim = sum([len(item) for item in conf.mask_indexes])
    # dropped_dim = sum([1 for item in conf.mask_indexes if len(item) == 1])
    dropped_dim = 0

    print("Action dim:", action_dim)
    print("Dropped dim:", dropped_dim)

    env_config = {
        "feature_dim": conf.remote_env.feature_dim,
        "action_dim": action_dim,
        "dropped_dim": dropped_dim,
        "zipf": conf.zipf,
        "socket_host": conf.remote_env.socket_host,
        "socket_port": conf.remote_env.socket_port,
        "offline": False,
        "parallelism": conf.parallelism,
        "num_hot_keys": conf.num_hot_keys,
        "max_steps": conf.max_steps,
        "masks": get_masks_from_mask_indexes(conf.mask_indexes),
    }
    env_class = RemoteSimEnv

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
        .learners(
            num_learners=0,
            num_gpus_per_learner=min(0.5, conf.num_gpus),
            num_cpus_per_learner=conf.num_cpus - 1,
        )
        .env_runners(
            num_env_runners=1,
            num_envs_per_env_runner=1,
            num_cpus_per_env_runner=1,
            num_gpus_per_env_runner=0,
            sample_timeout_s=300,
            # batch_mode="complete_episodes",
        )
        .training(
            lr=None,
            gamma=conf.gamma,
            train_batch_size_per_learner=conf.train_batch_size,
            training_intensity=conf.training_intensity,
            actor_lr=conf.remote_env.offline.actor_lr,
            critic_lr=conf.remote_env.offline.critic_lr,
            alpha_lr=conf.remote_env.offline.alpha_lr,
            initial_alpha=conf.remote_env.offline.initial_alpha,
            store_buffer_in_checkpoints=True,
            tau=conf.remote_env.offline.tau,
            replay_buffer_config=dict(
                _enable_replay_buffer_api=True,
                type="PrioritizedEpisodeReplayBuffer",
                capacity=100000,
            ),
            twin_q=False,
            learner_class=CQLNTorchLearner,
            num_steps_sampled_before_learning_starts=conf.num_steps_sampled_before_learning_starts,
            bc_iters=conf.remote_env.offline.cql_bc_iters,
            temperature=conf.remote_env.offline.cql_temperature,
            min_q_weight=conf.remote_env.offline.cql_min_q_weight,
        )
        .evaluation(evaluation_interval=1)
        .reporting(metrics_num_episodes_for_smoothing=1)
        .callbacks(CustomMetricsCallback)
        .rl_module(
            rl_module_spec=RLModuleSpec(
                module_class=CQLNTorchRLModule,
                catalog_class=SACNCatalog,
                model_config=dict(
                    ensemble_size=conf.ensemble_size,
                ),
            )
        )
        .offline_data(
            input_=conf.remote_env.offline.data_path,
            input_compress_columns=[],
            offline_sampling=True,
            dataset_num_iters_per_learner=1,
            # prelearner_class=UpdatedOfflinePreLearner,
            map_batches_kwargs={
                "num_gpus": min(0.5, conf.num_gpus),
                "concurrency": 1,
            },
            input_read_episodes=False,
            output=None,
            output_write_episodes=False,
        )
    )

    offline_tuner = Tuner(
        CQL,
        param_space=offline_config,
        run_config=RunConfig(
            checkpoint_config=CheckpointConfig(
                checkpoint_frequency=25,
                checkpoint_at_end=True,
                num_to_keep=2,
            ),
            stop={
                "training_iteration": conf.remote_env.offline.num_iters,
            },
        ),
    )

    results = offline_tuner.fit()
    best_result = results.get_best_result()
    print(best_result)
    with open("/home/user/code/saspartitioner/scripts/rl/checkpoint_loc.txt", "w") as f:
        f.write(best_result.path)


def online_training_remote_sac_n():
    action_dim = sum([len(item) for item in conf.mask_indexes])
    # These "hot" keys are assigned only one partition. No need to partition them.
    # dropped_dim = sum([1 for item in conf.mask_indexes if len(item) == 1])
    dropped_dim = 0

    print("Action dim:", action_dim)
    print("Dropped dim:", dropped_dim)

    env_config = {
        "feature_dim": conf.remote_env.feature_dim,
        "action_dim": action_dim,
        "dropped_dim": dropped_dim,
        "zipf": conf.zipf,
        "socket_host": conf.remote_env.socket_host,
        "socket_port": conf.remote_env.socket_port,
        "offline": False,
        "parallelism": conf.parallelism,
        "num_hot_keys": conf.num_hot_keys,
        "max_steps": conf.max_steps,
        "masks": get_masks_from_mask_indexes(conf.mask_indexes),
    }
    env_class = (
        RemoteSimEnv
        if (
            hasattr(conf.remote_env.online, "simulation")
            and conf.remote_env.online.simulation
        )
        else RemoteEnv
    )

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
        .learners(num_learners=0, num_gpus_per_learner=conf.num_gpus)
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
            twin_q=False,
            learner_class=SACNTorchLearner,
            # num_steps_sampled_before_learning_starts=conf.num_steps_sampled_before_learning_starts,
            num_steps_sampled_before_learning_starts=(
                0 if checkpoint_path else conf.num_steps_sampled_before_learning_starts
            ),
        )
        .evaluation(evaluation_interval=0)
        .reporting(metrics_num_episodes_for_smoothing=1)
        .callbacks(CustomMetricsCallback)
        .rl_module(
            rl_module_spec=RLModuleSpec(
                module_class=SACNTorchRLModule,
                catalog_class=SACNCatalog,
                model_config=dict(
                    ensemble_size=conf.ensemble_size,
                ),
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
                num_to_keep=1,
            ),
            stop={
                "env_runners/negative_imbalance": -0.01,
                "training_iteration": conf.remote_env.online.num_iters,
            },
        ),
    )

    if not conf.remote_env.online.evaluate_only:
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
        with open(
            "/home/user/code/saspartitioner/scripts/rl/checkpoint_loc.txt", "w"
        ) as f:
            f.write(best_result.path)

    else:

        def trainable(c):
            algo = online_config.build()
            for i in range(conf.remote_env.online.num_iters):
                results = algo.evaluate()
                train.report(result + online_config.to_dict())
            return results

        tuner = tune.Tuner(trainable, param_space={})
        result_grid = tuner.fit()
        for i, result in enumerate(result_grid):
            if result.error:
                print(f"Trial #{i} had an error:", result.error)
                continue
            print(
                f"Trial #{i} finished successfully with an imbalance of:",
                result.metrics["env_runners"]["imbalance"],
            )

        results_df = result_grid.get_dataframe()
        with pd.option_context(
            "display.max_rows",
            None,
            "display.max_columns",
            None,
            "display.precision",
            4,
        ):
            print(results_df)


if __name__ == "__main__":
    # ray.init(dashboard_host="0.0.0.0")
    if not torch.cuda.is_available():
        print("Cuda is not available.")
    os.environ["CUDA_VISIBLE_DEVICES"] = "1"

    ray.init(num_gpus=1)

    if conf.remote_env.run_mode == "offline":
        offline_train_remote_cql_n()
    elif conf.remote_env.run_mode == "online":
        online_training_remote_sac_n()
    else:
        raise ValueError("Invalid run mode.")

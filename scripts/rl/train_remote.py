import os
from typing import Type, Tuple

import gymnasium as gym
import pandas as pd
import ray
from ray.rllib.algorithms import AlgorithmConfig, SACConfig, SAC
from ray.rllib.core.rl_module import RLModuleSpec
from ray.rllib.utils.from_config import NotProvided
from ray.rllib.utils.typing import LearningRateOrSchedule
from ray.train import RunConfig, CheckpointConfig
from ray.tune import Trainable, Tuner, with_resources, CLIReporter
from ray.tune.experiment import Trial

from callback import CustomMetricsCallback
from configurations import Config
from environment.remote_env import RemoteEnv
from models.masked_module import MaskedTorchRLModule


# parameters
# gamma = 0.99
# feature_dim = 64
#
# train_batch_size = 64
# training_intensity = 128
# # actor_lr = grid_search([2e-5, 1.5e-5, 1e-5, 5e-6])
# actor_lr = 1.5e-5
# # critic_lr = grid_search([2e-3, 1.5e-3, 1e-3, 5e-4])
# critic_lr = 1e-3
# # tau = grid_search([5e-6, 1e-5, 1.5e-5, 2e-5])
# tau = 1e-5
# model = "default"
# num_gpus = 0
# num_cpus = 1
# num_steps_lifetime = 1728000
#
# # Remote training
# remote = False
# socket_host = "0.0.0.0"
# socket_port = 49985  # RLLIB


class TrialTerminationReporter(CLIReporter):
    def __init__(self):
        super(TrialTerminationReporter, self).__init__()
        self.num_terminated = 0

    def should_report(self, trials, done=False):
        """Reports only on trial termination events."""
        old_num_terminated = self.num_terminated
        self.num_terminated = len([t for t in trials if t.status == Trial.TERMINATED])
        return self.num_terminated > old_num_terminated


def get_sac_config_remote(
    env_class: Type[gym.Env], conf: Config
) -> Tuple[AlgorithmConfig, Type[Trainable]]:
    action_dim = sum([len(item) for item in conf.mask_indexes])
    env_config = {
        "feature_dim": conf.remote_env.feature_dim,
        "parallelism": conf.parallelism,
        "action_dim": action_dim,
        "socket_host": conf.remote_env.socket_host,
        "socket_port": conf.remote_env.socket_port,
    }

    if not conf.remote_env.checkpoint_path:
        load_state_path = None
    else:
        load_state_path = os.path.join(
            conf.remote_env.checkpoint_path, "learner_group", "learner", "rl_module"
        )

    if conf.model == "custom":
        rl_module_spec = RLModuleSpec(
            module_class=MaskedTorchRLModule,
            model_config=dict(),
            load_state_path=load_state_path,
        )
    elif conf.model == "default":
        rl_module_spec = RLModuleSpec(
            load_state_path=load_state_path,
        )
    else:
        raise ValueError(f"Unknown model {conf.model}")

    return (
        SACConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(env_class, env_config=env_config)
        .framework(framework="torch")
        .resources(num_gpus=conf.num_gpus)
        .learners(num_learners=1, num_gpus_per_learner=0)
        .env_runners(
            num_env_runners=1 if conf.num_cpus > 1 else 0,
            num_envs_per_env_runner=max(1, conf.num_cpus - 1),
            num_cpus_per_env_runner=conf.num_cpus - 1,
            num_gpus_per_env_runner=0,
            sample_timeout_s=300,
        )  # , batch_mode="complete_episodes")
        .training(
            lr=None,
            gamma=conf.gamma,
            train_batch_size_per_learner=conf.train_batch_size,
            training_intensity=conf.training_intensity,
            actor_lr=conf.actor_lr,
            critic_lr=conf.critic_lr,
            alpha_lr=conf.critic_lr,
            tau=conf.tau,
            initial_alpha=conf.initial_alpha,
            store_buffer_in_checkpoints=True,
            replay_buffer_config=dict(
                _enable_replay_buffer_api=True,
                type="PrioritizedEpisodeReplayBuffer",
                capacity=100000,
            ),
            num_steps_sampled_before_learning_starts=conf.num_steps_sampled_before_learning_starts,
        )
        .evaluation(evaluation_interval=0)
        .reporting(metrics_num_episodes_for_smoothing=1)
        .callbacks(CustomMetricsCallback)
        .rl_module(rl_module_spec=rl_module_spec),
        SAC,
    )


def train_with_tune(
    config: AlgorithmConfig, trainable: Type[Trainable], num_steps: int
):
    # trainer = tune.with_resources(PPO, {"cpu": 48, "gpu": 1})
    trainable_with_resources = with_resources(trainable, {"cpu": 14})
    tuner = Tuner(
        trainable,
        param_space=config,
        run_config=RunConfig(
            stop={
                "env_runners/negative_imbalance": -0.01,
                "num_env_steps_sampled_lifetime": num_steps,
            },
            checkpoint_config=CheckpointConfig(
                checkpoint_frequency=10,
                checkpoint_at_end=True,
            ),
            progress_reporter=TrialTerminationReporter(),
        ),
    )
    return tuner.fit()


def resume_training_with_tune(config: AlgorithmConfig, trainable: Type[Trainable]):
    # trainer = tune.with_resources(PPO, {"cpu": 48, "gpu": 1})
    tuner = Tuner.restore(
        "/home/user/ray_results/SAC_2024-12-05_09-53-11",
        trainable,
        resume_unfinished=True,
        restart_errored=True,
        param_space=config,
    )
    return tuner.fit()


def test_train(config: AlgorithmConfig):
    algo = config.build().train()


if __name__ == "__main__":
    os.environ["RAY_AIR_NEW_OUTPUT"] = "0"
    os.environ["CUDA_VISIBLE_DEVICES"] = "1"
    ray.init(dashboard_host="0.0.0.0")

    conf = Config()
    config, trainable = get_sac_config_remote(RemoteEnv, conf)

    # print("total batch size:", config.total_train_batch_size)
    result_grid = train_with_tune(
        config, trainable, conf.remote_env.online.num_steps_lifetime
    )
    # result_grid = resume_training_with_tune(config, trainable)

    for i, result in enumerate(result_grid):
        if result.error:
            print(f"Trial #{i} had an error:", result.error)
            continue
        if (
            "env_runners" in result.metrics
            and "imbalance" in result.metrics["env_runners"]
        ):
            print(
                f"Trial #{i} finished successfully with an imbalance of:",
                result.metrics["env_runners"]["imbalance"],
            )
        else:
            print(f"Trial #{i} finished successfully, but imbalance is not found")

    results_df = result_grid.get_dataframe()
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
                    "config/tau",
                    "env_runners/imbalance",
                ]
            ]
            if "env_runners/imbalance" in results_df.columns
            else results_df[
                [
                    "config/actor_lr",
                    "config/critic_lr",
                    "config/tau",
                ]
            ]
        )

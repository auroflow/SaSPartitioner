import os
from typing import Type, Tuple

import gymnasium as gym
from ray.rllib.algorithms import AlgorithmConfig, SACConfig, SAC, Algorithm
from ray.rllib.core.rl_module import RLModuleSpec
from ray.rllib.utils.from_config import NotProvided
from ray.tune import Trainable

from callback import CustomMetricsCallback
from configurations import Config
from models.masked_module import MaskedTorchRLModule
from utils import get_masks_from_mask_indexes


def get_sac_config(
    env_class: Type[gym.Env],
    conf: Config,
) -> Tuple[SACConfig, Type[SAC]]:
    masks = get_masks_from_mask_indexes(conf.mask_indexes)

    print("masks:", masks)

    env_config = {
        "parallelism": conf.parallelism,
        "max_steps": conf.max_steps,
        "num_hot_keys": conf.num_hot_keys,
        "zipf": conf.zipf,
        "masks": masks,
        "num_features_per_subtask": conf.num_features_per_worker,
    }

    if conf.model == "custom":
        rl_module_spec = RLModuleSpec(
            module_class=MaskedTorchRLModule,
            model_config=dict(),
        )
    elif conf.model == "default":
        rl_module_spec = RLModuleSpec()
    else:
        raise ValueError(f"Unknown model {conf.model}")

    return (
        SACConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(
            env=env_class,
            env_config=env_config,
            # normalize_actions=True,
        )
        .framework(framework="torch")
        .resources(num_gpus=conf.num_gpus)
        .learners(num_learners=0, num_gpus_per_learner=0)
        .env_runners(
            num_env_runners=1 if conf.num_cpus > 1 else 0,
            num_envs_per_env_runner=max(1, conf.num_cpus - 1),
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
        .evaluation(evaluation_interval=0)
        .reporting(metrics_num_episodes_for_smoothing=1)
        .callbacks(CustomMetricsCallback)
        .rl_module(rl_module_spec=rl_module_spec),
        SAC,
    )

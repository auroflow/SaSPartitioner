import logging
from typing import Union, Optional, Dict, Callable

import gymnasium as gym
from ray.rllib import BaseEnv, Policy
from ray.rllib.algorithms import Algorithm, SAC
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core.rl_module import RLModule
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.evaluation.episode_v2 import EpisodeV2
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.typing import EpisodeType, PolicyID

logger = logging.getLogger(__name__)
checkpoint_path = "/home/user/ray_results/SAC_2024-12-13_19-23-58/SAC_RtEnv_bf33e_00000_0_2024-12-13_19-23-58/checkpoint_000001"


def get_episode_info(episode: Union[EpisodeType, EpisodeV2]) -> dict[str, float]:
    try:
        info = episode.get_infos(-1)
    except:
        try:
            info = episode.last_info_for()
        except:
            raise ValueError(f"Unsupported episode type: {type(episode)}")
    return info


def log(
    name: str,
    metrics_logger: MetricsLogger,
    info: dict[str, float],
    fn: Callable[[dict[str, float]], float],
) -> None:
    try:
        value = fn(info)
        metrics_logger.log_value(name, value)
    except:
        pass


class CustomMetricsCallback(DefaultCallbacks):

    def on_algorithm_init(
        self,
        *,
        algorithm: "Algorithm",
        metrics_logger: Optional[MetricsLogger] = None,
        **kwargs,
    ) -> None:
        pass

    def on_episode_end(
        self,
        *,
        episode: Union[EpisodeType, EpisodeV2],
        env_runner: Optional["EnvRunner"] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        rl_module: Optional[RLModule] = None,
        # TODO (sven): Deprecate these args.
        worker: Optional["EnvRunner"] = None,
        base_env: Optional[BaseEnv] = None,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        **kwargs,
    ) -> None:
        info = get_episode_info(episode)
        log("imbalance", metrics_logger, info, lambda info: info["imbalance"])
        log("negative_imbalance", metrics_logger, info, lambda info: -info["imbalance"])
        log("cv", metrics_logger, info, lambda info: info["cv"])
        log("nmad", metrics_logger, info, lambda info: info["nmad"])

    def on_episode_step(
        self,
        *,
        episode: Union[EpisodeType, EpisodeV2],
        env_runner: Optional["EnvRunner"] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        rl_module: Optional[RLModule] = None,
        # TODO (sven): Deprecate these args.
        worker: Optional["EnvRunner"] = None,
        base_env: Optional[BaseEnv] = None,
        policies: Optional[Dict[PolicyID, Policy]] = None,
        **kwargs,
    ) -> None:
        info = get_episode_info(episode)
        log("imbalance", metrics_logger, info, lambda info: info["imbalance"])
        log("negative_imbalance", metrics_logger, info, lambda info: -info["imbalance"])
        log("cv", metrics_logger, info, lambda info: info["cv"])

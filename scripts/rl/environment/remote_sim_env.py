import logging
import socket
from typing import Any, SupportsFloat

import gymnasium as gym
import numpy as np
from gymnasium.core import ObsType, ActType

import pickle
import array

from numpy.random import Generator

from utils import (
    generate_zipf,
    get_imbalance,
    normalize,
    get_perf,
    get_masks_from_mask_indexes,
    read_distribution,
    get_num_true_hot_keys_from_masks,
)
from configurations import Config

logger = logging.getLogger(__name__)

EPS = 1e-6
SEED = 88798


def get_workload(routing_table: np.ndarray, key_dist: np.ndarray):
    total = np.sum(routing_table, axis=0, keepdims=True)
    total[total == 0] = 1
    routing_table = routing_table / total
    return np.dot(routing_table, key_dist).astype(np.float32)


class SimulatedSubtask:
    def __init__(self, num_features: int, rng: Generator):
        self.rng = rng
        self.features = rng.random(num_features, np.float32) * 0.2 + 0.9
        self.weights = np.array(generate_zipf(2.5, num_features))
        self.speed = np.dot(self.features, self.weights)

    def next_window(self):
        # delta = self.rng.random(3, np.float32) * 0.01 - 0.005
        # # delta = np.zeros_like(self.features)
        # self.features += delta
        # self.features = np.clip(self.features, 0, 2)
        # self.speed = np.dot(self.features, self.weights)
        pass

    def get_features(self):
        return self.features

    def get_speed(self):
        return self.speed


class RemoteSimEnv(gym.Env):
    def __init__(self, config):
        super().__init__()
        self.offline = "offline" in config and config["offline"]
        self.parallelism = config["parallelism"]
        self.num_hot_keys = config["num_hot_keys"]
        self.feature_dim = config["feature_dim"] * config["parallelism"]
        self.action_dim = config["action_dim"]
        self.max_steps = config["max_steps"]
        # [normalized worker counts + metrics for worker 1 + ... + metrics for worker n]
        self.observation_space = gym.spaces.Box(
            low=np.float32(0),
            high=np.float32(2),
            shape=(self.feature_dim,),
            dtype=np.float32,
        )
        # [routing decisions]
        self.action_space = gym.spaces.Box(
            low=np.float32(0),
            high=np.float32(1),
            shape=(self.action_dim,),
            dtype=np.float32,
        )
        self.total_steps = 0

        # For simulating the environment
        self.masks = np.array(config["masks"])
        self.num_true_hot_keys = get_num_true_hot_keys_from_masks(config["masks"])
        if "distribution_file" in config and config["distribution_file"]:
            self.key_dist = np.array(read_distribution(config["distribution_file"]))[
                : config["num_hot_keys"]
            ]
        else:
            self.key_dist = generate_zipf(config["zipf"], config["num_hot_keys"])
        self.subtasks = []
        self.rng = np.random.default_rng(SEED)
        self._generate_random_subtasks()
        self.episode = 0
        print("Speeds:", [subtask.get_speed() for subtask in self.subtasks])

        self.current_step = 0
        self._obs = None
        self._info = None
        self._routing_table = None
        initial_action = np.ones((self.masks.shape[0],), np.float32)
        initial_routing_table = self._get_routing_table(initial_action)
        initial_times = self._get_times(initial_routing_table)
        self._get_obs(initial_routing_table)
        self.initial_imbalance = 0.8
        self.initial_perf = 0.6
        print(
            "Initial imbalance:",
            self.initial_imbalance,
            "Initial CV:",
            self.initial_perf,
        )

    def _generate_random_subtasks(self):
        self.subtasks.clear()
        self.rng = np.random.default_rng(
            SEED
        )  # So that the subtasks are the same for each episode
        for _ in range(self.parallelism):
            seed = self.rng.integers(0, 65536)
            self.subtasks.append(SimulatedSubtask(3, np.random.default_rng(seed)))

    def _get_times(self, routing_table: np.ndarray) -> np.ndarray:
        """
        Get the time taken by each subtask to process the workload
        :param routing_table: the routing table of size (parallelism, num_hot_keys)
        :return: the time taken by each subtask
        """
        speeds = np.array(
            [subtask.get_speed() for subtask in self.subtasks], np.float32
        )
        workload = get_workload(routing_table, self.key_dist)
        times = workload / (speeds + EPS)
        return times

    def _get_time_imbalance(self, routing_table: np.ndarray) -> float:
        times = self._get_times(routing_table)
        return get_imbalance(times)

    def _get_obs(self, routing_table: np.ndarray) -> ObsType:
        times = self._get_times(routing_table)
        workload = get_workload(routing_table, self.key_dist)
        assert len(times.shape) == 1
        assert len(workload.shape) == 1
        busy_ratio = times * 0.42 * len(times)
        # if busy ratio is greater than 1, then assign the value 2
        busy_ratio[busy_ratio >= 1] = 2
        normalize(workload)
        obs = np.concatenate((workload, busy_ratio), axis=0)
        self._obs = obs
        return obs

    def _get_info(self, imbalance, cv) -> dict[str, Any]:
        info = {"imbalance": imbalance, "cv": cv}
        self._info = info
        return info

    def _get_perf_reward_2(self, routing_table, perf) -> SupportsFloat:
        from_initial = (self.initial_perf - perf) / self.initial_perf
        if from_initial > 0:
            r = (1 + from_initial) ** 2 - 1
        else:
            r = -((1 - from_initial) ** 2 - 1)
        return r

    def reset(
        self,
        *,
        seed: int | None = None,
        options: dict[str, Any] | None = None,
    ) -> tuple[ObsType, dict[str, Any]]:
        super().reset(seed=seed, options=options)
        self._generate_random_subtasks()
        self.current_step = 0
        # initial_times = self._get_times(self._routing_table)
        # self.initial_imbalance = get_imbalance(initial_times)
        # self.initial_cv = get_cv(initial_times)
        self.episode += 1
        return self._get_obs(self._routing_table), self._get_info(
            self.initial_imbalance, self.initial_perf
        )

    def _get_routing_table(self, action):
        if action.ndim == 1:
            routing_table = np.zeros((self.parallelism, self.num_hot_keys), np.float32)
            routing_table[self.masks[:, 0], self.masks[:, 1]] = action
            routing_table[:, self.num_true_hot_keys :] = 1 / self.parallelism
        else:
            routing_table = np.zeros(
                (action.shape[0], self.parallelism, self.num_hot_keys), np.float32
            )
            routing_table[:, self.masks[:, 0], self.masks[:, 1]] = action
            routing_table[:, :, self.num_true_hot_keys :] = 1 / self.parallelism
        self._routing_table = routing_table
        return routing_table

    def step(
        self, action: ActType
    ) -> tuple[ObsType, SupportsFloat, bool, bool, dict[str, Any]]:
        # print("Step: batch size =", action.shape[0] if action.ndim == 2 else 0)
        routing_table = self._get_routing_table(action)
        times = self._get_times(routing_table)
        imbalance = get_imbalance(times)
        perf = get_perf(times)
        # reward = self._get_imbalance_reward(routing_table, imbalance)
        reward = self._get_perf_reward_2(routing_table, perf)
        # if np.random.rand() < 0.001:
        #     print("Action:", routing_table, "Imbalance:", imbalance, "perf:", perf, "Reward:", reward)
        info = self._get_info(imbalance, perf)
        obs = self._get_obs(routing_table)
        done = self.current_step >= self.max_steps
        # done = False
        for subtask in self.subtasks:
            subtask.next_window()
        self.current_step += 1
        # if self.episode == 1:
        #     print("Imbalance:", imbalance, "CV:", cv, "Reward:", reward)
        return obs, reward, done, False, info


if __name__ == "__main__":
    conf = Config()
    env = RemoteSimEnv(
        {
            "feature_dim": conf.remote_env.feature_dim,
            "action_dim": sum([len(item) for item in conf.mask_indexes]),
            "parallelism": conf.parallelism,
            "num_hot_keys": conf.num_hot_keys,
            "zipf": conf.zipf,
            "max_steps": 128,
            "masks": get_masks_from_mask_indexes(conf.mask_indexes),
        }
    )

    obs = env.reset()
    print("Initial observation:", obs)
    done = False
    total_reward = 0
    while not done:
        action = np.random.rand(sum([len(item) for item in conf.mask_indexes])).astype(
            np.float32
        )
        obs, reward, done, _, _ = env.step(action)
        total_reward += reward
    print("Total reward:", total_reward)
    env.close()

from typing import Any, SupportsFloat

import gymnasium as gym
import numpy as np
from gymnasium.core import ObsType, ActType
from numpy.random import Generator

from utils import generate_zipf, get_perf, get_imbalance

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
        self.features = rng.random(num_features, np.float32) * 0.6 + 0.7
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


class RtEnv(gym.Env):
    def __init__(self, config):
        super().__init__()
        self.parallelism = config["parallelism"]
        self.num_hot_keys = config["num_hot_keys"]
        self.max_steps = config["max_steps"]
        self.num_features_per_subtask = config["num_features_per_subtask"]
        self.zipf = config.get("zipf", 0)
        self.masks = np.array(config["masks"])  # a list of (worker_id, key_id) pairs

        self.key_dist = generate_zipf(self.zipf, self.num_hot_keys)

        self.subtasks = []
        self.rng = np.random.default_rng(SEED)
        self._generate_random_subtasks()
        self.episode = 0
        # print speed
        print("Speeds:", [subtask.get_speed() for subtask in self.subtasks])
        self.observation_space = gym.spaces.Box(
            low=np.float32(0),
            high=np.float32(2),
            shape=(self.parallelism * (1 + self.num_features_per_subtask),),
            dtype=np.float32,
        )
        self.action_space = gym.spaces.Box(
            low=np.float32(0),
            high=np.float32(1),
            shape=(len(self.masks),),
            dtype=np.float32,
        )
        self.current_step = 0
        self._obs = None
        self._info = None
        self._routing_table = None
        initial_action = np.ones((self.masks.shape[0],), np.float32)
        initial_routing_table = self._get_routing_table(initial_action)
        initial_times = self._get_times(initial_routing_table)
        self._get_obs(initial_routing_table)
        self.initial_imbalance = get_imbalance(initial_times)
        self.initial_perf = get_perf(initial_times)
        self._get_info(self.initial_imbalance, self.initial_perf)
        print(
            "Initial imbalance:",
            self.initial_imbalance,
            "Initial CV:",
            self.initial_perf,
        )
        self.prev_imbalance = self.initial_imbalance
        self.prev_perf = self.initial_perf

    def _generate_random_subtasks(self):
        self.subtasks.clear()
        self.rng = np.random.default_rng(
            SEED
        )  # So that the subtasks are the same for each episode
        for _ in range(self.parallelism):
            seed = self.rng.integers(0, 65536)
            self.subtasks.append(
                SimulatedSubtask(
                    self.num_features_per_subtask, np.random.default_rng(seed)
                )
            )

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
        obs = times / np.max(times)
        features = []
        for subtask in self.subtasks:
            features.extend(subtask.get_features())
        obs = np.concatenate((obs, np.array(features)), axis=0)
        self._obs = obs
        return obs

    def _get_info(self, imbalance, cv) -> dict[str, Any]:
        info = {"imbalance": imbalance, "cv": cv}
        self._info = info
        return info

    def _get_imbalance_reward(self, routing_table, imbalance) -> SupportsFloat:
        from_initial = (self.initial_imbalance - imbalance) / self.initial_imbalance
        from_prev = (self.prev_imbalance - imbalance) / self.prev_imbalance
        if from_initial > 0:
            r = ((1 + from_initial) ** 2 - 1) * abs(1 + from_prev)
        else:
            r = -((1 - from_initial) ** 2 - 1) * abs(1 - from_prev)
        # if from_prev < 0 < r:
        #     r = 0
        return r

    def _get_imbalance_reward_2(self, routing_table, imbalance) -> SupportsFloat:
        from_initial = (self.initial_imbalance - imbalance) / self.initial_imbalance
        return from_initial

    def _get_perf_reward(self, routing_table, perf) -> SupportsFloat:
        from_initial = (self.initial_perf - perf) / self.initial_perf
        from_prev = (self.prev_perf - perf) / self.prev_perf
        if from_initial > 0:
            r = ((1 + from_initial) ** 2 - 1) * abs(1 + from_prev)
        else:
            r = -((1 - from_initial) ** 2 - 1) * abs(1 - from_prev)
        return r

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
        self.prev_imbalance = self.initial_imbalance
        self.prev_perf = self.initial_perf
        self.episode += 1
        return self._get_obs(self._routing_table), self._get_info(
            self.initial_imbalance, self.initial_perf
        )

    def _get_routing_table(self, action):
        if action.ndim == 1:
            routing_table = np.zeros((self.parallelism, self.num_hot_keys), np.float32)
            routing_table[self.masks[:, 0], self.masks[:, 1]] = action
        else:
            routing_table = np.zeros(
                (action.shape[0], self.parallelism, self.num_hot_keys), np.float32
            )
            routing_table[:, self.masks[:, 0], self.masks[:, 1]] = action
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
        #     print("Action:", routing_table, "Imbalance:", imbalance, "CV:", cv, "Reward:", reward)
        info = self._get_info(imbalance, perf)
        obs = self._get_obs(routing_table)
        done = self.current_step >= self.max_steps
        # done = False
        for subtask in self.subtasks:
            subtask.next_window()
        self.current_step += 1
        # if self.episode == 1:
        #     print("Imbalance:", imbalance, "CV:", cv, "Reward:", reward)
        self.prev_imbalance = imbalance
        self.prev_perf = perf
        return obs, reward, done, False, info


if __name__ == "__main__":
    routing_table = np.array([[1, 1, 0], [0, 1, 0], [0, 0, 1]], np.float32)
    key_dist = np.array([10, 20, 30], np.int32)
    workload = get_workload(routing_table, key_dist)
    print(workload)

import logging
import socket
from typing import Any, SupportsFloat

import gymnasium as gym
import numpy as np
from gymnasium.core import ObsType, ActType

import pickle
import array

from environment.rt_env import get_workload

logger = logging.getLogger(__name__)

EPS = 1e-6
SEED = 88798


class RemoteEnv(gym.Env):
    def __init__(self, config):
        super().__init__()
        self.offline = "offline" in config and config["offline"]
        self.feature_dim = config["feature_dim"] * config["parallelism"]
        self.action_dim = config["action_dim"]
        self.dropped_dim = config["dropped_dim"]
        self.socket_host = config["socket_host"]
        self.socket_port = config["socket_port"]
        self.observation_space = gym.spaces.Box(
            low=np.float32(0),
            high=np.float32(2),
            shape=(self.feature_dim,),
            dtype=np.float32,
        )
        self.action_space = gym.spaces.Box(
            low=np.float32(0),
            high=np.float32(1),
            shape=(self.action_dim - self.dropped_dim,),
            dtype=np.float32,
        )
        self.total_steps = 0

        if self.offline:
            self.obs = np.zeros((self.feature_dim,), dtype=np.float32)
            self.info = {}
            return

        # Connect to ExternalEnv on Java side
        self.s = socket.socket()
        self.s.bind((self.socket_host, self.socket_port))
        self.s.listen(5)
        print(f"Listening on {self.socket_host}:{self.socket_port}...")
        self.c, addr = self.s.accept()
        print(f"Connection from {addr}")

        # Initial state
        initial_request = self._read_object()
        self.obs = np.array(initial_request["obs"], dtype=np.float32)
        self.info = initial_request["info"]

    def reset(
        self,
        *,
        seed: int | None = None,
        options: dict[str, Any] | None = None,
    ) -> tuple[ObsType, dict[str, Any]]:
        super().reset(seed=seed, options=options)
        # print("OBS shape:", self.obs.shape, "content:", self.obs)
        return self.obs, self.info

    def step(
        self, action: ActType
    ) -> tuple[ObsType, SupportsFloat, bool, bool, dict[str, Any]]:
        if self.offline:
            logger.info("Offline mode, returning random obs and reward")
            obs = np.random.rand(self.feature_dim).astype(np.float32)
            reward = np.random.rand()
            done = False
            return obs, reward, done, False, {}

        # print("Step: batch size =", action.shape[0] if action.ndim == 2 else 0)
        if action.ndim == 1:
            action = action.tolist()
        elif action.ndim == 2 and action.shape[0] == 1:
            action = action[0].tolist()
        else:
            raise ValueError(f"Invalid action shape: {action.shape}")

        action_arr = array.array("d", action)
        # print(
        #     f"Step {self.total_steps}: Sending the next action of type {type(action_arr)}"
        # )
        self._dump_object(dict(action=action_arr))
        # print(f"Step {self.total_steps}: Action sent, reading the response...")
        response = self._read_object()
        # print(f"Step {self.total_steps}: Response received.")
        self.obs = np.array(response["obs"], dtype=np.float32)
        self.info = response["info"]
        # print(f"Step {self.total_steps} actions: {action}")
        # print(f"Step {self.total_steps} new_obs: {self.obs}")
        reward = response["reward"]
        done = response["done"]
        self.total_steps += 1
        return self.obs, reward, done, False, self.info

    def close(self):
        if self.offline:
            return
        self.c.close()
        self.s.close()

    def _read_object(self):
        # Read an integer
        length_bytes = self._read_complete_bytes(4)
        length = int.from_bytes(length_bytes, byteorder="big")
        # Read the data
        data = self._read_complete_bytes(length)
        return pickle.loads(data)

    def _dump_object(self, obj):
        data = pickle.dumps(obj)
        length = len(data).to_bytes(4, byteorder="big")
        # print(f"Sending {len(data)} bytes...")
        self.c.sendall(length + data)

    def _read_complete_bytes(self, length):
        data = b""
        while len(data) < length:
            data += self.c.recv(length - len(data))
        return data


if __name__ == "__main__":
    arr = [1, 2, 3, 4, 5]
    data = pickle.dumps(arr)
    print(data)
    length = len(data).to_bytes(4, byteorder="big")

    arr1 = pickle.loads(data)
    print(arr1)

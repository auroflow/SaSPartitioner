import base64
import os
import pickle
from typing import Optional

import lz4
import numpy as np
import ray
from ray.data import Dataset
from ray.rllib.core import Columns
from ray.rllib.utils.compression import pack, unpack

from configurations import Config
from utils import (
    get_masks_from_mask_indexes,
    normalize,
    get_perf_reward_from_initial,
    get_perf,
    get_imbalance,
    get_num_true_hot_keys_from_masks,
)


def peak_data(path):
    ds = ray.data.read_parquet(path).take(20)
    for i in range(len(ds)):
        obs = ds[i][Columns.OBS]
        act = ds[i][Columns.ACTIONS]
        rew = ds[i][Columns.REWARDS]
        next_obs = ds[i][Columns.NEXT_OBS]
        print(f"Obs: {obs}\nAct: {act}\nRew: {rew}\nNext obs: {next_obs}")


def read_log(log_paths: list[str]) -> Dataset:
    """
    Dataset columns:
        eps_id (string),
        agent_id (null),
        module_id (null),
        obs (string, packed),
        actions (string, packed),
        rewards (double),
        new_obs (string, packed),
        terminateds (bool),
        truncateds (bool),
        weights_seq_no (int64, 0)
    :param path:
    :return:
    """
    conf = Config()
    mask_indexes = conf.mask_indexes
    num_keys = get_num_true_hot_keys_from_masks(
        get_masks_from_mask_indexes(mask_indexes)
    )
    print("Num keys:", num_keys)

    # samples: ts, worker stats, metrics, actions
    samples = list[tuple[int, list[float], list[float], list[float]]]()

    worker_stats: Optional[tuple[int, list[float]]] = None
    metrics: Optional[tuple[int, list[float]]] = None
    action: Optional[tuple[int, list[float]]] = None

    def parse_array_parts(parts: list[str]):
        arr = list[float]()
        for part in parts:
            if part.startswith("["):
                part = part[1:]
            if part.endswith("]"):
                part = part[:-1]
            if part.endswith(","):
                part = part[:-1]
            arr.append(float(part))
        return arr

    # First read from the files.
    for log_path in log_paths:
        print("Reading", log_path)
        with open(log_path, "r") as f:
            line = f.readline()
            while line:
                read_new_line = True
                line = line.strip()

                if "non-idle times" in line:
                    # 2024-12-24 08:08:41,025 INFO  cn.edu.zju.daily.metricflux.partitionerv2.learning.AutoExternalEnv [] - Metric id 1734998921000 imbalance: 0.3060596901654326, nmad: 0.10250954119875895, reward: 0.41512640881864626, non-idle times: [...]
                    parts = line.split(" [] - ")[1].split()
                    ts = int(parts[2])
                    mtr = parse_array_parts(parts[11:])
                    mtr = [(m / 1000 if m < 1000 else 2) for m in mtr]
                    metrics = (ts, mtr)
                elif "worker statistics" in line:
                    # 2024-12-24 08:08:41,026 INFO  cn.edu.zju.daily.metricflux.partitionerv2.containerv2.StaticHotKeyCBandit [] - Slide 1734998921000: worker statistics: [1976, ...]
                    parts = line.split(" [] - ")[1].split()
                    ts = int(parts[1][:-1])
                    st = parse_array_parts(parts[4:])
                    normalize(st)
                    worker_stats = (ts, st)
                elif "partition table" in line:
                    # 2024-12-24 08:08:41,026 INFO  cn.edu.zju.daily.metricflux.partitionerv2.containerv2.StaticHotKeyCBandit [] - Slide 1734998921000: partition table:
                    #   0: [100, 100, 100, 0, 0, 0, ...]
                    #   1: [0, 0, 0, 100, 100, 100, ...]
                    parts = line.split(" [] - ")[1].split()
                    ts = int(parts[1][:-1])
                    act = list[float]()
                    i = 0
                    while i < num_keys:
                        newline = f.readline()
                        if not newline:
                            break
                        if not newline.startswith("  "):
                            line = newline
                            read_new_line = False
                            break
                        parts = newline.strip().split()
                        key = int(parts[0][:-1])
                        while i < key and i < num_keys:
                            print(f"{i} is missing")
                            act.extend([0.0] * len(mask_indexes[i]))
                            i += 1
                        if i == key and i < num_keys:
                            key_partition = parse_array_parts(parts[1:])
                            for j in mask_indexes[i]:
                                act.append(key_partition[j])
                        i += 1
                    max_act = max(act)
                    act = [a / max_act for a in act]
                    action = (ts, act)

                if worker_stats and metrics and action:
                    if worker_stats[0] == metrics[0] == action[0]:
                        ts = worker_stats[0]
                        samples.append((ts, worker_stats[1], metrics[1], action[1]))
                        worker_stats = metrics = action = None
                if read_new_line:
                    line = f.readline()

    # Then convert them to RL-compatible datasets.
    dataset = list[dict[str, any]]()
    step = 0
    eps_id = 0
    num_steps = conf.max_steps
    initial_perf = conf.remote_env.initial_perf
    for i in range(1, len(samples)):
        perf = get_perf(samples[i][2])

        if conf.remote_env.offline.include_workload_features:
            obs = np.array(samples[i - 1][1] + samples[i - 1][2])
            next_obs = np.array(samples[i][1] + samples[i][2])
        else:
            obs = np.array(samples[i - 1][2])
            next_obs = np.array(samples[i][2])

        data = {
            Columns.EPS_ID: str(eps_id),
            Columns.AGENT_ID: None,
            Columns.MODULE_ID: None,
            Columns.OBS: obs,
            Columns.ACTIONS: np.array(samples[i][3]),
            Columns.REWARDS: get_perf_reward_from_initial(perf, initial_perf),
            Columns.NEXT_OBS: next_obs,
            Columns.TERMINATEDS: step == num_steps - 1,
            Columns.TRUNCATEDS: False,
            # "weights_seq_no": 0,
        }
        dataset.append(data)
        step += 1
        if step == num_steps:
            step = 0
            eps_id += 1

    print("Obs shape:", dataset[0][Columns.OBS].shape)
    print("Action shape:", dataset[0][Columns.ACTIONS].shape)
    return ray.data.from_items(dataset)


if __name__ == "__main__":
    conf = Config()
    log_folder = conf.remote_env.offline.log_folder
    # log_folder = "/home/user/code/saspartitioner/scripts/rl/offline_data/log/original_logs"
    log_paths = [
        os.path.join(log_folder, s)
        for s in os.listdir(log_folder)
        if "-taskexecutor-" in s and ".log" in s
    ]
    log_paths.sort(key=lambda x: x + ".9" if x.endswith(".log") else x)

    ds = read_log(log_paths)
    ds.write_parquet(
        conf.remote_env.offline.data_path,
        # ray_remote_args={'num_gpus': min(1, conf.num_gpus)},
    )

    path = os.path.join(conf.remote_env.offline.data_path, "0_000000_000000.parquet")
    peak_data(path)

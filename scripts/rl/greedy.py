from random import shuffle

import numpy as np

from environment.rt_env import RtEnv
from utils import generate_zipf, get_imbalance

parallelism = 64
num_hot_keys = 10
zipf = 1.5
gamma = 0.99
num_features_per_worker = 3
max_steps = 128


mask_indexes = [
    list(range(0, 33)),
    list(range(32, 44)),
    list(range(43, 50)),
    list(range(49, 54)),
    list(range(53, 57)),
    list(range(56, 59)),
    list(range(58, 61)),
    list(range(60, 62)),
    list(range(61, 63)),
    list(range(62, 64)),
]

masks = []
for i in range(len(mask_indexes)):
    item = mask_indexes[i]
    for j in range(len(item)):
        masks.append([item[j], i])
print("Mask:", masks)


if __name__ == "__main__":
    env_config = {
        "parallelism": parallelism,
        "max_steps": max_steps,
        "num_hot_keys": num_hot_keys,
        "zipf": zipf,
        "masks": masks,
        "num_features_per_subtask": num_features_per_worker,
    }

    env = RtEnv(config=env_config)
    speeds = np.array([subtask.get_speed() for subtask in env.subtasks])

    key_sizes = generate_zipf(zipf, num_hot_keys)
    pool = []
    for i in range(num_hot_keys):
        for j in range(key_sizes[i]):
            pool.append(i)

    shuffle(pool)

    total_time = 100000
    weights = total_time / speeds
    print("Weights:", weights)

    sizes = np.zeros_like(speeds)

    for key in pool:
        allowed = mask_indexes[key]
        # Choose the smallest size among allowed partitions
        chosen = np.argmin(sizes[allowed]) + allowed[0]
        sizes[chosen] += weights[key]

    print("Sizes:", sizes)
    print("Imbalance", get_imbalance(sizes))

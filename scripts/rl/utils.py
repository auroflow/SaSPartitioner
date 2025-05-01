import numpy as np
from scipy.stats import zipfian

EPS = 1e-6
ZIPF_N = 10000


def generate_zipf(
    alpha: float, length: int, *, data_size=1000000, get_freq=False
) -> np.ndarray:
    x = np.arange(1, length + 1)
    y = zipfian.pmf(x, alpha, ZIPF_N)
    if get_freq:
        return y
    else:
        y = y * data_size
        return y.astype(np.int32)


def get_mask_indexes_heuristic(
    *,
    distribution_file="",
    alpha: float = 0.0,
    parallelism: int,
    spread_keys=0,
    num_hot_keys=-1,
    num_true_hot_keys=-1,
):

    if distribution_file:
        d = read_distribution(distribution_file)
        d = np.array(d) / np.sum(d)
        print("Distribution:", d)
        if num_hot_keys <= 0:
            num_hot_keys = len(d)
    else:
        assert num_hot_keys > 0
        d = generate_zipf(alpha, num_hot_keys, get_freq=True)
        print(f"Zipf-{alpha}:", d)

    cum_d = np.cumsum(d)
    if num_true_hot_keys <= 0:
        num_true_hot_keys = np.argmax(parallelism * d <= cum_d).item()

    print(
        f"get_mask_indexes_heuristic: num_hot_keys = {num_hot_keys}, num_true_hot_keys = {num_true_hot_keys}"
    )
    spread_keys = min(spread_keys, num_true_hot_keys)

    mask_indexes = []
    for i in range(spread_keys):
        mask_indexes.append(list(range(0, parallelism)))
    if spread_keys < num_true_hot_keys:
        # For the remaining hot keys, assign all parallelisms according to their size
        cs = np.cumsum(
            d[spread_keys:num_true_hot_keys]
            / np.sum(d[spread_keys:num_true_hot_keys])
            * parallelism
        )
        cs = np.concatenate(([0] * (1 + spread_keys), cs))
        for i in range(spread_keys, num_true_hot_keys):
            lower = np.floor(cs[i]).astype(np.int32)
            upper = np.minimum(parallelism, np.ceil(cs[i + 1]).astype(np.int32))
            if i < 20:
                print(f"Key {i}: {lower} - {upper}")
            mask_indexes.append(list(range(lower, upper)))
    return mask_indexes


def get_masks_from_mask_indexes(mask_indexes) -> list[list[int]]:
    """
    Returns a list of permitted (worker_id, key_id) pairs.
    :param mask_indexes: mask indexes (a list of worker id lists for each key)
    :return:
    """
    masks = []
    for i in range(len(mask_indexes)):
        item = mask_indexes[i]
        for j in range(len(item)):
            masks.append([item[j], i])
    return masks


def get_num_true_hot_keys_from_masks(masks: list[list[int]]) -> int:
    return len(set([item[1] for item in masks]))


def normalize(x: list[float]) -> None:
    mean = np.mean(x)
    std = np.std(x)
    range_factor = 5 * std

    for i in range(len(x)):
        x[i] = (x[i] - mean) / range_factor + 1
        x[i] = max(0.0, min(2.0, x[i]))


def get_perf_reward_from_initial(perf: float, initial_perf: float) -> float:
    from_initial = (initial_perf - perf) / initial_perf
    if from_initial > 0:
        r = (1 + from_initial) ** 2 - 1
    else:
        r = -((1 - from_initial) ** 2 - 1)
    return r


def get_perf(arr: np.ndarray | list[float]) -> float:
    """half imbalance, half nmad"""
    nmad = np.sum(np.abs(arr - np.mean(arr))) / (np.mean(arr) * len(arr))
    imbalance = get_imbalance(arr)
    return (nmad + imbalance) / 2


def get_imbalance(arr: np.ndarray) -> float:
    imbalance = (np.max(arr) - np.mean(arr)) / (np.mean(arr) + EPS)
    if imbalance < -EPS:
        raise ValueError("imbalance < 0, arr = " + str(arr))
    return imbalance


def read_distribution(filename: str) -> list[int]:
    with open(filename, "r") as f:
        d = f.readlines()
    d = [int(item.strip().split(",")[-1]) for item in d]
    return d


if __name__ == "__main__":
    file = "/home/user/code/data/t4sa-freq.csv"
    idxs = get_mask_indexes_heuristic(
        distribution_file=file,
        alpha=1.5,
        parallelism=128,
        num_hot_keys=0,
        num_true_hot_keys=0,
    )
    print([i[0] for i in idxs])
    print([i[-1] + 1 for i in idxs])

    # mask_indexes = get_mask_indexes_heuristic(distribution_file=file, parallelism=64, spread_keys=1,
    #                                           num_hot_keys=10)

    #
    # action_dim = sum([len(item) for item in mask_indexes])
    # print(action_dim)
    #
    # print(read_distribution("/home/user/code/data/election2024-freq.csv"))
    # print("Mask indexes (inclusive - exclusive):")
    # for i, item in enumerate(mask_indexes):
    #     print(f"{i}: {item[0]} - {item[-1]}")
    #
    # masks = get_masks_from_mask_indexes(mask_indexes)
    # print(masks)
    #
    # d = generate_zipf(1.0, 10, get_freq=True)
    # print("zipf 1.0:", d)
    # d = generate_zipf(1.5, 10, get_freq=True)
    # print("zipf 1.5:", d)
    # # cumsum_dist = np.cumsum(d)
    # # print(np.argmax(cumsum_dist / parallelism > 0.01))

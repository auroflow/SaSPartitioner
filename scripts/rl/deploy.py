import pandas as pd
from ray import train, tune
from ray.rllib.algorithms import SAC, Algorithm
from ray.train import Checkpoint, RunConfig, CheckpointConfig
import tempfile

from ray.tune import TuneConfig

from algo.sac_online import get_sac_config
from configurations import Config
from environment.rt_env import RtEnv
from pprint import pprint

from ray.tune.registry import register_env

register_env("RtEnv", RtEnv)


def trainable(config: dict):
    conf = Config()
    config, algo_class = get_sac_config("RtEnv", conf)

    if conf.simulator_env.checkpoint_path:
        checkpoint = Checkpoint.from_directory(conf.simulator_env.checkpoint_path)

        # It seems that alpha is lost when loading the checkpoint.
        with checkpoint.as_directory() as chkpt_dir:
            algo = Algorithm.from_checkpoint(chkpt_dir)
        previous_alpha = algo.get_state()["learner_group"]["learner"]["metrics_logger"][
            "stats"
        ][("default_policy", "alpha_value")]["values"][0]
        print("Previous alpha:", previous_alpha)

        # Initialize again with correct initial alpha
        config.training(initial_alpha=previous_alpha)
        algo = config.build()
        with checkpoint.as_directory() as chkpt_dir:
            algo.restore_from_path(chkpt_dir)
    else:
        algo = config.build()

    for i in range(conf.simulator_env.num_iters):
        result = algo.train()

        with tempfile.TemporaryDirectory() as tmpdir:
            algo.save_to_path(tmpdir)
            checkpoint = Checkpoint.from_directory(tmpdir)
            train.report(result + config.to_dict(), checkpoint=checkpoint)

    # new_model, trainable = get_sac_config(RtEnv, conf)
    # weights = sac.get_weights()


def train_with_custom_trainable():
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


def train_with_algo_class():
    conf = Config()
    config, algo_class = get_sac_config("RtEnv", conf)
    tuner = tune.Tuner(
        algo_class,
        param_space=config,
        run_config=RunConfig(
            stop={
                "env_runners/negative_imbalance": -0.01,
                "training_iteration": conf.simulator_env.num_iters,
            },
            checkpoint_config=CheckpointConfig(
                checkpoint_at_end=True,
            ),
        ),
    )
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


if __name__ == "__main__":
    train_with_algo_class()
    # train_with_custom_trainable()

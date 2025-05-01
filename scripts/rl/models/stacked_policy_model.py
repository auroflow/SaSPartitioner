from typing import Dict, List

import gymnasium as gym
import torch
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils import override
from ray.rllib.utils.torch_utils import FLOAT_MIN
from ray.rllib.utils.typing import ModelConfigDict, TensorType
from torch import nn


class StackedPolicyModel(TorchModelV2, nn.Module):
    """Generic fully connected network."""

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
        **custom_model_config
    ):
        """
        :param obs_space:
        :param action_space:
        :param num_outputs:
        :param model_config:
        :param name:
        :param custom_model_config:
        """
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)
        self.num_groups = custom_model_config["num_workers"]
        self.num_inputs_per_group = custom_model_config["num_features_per_worker"]

        # assert self.num_inputs_per_group * self.num_groups == obs_space.shape[
        #     0], f"Expected {self.num_inputs_per_group} * {self.num_groups} inputs, got {obs_space.shape[0]}"

        self.shared_mlp = nn.Sequential(
            nn.Linear(self.num_inputs_per_group * self.num_groups, 128),
            nn.LeakyReLU(negative_slope=0.2),
            nn.BatchNorm1d(num_features=128),
            nn.Linear(128, 128),
            nn.Tanh(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.Tanh(),
            nn.Linear(64, 1),
        )
        self.final_layer = nn.Linear(self.num_groups, num_outputs)
        action_mask = custom_model_config.get("action_mask")
        if action_mask is not None:
            self.action_mask = torch.clamp(
                torch.log(torch.tensor(action_mask)), min=FLOAT_MIN
            )
        else:
            self.action_mask = None

    @override(TorchModelV2)
    def forward(
        self,
        input_dict: Dict[str, TensorType],
        state: List[TensorType],
        seq_lens: TensorType,
    ) -> (TensorType, List[TensorType]):
        obs = input_dict["obs_flat"].float()
        batch_size = obs.shape[0]
        obs = obs.view(-1, 1, self.num_inputs_per_group)
        shared_out = self.shared_mlp(obs)
        shared_out = shared_out.view(batch_size, -1)
        out = self.final_layer(shared_out)

        if self.action_mask is not None:
            out = out + self.action_mask

        print("Policy net output:", out)

        return out, state

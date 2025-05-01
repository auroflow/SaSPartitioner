import numpy as np
from ray.rllib.algorithms.sac.sac_catalog import SACCatalog

import gymnasium as gym
from ray.rllib.core.models.base import Encoder, Model

from models.sac_n.config import MLPEnsembleEncoderConfig, MLPEnsembleHeadConfig


class SACNCatalog(SACCatalog):
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
        view_requirements: dict = None,
    ):
        assert (
            "ensemble_size" in model_config_dict
        ), "Ensemble size must be provided in model_config_dict."

        super().__init__(
            observation_space, action_space, model_config_dict, view_requirements
        )
        self.ensemble_size = self._model_config_dict["ensemble_size"]
        self.qf_head_config = MLPEnsembleHeadConfig(
            ensemble_size=self.ensemble_size,
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_qf_head_hiddens,
            hidden_layer_activation=self.pi_and_qf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=1,
        )

    def build_qf_encoder(self, framework: str) -> Encoder:
        # Compute the required dimension for the action space.
        required_action_dim = self.action_space.shape[0]

        # Encoder input for the Q-network contains state and action. We
        # need to infer the shape for the input from the state and action
        # spaces
        if (
            isinstance(self.observation_space, gym.spaces.Box)
            and len(self.observation_space.shape) == 1
        ):
            input_space = gym.spaces.Box(
                -np.inf,
                np.inf,
                (self.observation_space.shape[0] + required_action_dim,),
                dtype=np.float32,
            )
        else:
            raise ValueError("The observation space is not supported by RLlib's SAC.")

        self.qf_encoder_hiddens = self._model_config_dict["fcnet_hiddens"][:-1]
        self.qf_encoder_activation = self._model_config_dict["fcnet_activation"]

        self.qf_encoder_config = MLPEnsembleEncoderConfig(
            input_dims=input_space.shape,
            ensemble_size=self.ensemble_size,
            hidden_layer_dims=self.qf_encoder_hiddens,
            hidden_layer_activation=self.qf_encoder_activation,
            output_layer_dim=self.latent_dims[0],
            output_layer_activation=self.qf_encoder_activation,
        )

        return self.qf_encoder_config.build(framework=framework)

    def build_qf_head(self, framework: str) -> Model:
        return self.qf_head_config.build(framework=framework)

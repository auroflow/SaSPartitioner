from ray.rllib.algorithms.sac.sac_catalog import SACCatalog
from ray.rllib.core.models.configs import MLPHeadConfig


class SACClusterCatalog(SACCatalog):
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
        view_requirements: dict = None,
    ):
        assert (
            "cluster_size" in model_config_dict
        ), "Cluster size must be provided in model_config_dict."

        super().__init__(
            observation_space, action_space, model_config_dict, view_requirements
        )
        self.cluster_size = self._model_config_dict["cluster_size"]
        self.qf_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_qf_head_hiddens,
            hidden_layer_activation=self.pi_and_qf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=self.cluster_size,
        )

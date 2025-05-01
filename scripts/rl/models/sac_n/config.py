from dataclasses import dataclass

from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.models.configs import MLPEncoderConfig, MLPHeadConfig


@dataclass
class MLPEnsembleEncoderConfig(MLPEncoderConfig):
    ensemble_size: int = None

    def _validate(self, framework: str = "torch"):
        super()._validate(framework)
        assert self.ensemble_size is not None, "Ensemble size must be provided."

    def build(self, framework: str = "torch") -> Encoder:
        self._validate(framework)

        if framework == "torch":
            from models.sac_n.model import TorchMLPEnsembleEncoder

            return TorchMLPEnsembleEncoder(self)
        else:
            raise NotImplementedError(f"Framework {framework} is not supported.")


@dataclass
class MLPEnsembleHeadConfig(MLPHeadConfig):
    ensemble_size: int = None

    def _validate(self, framework: str = "torch"):
        super()._validate(framework)
        assert self.ensemble_size is not None, "Ensemble size must be provided."

    def build(self, framework: str = "torch") -> Model:
        self._validate(framework)

        if framework == "torch":
            from models.sac_n.model import TorchMLPEnsembleHead

            return TorchMLPEnsembleHead(self)
        else:
            raise NotImplementedError(f"Framework {framework} is not supported.")

import math
from dataclasses import dataclass
from typing import Optional, Union, Callable, Dict, List

from ray.rllib.core import Columns
from ray.rllib.core.models.base import Encoder, ENCODER_OUT, Model
from ray.rllib.core.models.configs import MLPEncoderConfig
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.models.utils import get_activation_fn, get_initializer_fn
from ray.rllib.utils import override
from torch import nn
import torch

from models.sac_n.config import MLPEnsembleEncoderConfig, MLPEnsembleHeadConfig


class VectorizedLinear(nn.Module):
    def __init__(
        self,
        in_features: int,
        out_features: int,
        ensemble_size: int,
        bias: bool = True,
        device=None,
        dtype=None,
    ) -> None:
        factory_kwargs = {"device": device, "dtype": dtype}
        super().__init__()
        self.in_features = in_features
        self.out_features = out_features
        self.ensemble_size = ensemble_size

        self.weight = nn.Parameter(
            torch.empty((ensemble_size, out_features, in_features), **factory_kwargs)
        )
        if bias:
            self.bias = nn.Parameter(
                torch.empty((ensemble_size, out_features), **factory_kwargs)
            )
        else:
            self.bias = None
            self.register_parameter("bias", None)

        self.reset_parameters()

    def reset_parameters(self):
        for layer in range(self.ensemble_size):
            nn.init.kaiming_uniform_(self.weight[layer], a=math.sqrt(5))

        if self.bias is not None:
            fan_in, _ = nn.init._calculate_fan_in_and_fan_out(self.weight[0])
            bound = 1 / math.sqrt(fan_in) if fan_in > 0 else 0
            nn.init.uniform_(self.bias, -bound, bound)

    def forward(self, input: torch.Tensor) -> torch.Tensor:
        assert len(input.shape) == 3, (
            f"shape {input.shape} is incorrect, "
            "should be [ensemble_size, batch_size, in_features]"
        )
        bmm = torch.bmm(
            input, self.weight.transpose(1, 2)
        )  # [ensemble_size, batch_size, out_features]
        if self.bias is not None:
            return bmm + self.bias.unsqueeze(1)
        else:
            return bmm

    def extra_repr(self) -> str:
        return f"in_features={self.in_features}, out_features={self.out_features}, ensemble_size={self.ensemble_size}"


class TorchMLPEnsemble(nn.Module):
    """A multi-layer perceptron with N dense layers.

    All layers (except for an optional additional extra output layer) share the same
    activation function, bias setup (use bias or not), and LayerNorm setup
    (use layer normalization or not).

    If `output_dim` (int) is not None, an additional, extra output dense layer is added,
    which might have its own activation function (e.g. "linear"). However, the output
    layer does NOT use layer normalization.
    """

    def __init__(
        self,
        *,
        input_dim: int,
        ensemble_size: int,
        hidden_layer_dims: List[int],
        hidden_layer_activation: Union[str, Callable] = "relu",
        hidden_layer_use_bias: bool = True,
        hidden_layer_use_layernorm: bool = False,
        hidden_layer_weights_initializer: Optional[Union[str, Callable]] = None,
        hidden_layer_weights_initializer_config: Optional[Union[str, Callable]] = None,
        hidden_layer_bias_initializer: Optional[Union[str, Callable]] = None,
        hidden_layer_bias_initializer_config: Optional[Dict] = None,
        output_dim: Optional[int] = None,
        output_use_bias: bool = True,
        output_activation: Union[str, Callable] = "linear",
        output_weights_initializer: Optional[Union[str, Callable]] = None,
        output_weights_initializer_config: Optional[Dict] = None,
        output_bias_initializer: Optional[Union[str, Callable]] = None,
        output_bias_initializer_config: Optional[Dict] = None,
    ):
        """Initialize a TorchVectorizedMLP object, used for Q-ensemble networks.

        Args:
            input_dim: The input dimension of the network. Must not be None.
            ensemble_size: Q-ensemble size.
            hidden_layer_dims: The sizes of the hidden layers. If an empty list, only a
                single layer will be built of size `output_dim`.
            hidden_layer_use_layernorm: Whether to insert a LayerNormalization
                functionality in between each hidden layer's output and its activation.
            hidden_layer_use_bias: Whether to use bias on all dense layers (excluding
                the possible separate output layer).
            hidden_layer_activation: The activation function to use after each layer
                (except for the output). Either a torch.nn.[activation fn] callable or
                the name thereof, or an RLlib recognized activation name,
                e.g. "ReLU", "relu", "tanh", "SiLU", or "linear".
            hidden_layer_weights_initializer: The initializer function or class to use
                forweights initialization in the hidden layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            hidden_layer_weights_initializer_config: Configuration to pass into the
                initializer defined in `hidden_layer_weights_initializer`.
            hidden_layer_bias_initializer: The initializer function or class to use for
                bias initialization in the hidden layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            hidden_layer_bias_initializer_config: Configuration to pass into the
                initializer defined in `hidden_layer_bias_initializer`.
            output_dim: The output dimension of the network. If None, no specific output
                layer will be added and the last layer in the stack will have
                size=`hidden_layer_dims[-1]`.
            output_use_bias: Whether to use bias on the separate output layer,
                if any.
            output_activation: The activation function to use for the output layer
                (if any). Either a torch.nn.[activation fn] callable or
                the name thereof, or an RLlib recognized activation name,
                e.g. "ReLU", "relu", "tanh", "SiLU", or "linear".
            output_layer_weights_initializer: The initializer function or class to use
                for weights initialization in the output layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            output_layer_weights_initializer_config: Configuration to pass into the
                initializer defined in `output_layer_weights_initializer`.
            output_layer_bias_initializer: The initializer function or class to use for
                bias initialization in the output layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            output_layer_bias_initializer_config: Configuration to pass into the
                initializer defined in `output_layer_bias_initializer`.
        """
        super().__init__()
        assert input_dim > 0

        self.input_dim = input_dim
        self.ensemble_size = ensemble_size

        hidden_activation = get_activation_fn(
            hidden_layer_activation, framework="torch"
        )
        hidden_weights_initializer = get_initializer_fn(
            hidden_layer_weights_initializer, framework="torch"
        )
        hidden_bias_initializer = get_initializer_fn(
            hidden_layer_bias_initializer, framework="torch"
        )
        output_weights_initializer = get_initializer_fn(
            output_weights_initializer, framework="torch"
        )
        output_bias_initializer = get_initializer_fn(
            output_bias_initializer, framework="torch"
        )

        layers = []
        dims = (
            [self.input_dim]
            + list(hidden_layer_dims)
            + ([output_dim] if output_dim else [])
        )
        for i in range(0, len(dims) - 1):
            # Whether we are already processing the last (special) output layer.
            is_output_layer = output_dim is not None and i == len(dims) - 2

            layer = VectorizedLinear(
                dims[i],
                dims[i + 1],
                ensemble_size,
                bias=output_use_bias if is_output_layer else hidden_layer_use_bias,
            )
            # Initialize layers, if necessary.
            if is_output_layer:
                # Initialize output layer weigths if necessary.
                if output_weights_initializer:
                    output_weights_initializer(
                        layer.weight, **output_weights_initializer_config or {}
                    )
                # Initialize output layer bias if necessary.
                if output_bias_initializer:
                    output_bias_initializer(
                        layer.bias, **output_bias_initializer_config or {}
                    )
            # Must be hidden.
            else:
                # Initialize hidden layer weights if necessary.
                if hidden_layer_weights_initializer:
                    hidden_weights_initializer(
                        layer.weight, **hidden_layer_weights_initializer_config or {}
                    )
                # Initialize hidden layer bias if necessary.
                if hidden_layer_bias_initializer:
                    hidden_bias_initializer(
                        layer.bias, **hidden_layer_bias_initializer_config or {}
                    )

            layers.append(layer)

            # We are still in the hidden layer section: Possibly add layernorm and
            # hidden activation.
            if not is_output_layer:
                # Insert a layer normalization in between layer's output and
                # the activation.
                if hidden_layer_use_layernorm:
                    # We use an epsilon of 0.001 here to mimick the Tf default behavior.
                    # The last dimension will be normalized whose dim is expected to be dims[i + 1].
                    layers.append(nn.LayerNorm(dims[i + 1], eps=0.001))
                # Add the activation function.
                if hidden_activation is not None:
                    layers.append(hidden_activation())

        # Add output layer's (if any) activation.
        output_activation = get_activation_fn(output_activation, framework="torch")
        if output_dim is not None and output_activation is not None:
            layers.append(output_activation())

        self.mlp = nn.Sequential(*layers)

    def forward(self, x):
        # For encoder, x: [batch_size, state_dim + action_dim (feature_dim)]
        # For head, x: [ensemble_size, batch_size, encoder_output_dim]
        # print("Ensemble input original shape:", x.shape)
        if x.dim() != 3:
            assert x.dim() == 2
            x = x.unsqueeze(0).repeat_interleave(self.ensemble_size, dim=0)
        # x: [ensemble_size, batch_size, feature_dim]
        assert x.dim() == 3
        assert x.shape[0] == self.ensemble_size
        # print("   Ensemble input interleaved:", torch.mean(x, dim=(1, 2)), "shape:", x.shape)
        # For encoder, output: [ensemble_size, batch_size, encoder_output_dim]
        # For head, output: [ensemble_size, batch_size]
        return self.mlp(x).squeeze(-1)


class TorchMLPEnsembleEncoder(TorchModel, Encoder):
    def __init__(self, config: MLPEnsembleEncoderConfig):
        super().__init__(config)

        self.net = TorchMLPEnsemble(
            input_dim=config.input_dims[0],
            ensemble_size=config.ensemble_size,
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            hidden_layer_use_bias=config.hidden_layer_use_bias,
            hidden_layer_weights_initializer=config.hidden_layer_weights_initializer,
            hidden_layer_weights_initializer_config=(
                config.hidden_layer_weights_initializer_config
            ),
            hidden_layer_bias_initializer=config.hidden_layer_bias_initializer,
            hidden_layer_bias_initializer_config=(
                config.hidden_layer_bias_initializer_config
            ),
            output_dim=config.output_layer_dim,
            output_activation=config.output_layer_activation,
            output_use_bias=config.output_layer_use_bias,
            output_weights_initializer=config.output_layer_weights_initializer,
            output_weights_initializer_config=(
                config.output_layer_weights_initializer_config
            ),
            output_bias_initializer=config.output_layer_bias_initializer,
            output_bias_initializer_config=config.output_layer_bias_initializer_config,
        )

    @override(Model)
    def _forward(self, input_dict: dict, **kwargs) -> dict:
        return {ENCODER_OUT: self.net(input_dict[Columns.OBS])}


class TorchMLPEnsembleHead(TorchModel):
    def __init__(self, config: MLPEnsembleHeadConfig):
        super().__init__(config)

        self.net = TorchMLPEnsemble(
            input_dim=config.input_dims[0],
            ensemble_size=config.ensemble_size,
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            hidden_layer_use_bias=config.hidden_layer_use_bias,
            hidden_layer_weights_initializer=config.hidden_layer_weights_initializer,
            hidden_layer_weights_initializer_config=(
                config.hidden_layer_weights_initializer_config
            ),
            hidden_layer_bias_initializer=config.hidden_layer_bias_initializer,
            hidden_layer_bias_initializer_config=(
                config.hidden_layer_bias_initializer_config
            ),
            output_dim=config.output_layer_dim,
            output_activation=config.output_layer_activation,
            output_use_bias=config.output_layer_use_bias,
            output_weights_initializer=config.output_layer_weights_initializer,
            output_weights_initializer_config=(
                config.output_layer_weights_initializer_config
            ),
            output_bias_initializer=config.output_layer_bias_initializer,
            output_bias_initializer_config=config.output_layer_bias_initializer_config,
        )
        # If log standard deviations should be clipped. This should be only true for
        # policy heads. Value heads should never be clipped.
        self.clip_log_std = config.clip_log_std
        # The clipping parameter for the log standard deviation.
        self.log_std_clip_param = torch.Tensor([config.log_std_clip_param])
        # Register a buffer to handle device mapping.
        self.register_buffer("log_std_clip_param_const", self.log_std_clip_param)

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        # Only clip the log standard deviations, if the user wants to clip. This
        # avoids also clipping value heads.
        if self.clip_log_std:
            # Forward pass.
            means, log_stds = torch.chunk(self.net(inputs), chunks=2, dim=-1)
            # Clip the log standard deviations.
            log_stds = torch.clamp(
                log_stds, -self.log_std_clip_param_const, self.log_std_clip_param_const
            )
            return torch.cat((means, log_stds), dim=-1)
        # Otherwise just return the logits.
        else:
            return self.net(inputs)

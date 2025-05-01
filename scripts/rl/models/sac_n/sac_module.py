from abc import abstractmethod
from time import sleep
from typing import List, Tuple, Dict, Any

from ray.rllib import SampleBatch
from ray.rllib.algorithms.sac.sac_learner import (
    QF_PREDS,
    ACTION_DIST_INPUTS_NEXT,
    QF_TWIN_PREDS,
)
from ray.rllib.core import Columns
from ray.rllib.core.learner.utils import make_target_network
from ray.rllib.core.models.base import Encoder, Model, ENCODER_OUT
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module import RLModule
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI, TargetNetworkAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils import override
from ray.rllib.utils.typing import NetworkType

import torch
from torch import Tensor
from configurations import Config

conf = Config()


class SACNRLModule(RLModule, InferenceOnlyAPI, TargetNetworkAPI):

    def __init__(self):
        super().__init__()

    @override(RLModule)
    def setup(self):
        if self.catalog is None and hasattr(self, "_catalog_ctor_error"):
            raise self._catalog_ctor_error

        if self.model_config.get("twin_q") == True:
            raise ValueError("twin_q should be false for SAC-N RLModule.")
        self.twin_q = False

        # Build the encoder for the policy.
        self.pi_encoder = self.catalog.build_encoder(framework=self.framework)

        if not self.inference_only or self.framework != "torch":
            # SAC needs a separate Q network encoder (besides the pi network).
            # This is because the Q network also takes the action as input
            # (concatenated with the observations).
            self.qf_encoder = self.catalog.build_qf_encoder(framework=self.framework)

        # Build heads.
        self.pi = self.catalog.build_pi_head(framework=self.framework)

        if not self.inference_only or self.framework != "torch":
            self.qf = self.catalog.build_qf_head(framework=self.framework)

    @override(TargetNetworkAPI)
    def make_target_networks(self):
        self.target_qf_encoder = make_target_network(self.qf_encoder)
        self.target_qf = make_target_network(self.qf)

    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        ret = ["qf", "target_qf", "qf_encoder", "target_qf_encoder"]
        return ret

    @override(TargetNetworkAPI)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """Returns target Q and Q network(s) to update the target network(s)."""
        return [
            (self.qf_encoder, self.target_qf_encoder),
            (self.qf, self.target_qf),
        ]

    # TODO (simon): SAC does not support RNNs, yet.
    @override(RLModule)
    def get_initial_state(self) -> dict:
        # if hasattr(self.pi_encoder, "get_initial_state"):
        #     return {
        #         ACTOR: self.pi_encoder.get_initial_state(),
        #         CRITIC: self.qf_encoder.get_initial_state(),
        #         CRITIC_TARGET: self.qf_target_encoder.get_initial_state(),
        #     }
        # else:
        #     return {}
        return {}

    @override(RLModule)
    def input_specs_train(self) -> SpecType:
        return [
            SampleBatch.OBS,
            SampleBatch.ACTIONS,
            SampleBatch.NEXT_OBS,
        ]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return [
            QF_PREDS,
            SampleBatch.ACTION_DIST_INPUTS,
            ACTION_DIST_INPUTS_NEXT,
        ]

    @abstractmethod
    def _qf_forward_train_helper(
        self, batch: Dict[str, Any], encoder: Encoder, head: Model
    ) -> Dict[str, Any]:
        """Executes the forward pass for Q networks.

        Args:
            batch: Dict containing a concatenated tensor with observations
                and actions under the key `SampleBatch.OBS`.
            encoder: An `Encoder` model for the Q state-action encoder.
            head: A `Model` for the Q head.

        Returns:
            The estimated Q-value using the `encoder` and `head` networks.
        """


class SACNTorchRLModule(TorchRLModule, SACNRLModule):
    framework: str = "torch"

    @override(RLModule)
    def _forward_inference(self, batch: Dict) -> Dict[str, Any]:
        output = {}

        # Pi encoder forward pass.
        pi_encoder_outs = self.pi_encoder(batch)

        # Pi head.
        output[Columns.ACTION_DIST_INPUTS] = self.pi(pi_encoder_outs[ENCODER_OUT])

        return output

    @override(RLModule)
    def _forward_exploration(self, batch: Dict, **kwargs) -> Dict[str, Any]:
        return self._forward_inference(batch)

    @override(RLModule)
    def _forward_train(self, batch: Dict) -> Dict[str, Any]:

        if conf.num_gpus > 0 and conf.remote_env.run_mode == "offline":
            batch[Columns.OBS] = batch[Columns.OBS].cuda()
            batch[Columns.ACTIONS] = batch[Columns.ACTIONS].cuda()
            batch[Columns.NEXT_OBS] = batch[Columns.NEXT_OBS].cuda()
            batch[Columns.REWARDS] = batch[Columns.REWARDS].cuda()
            batch[Columns.TERMINATEDS] = batch[Columns.TERMINATEDS].cuda()
            batch[Columns.TRUNCATEDS] = batch[Columns.TRUNCATEDS].cuda()

        if self.inference_only:
            raise RuntimeError(
                "Trying to train a module that is not a learner module. Set the "
                "flag `inference_only=False` when building the module."
            )
        output = {}

        # SAC needs also Q function values and action logits for next observations.
        batch_curr = {Columns.OBS: batch[Columns.OBS]}
        batch_next = {Columns.OBS: batch[Columns.NEXT_OBS]}

        # Encoder forward passes.
        pi_encoder_outs = self.pi_encoder(batch_curr)

        # Also encode the next observations (and next actions for the Q net).
        pi_encoder_next_outs = self.pi_encoder(batch_next)

        # Q-network(s) forward passes.
        batch_curr.update({Columns.ACTIONS: batch[Columns.ACTIONS]})
        output[QF_PREDS] = self._qf_forward_train_helper(
            batch_curr, self.qf_encoder, self.qf
        )  # [ensemble_size, batch_size]

        # Policy head.
        action_logits = self.pi(pi_encoder_outs[ENCODER_OUT])
        # Also get the action logits for the next observations.
        action_logits_next = self.pi(pi_encoder_next_outs[ENCODER_OUT])
        output[Columns.ACTION_DIST_INPUTS] = action_logits
        output[ACTION_DIST_INPUTS_NEXT] = action_logits_next

        # Get the train action distribution for the current policy and current state.
        # This is needed for the policy (actor) loss in SAC.
        action_dist_class = self.get_train_action_dist_cls()
        action_dist_curr = action_dist_class.from_logits(action_logits)
        # Get the train action distribution for the current policy and next state.
        # For the Q (critic) loss in SAC, we need to sample from the current policy at
        # the next state.
        try:
            action_dist_next = action_dist_class.from_logits(action_logits_next)
        except ValueError as e:
            # Print action logits next completely to help debugging.
            # Set print mode
            torch.set_printoptions(profile="full")
            print("Action logits next:", action_logits_next, sep="\n")
            print("Obs:", batch[Columns.OBS], sep="\n")
            print("Next obs:", batch[Columns.NEXT_OBS], sep="\n")
            print("Action:", batch[Columns.ACTIONS], sep="\n")
            print("Reward:", batch[Columns.REWARDS], sep="\n")
            sleep(5)
            # Reset print mode
            torch.set_printoptions(profile="default")
            raise e

        # Sample actions for the current state. Note that we need to apply the
        # reparameterization trick (`rsample()` instead of `sample()`) to avoid the
        # expectation over actions.
        actions_resampled = action_dist_curr.rsample()
        # Compute the log probabilities for the current state (for the critic loss).
        output["logp_resampled"] = action_dist_curr.logp(actions_resampled)

        # Sample actions for the next state.
        actions_next_resampled = action_dist_next.sample().detach()
        # Compute the log probabilities for the next state.
        output["logp_next_resampled"] = (
            action_dist_next.logp(actions_next_resampled)
        ).detach()

        # Compute Q-values for the current policy in the current state with
        # the sampled actions.
        q_batch_curr = {
            Columns.OBS: batch[Columns.OBS],
            Columns.ACTIONS: actions_resampled,
        }
        # Make sure we perform a "straight-through gradient" pass here,
        # ignoring the gradients of the q-net, however, still recording
        # the gradients of the policy net (which was used to rsample the actions used
        # here). This is different from doing `.detach()` or `with torch.no_grads()`,
        # as these two methds would fully block all gradient recordings, including
        # the needed policy ones.
        all_params = list(self.qf.parameters()) + list(self.qf_encoder.parameters())

        for param in all_params:
            param.requires_grad = False
        output["q_curr_ens"] = self.compute_q_values(q_batch_curr)  # ens for ensemble
        output["q_curr"] = output["q_curr_ens"].min(dim=0)[0]
        for param in all_params:
            param.requires_grad = True

        # Compute Q-values from the target Q network for the next state with the
        # sampled actions for the next state.
        q_batch_next = {
            Columns.OBS: batch[Columns.NEXT_OBS],
            Columns.ACTIONS: actions_next_resampled,
        }
        output["q_target_next_ens"] = self.forward_target(q_batch_next)
        output["q_target_next"] = output["q_target_next_ens"].min(dim=0)[0].detach()

        # Return the network outputs.
        return output

    @override(TargetNetworkAPI)
    def forward_target(self, batch: Dict[str, Any]) -> Tensor:
        target_qvs = self._qf_forward_train_helper(
            batch, self.target_qf_encoder, self.target_qf
        )
        return target_qvs

    # TODO (sven): Create `ValueFunctionAPI` and subclass from this.
    def compute_q_values(self, batch: Dict[str, Any]) -> Tensor:
        qvs = self._qf_forward_train_helper(batch, self.qf_encoder, self.qf)
        return qvs

    @override(SACNRLModule)
    def _qf_forward_train_helper(
        self, batch: Dict[str, Any], encoder: Encoder, head: Model
    ) -> Tensor:
        """Executes the forward pass for Q networks.

        Args:
            batch: Dict containing a concatenated tensor with observations
                and actions under the key `Columns.OBS`.
            encoder: An `Encoder` model for the Q state-action encoder.
            head: A `Model` for the Q head.

        Returns:
            The estimated (single) Q-value.
        """
        # Construct batch. Note, we need to feed observations and actions.
        obs = batch[Columns.OBS]
        actions = batch[Columns.ACTIONS]
        qf_batch = {Columns.OBS: torch.concat((obs, actions), dim=-1)}
        # Encoder forward pass. Output shape: [ensemble_size, batch_size, encoder_output_dim]
        qf_encoder_outs = encoder(qf_batch)

        # Q head forward pass. Output shape: [ensemble_size, batch_size]
        return head(qf_encoder_outs[ENCODER_OUT])

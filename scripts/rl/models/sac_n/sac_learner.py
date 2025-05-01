from typing import Dict, Any

import torch
from ray.rllib.algorithms import SACConfig
from ray.rllib.algorithms.sac.torch.sac_torch_learner import SACTorchLearner
from ray.rllib.core import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY
from ray.rllib.utils.metrics import ALL_MODULES, TD_ERROR_KEY
from ray.rllib.utils.typing import ModuleID, TensorType
from ray.rllib.algorithms.sac.sac_learner import (
    LOGPS_KEY,
    QF_LOSS_KEY,
    QF_MEAN_KEY,
    QF_MAX_KEY,
    QF_MIN_KEY,
    QF_PREDS,
    QF_TWIN_LOSS_KEY,
    QF_TWIN_PREDS,
    TD_ERROR_MEAN_KEY,
    SACLearner,
)


class SACNTorchLearner(SACTorchLearner):

    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: SACConfig,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType]
    ) -> TensorType:
        # Receive the current alpha hyperparameter.
        alpha = torch.exp(self.curr_log_alpha[module_id])

        # Get Q-values for the actually selected actions during rollout.
        # In the critic loss we use these as predictions.
        q_selected = fwd_out[QF_PREDS]  # [ensemble_size, batch_size]

        # Compute value function for next state (see eq. (3) in Haarnoja et al. (2018)).
        # Note, we use here the sampled actions in the log probabilities.
        q_target_next = (
            fwd_out["q_target_next"] - alpha.detach() * fwd_out["logp_next_resampled"]
        )
        # Now mask all Q-values with terminated next states in the targets.
        q_next_masked = (1.0 - batch[Columns.TERMINATEDS].float()) * q_target_next

        # Compute the right hand side of the Bellman equation.
        # Detach this node from the computation graph as we do not want to
        # backpropagate through the target network when optimizing the Q loss.
        q_selected_target = (
            (batch[Columns.REWARDS] + (config.gamma ** batch["n_step"]) * q_next_masked)
            .broadcast_to(q_selected.shape)
            .detach()
        )

        # Calculate the TD-error. Note, this is needed for the priority weights in
        # the replay buffer.
        td_error = torch.abs(q_selected - q_selected_target)

        # print("Q selected:", q_selected.mean(dim=1))

        critic_losses = torch.mean(
            batch["weights"]
            * torch.nn.HuberLoss(reduction="none", delta=1.0)(
                q_selected, q_selected_target
            ),
            dim=1,
        )

        # For the actor (policy) loss we need sampled actions from the current policy
        # evaluated at the current observations.
        # Note that the `q_curr` tensor below has the q-net's gradients ignored, while
        # having the policy's gradients registered. The policy net was used to rsample
        # actions used to compute `q_curr` (by passing these actions through the q-net).
        # Hence, we can't do `fwd_out[q_curr].detach()`!
        # Note further, we minimize here, while the original equation in Haarnoja et
        # al. (2018) considers maximization.
        # TODO (simon): Rename to `resampled` to `current`.
        actor_loss = torch.mean(
            alpha.detach() * fwd_out["logp_resampled"] - fwd_out["q_curr"]
        )

        # Optimize also the hyperparameter alpha by using the current policy
        # evaluated at the current state (sampled values).
        # TODO (simon): Check, why log(alpha) is used, prob. just better
        # to optimize and monotonic function. Original equation uses alpha.
        alpha_loss = -torch.mean(
            self.curr_log_alpha[module_id]
            * (fwd_out["logp_resampled"].detach() + self.target_entropy[module_id])
        )

        total_loss = actor_loss + torch.sum(critic_losses) + alpha_loss

        # Log the TD-error with reduce=None, such that - in case we have n parallel
        # Learners - we will re-concatenate the produced TD-error tensors to yield
        # a 1:1 representation of the original batch.
        self.metrics.log_value(
            key=(module_id, TD_ERROR_KEY),
            value=td_error.mean(dim=0),
            reduce=None,
            clear_on_reduce=True,
        )
        # Log other important loss stats (reduce=mean (default), but with window=1
        # in order to keep them history free).
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: actor_loss,
                QF_LOSS_KEY: torch.sum(critic_losses),
                QF_LOSS_KEY + "_1": critic_losses[0],
                QF_LOSS_KEY + "_2": critic_losses[1],
                "alpha_loss": alpha_loss,
                "alpha_value": alpha,
                "log_alpha_value": torch.log(alpha),
                "target_entropy": self.target_entropy[module_id],
                LOGPS_KEY: torch.mean(fwd_out["logp_resampled"]),
                QF_MEAN_KEY: torch.mean(fwd_out["q_curr"]),
                QF_MAX_KEY: torch.max(fwd_out["q_curr"]),
                QF_MIN_KEY: torch.min(fwd_out["q_curr"]),
                TD_ERROR_MEAN_KEY: torch.mean(td_error, dim=1),
                TD_ERROR_MEAN_KEY + "_1": torch.mean(td_error, dim=1)[0],
                TD_ERROR_MEAN_KEY + "_2": torch.mean(td_error, dim=1)[1],
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )

        return total_loss

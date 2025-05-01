# https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/classes/random_rlm.py
import tree
from ray.rllib import SampleBatch
from ray.rllib.algorithms import Algorithm
from ray.rllib.core.rl_module import RLModule
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils import override
from ray.rllib.utils.spaces.space_utils import batch as batch_func


class RandomRLModule(RLModule):

    framework = "torch"

    @override(RLModule)
    def _forward(self, batch, **kwargs):
        obs_batch_size = len(tree.flatten(batch[SampleBatch.OBS])[0])
        actions = batch_func(
            [self.action_space.sample() for _ in range(obs_batch_size)]
        )
        return {SampleBatch.ACTIONS: actions}

    @override(RLModule)
    def _forward_train(self, *args, **kwargs):
        # RandomRLModule should always be configured as non-trainable.
        # To do so, set in your config:
        # `config.multi_agent(policies_to_train=[list of ModuleIDs to be trained,
        # NOT including the ModuleID of this RLModule])`
        raise NotImplementedError("Random RLModule: Should not be trained!")

    @override(RLModule)
    def output_specs_inference(self):
        return [SampleBatch.ACTIONS]

    @override(RLModule)
    def output_specs_exploration(self):
        return [SampleBatch.ACTIONS]

    def compile(self, *args, **kwargs):
        """Dummy method for compatibility with TorchRLModule.

        This is hit when RolloutWorker tries to compile TorchRLModule."""
        pass

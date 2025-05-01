import os

__all__ = [
    "RtEnvConfig",
    "RemoteEnvConfig",
    "Config",
]

hostname = os.uname()[1]

if hostname == "localhost":
    from scripts.configuration_pool.localhost import (
        RtEnvConfig,
        RemoteEnvConfig,
        Config,
    )
else:
    raise ValueError(f"Unknown hostname: {hostname}")

"""Client for the Data Pipeline Debugging Environment."""

try:
    from openenv.core.env_client import EnvClient, StepResult
except ImportError:
    # Fallback: minimal client stub
    class StepResult:
        def __init__(self, observation, reward=0.0, done=False):
            self.observation = observation
            self.reward = reward
            self.done = done

    class EnvClient:
        def __init__(self, base_url: str = "", **kwargs):
            self.base_url = base_url

from .models import PipelineAction, PipelineObservation, PipelineState


class DataPipelineEnv(EnvClient):
    """Typed client for the Data Pipeline Debugging Environment.

    Usage:
        async with DataPipelineEnv(base_url="ws://localhost:8000") as env:
            result = await env.reset()
            result = await env.step(PipelineAction(command="inspect_schema", target="orders"))
    """

    def _parse_observation(self, data: dict) -> PipelineObservation:
        return PipelineObservation(**data)

    def _parse_state(self, data: dict) -> PipelineState:
        return PipelineState(**data)

"""FastAPI application for the Data Pipeline Debugging Environment."""

import os
import sys

# Ensure parent directory is importable (for Docker where WORKDIR=/app)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from openenv.core.env_server import create_app
    from models import PipelineAction, PipelineObservation
    from server.pipeline_environment import PipelineEnvironment
    _HAS_OPENENV = True
except ImportError:
    _HAS_OPENENV = False
    try:
        from models import PipelineAction, PipelineObservation
        from server.pipeline_environment import PipelineEnvironment
    except ImportError:
        from ..models import PipelineAction, PipelineObservation
        from .pipeline_environment import PipelineEnvironment


DEFAULT_TASK = os.getenv("PIPELINE_TASK", "type_mismatch_fix")


def _to_dict(obj):
    """Convert model to dict regardless of whether it's Pydantic or dataclass."""
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    elif hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    return {}


if _HAS_OPENENV:
    app = create_app(
        lambda: PipelineEnvironment(task_name=DEFAULT_TASK),
        PipelineAction,
        PipelineObservation,
        env_name="data_pipeline_env",
    )
else:
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    app = FastAPI(title="Data Pipeline Debugging Environment")
    _envs = {}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/reset")
    async def reset(body: dict = {}):
        task_name = body.get("task_name", DEFAULT_TASK)
        env = PipelineEnvironment(task_name=task_name)
        obs = env.reset(task_name=task_name)
        session_id = env.state.episode_id
        _envs[session_id] = env
        return {
            "observation": _to_dict(obs),
            "session_id": session_id,
        }

    @app.post("/step")
    async def step(body: dict = {}):
        session_id = body.get("session_id", "")
        env = _envs.get(session_id)
        if env is None:
            return JSONResponse(status_code=400, content={"error": "Invalid session_id. Call /reset first."})
        action = PipelineAction(
            command=body.get("command", ""),
            target=body.get("target", ""),
            parameters=body.get("parameters", {}),
        )
        obs = env.step(action)
        return {
            "observation": _to_dict(obs),
            "reward": obs.reward,
            "done": obs.done,
        }

    @app.get("/state")
    async def get_state(session_id: str = ""):
        env = _envs.get(session_id)
        if env is None:
            return JSONResponse(status_code=400, content={"error": "Invalid session_id."})
        return _to_dict(env.state)

    @app.get("/schema")
    async def get_schema():
        return {
            "action": PipelineAction.model_json_schema() if hasattr(PipelineAction, "model_json_schema") else {},
            "observation": PipelineObservation.model_json_schema() if hasattr(PipelineObservation, "model_json_schema") else {},
        }

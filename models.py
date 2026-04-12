"""
Typed models for the Data Pipeline Debugging Environment.

Action: Agent submits a command to inspect or fix the pipeline.
Observation: The environment returns pipeline state and feedback.
State: Episode metadata including step count, task info, and score.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Detect which base classes to use
_USE_PYDANTIC = False
_USE_OPENENV = False

try:
    from openenv.core.env_server.types import Action, Observation, State
    _USE_OPENENV = True
except ImportError:
    try:
        from pydantic import BaseModel
        _USE_PYDANTIC = True
    except ImportError:
        pass


if _USE_OPENENV:
    class PipelineAction(Action):
        command: str = ""
        target: str = ""
        parameters: Dict[str, Any] = {}

    class PipelineObservation(Observation):
        output: str = ""
        pipeline_status: str = "broken"
        bugs_found: int = 0
        bugs_fixed: int = 0
        total_bugs: int = 0
        available_tables: List[str] = []
        hint: str = ""
        error_message: str = ""

    class PipelineState(State):
        task_name: str = ""
        difficulty: str = "easy"
        bugs_remaining: int = 0
        total_bugs: int = 0
        commands_used: List[str] = []
        score: float = 0.0
        max_steps: int = 15

elif _USE_PYDANTIC:
    class PipelineAction(BaseModel):
        metadata: Dict[str, Any] = {}
        command: str = ""
        target: str = ""
        parameters: Dict[str, Any] = {}

    class PipelineObservation(BaseModel):
        done: bool = False
        reward: Optional[float] = None
        metadata: Dict[str, Any] = {}
        output: str = ""
        pipeline_status: str = "broken"
        bugs_found: int = 0
        bugs_fixed: int = 0
        total_bugs: int = 0
        available_tables: List[str] = []
        hint: str = ""
        error_message: str = ""

    class PipelineState(BaseModel):
        episode_id: Optional[str] = None
        step_count: int = 0
        task_name: str = ""
        difficulty: str = "easy"
        bugs_remaining: int = 0
        total_bugs: int = 0
        commands_used: List[str] = []
        score: float = 0.0
        max_steps: int = 15

else:
    @dataclass
    class PipelineAction:
        command: str = ""
        target: str = ""
        parameters: Dict[str, Any] = field(default_factory=dict)
        metadata: Dict[str, Any] = field(default_factory=dict)

    @dataclass
    class PipelineObservation:
        done: bool = False
        reward: Optional[float] = None
        output: str = ""
        pipeline_status: str = "broken"
        bugs_found: int = 0
        bugs_fixed: int = 0
        total_bugs: int = 0
        available_tables: List[str] = field(default_factory=list)
        hint: str = ""
        error_message: str = ""
        metadata: Dict[str, Any] = field(default_factory=dict)

    @dataclass
    class PipelineState:
        episode_id: Optional[str] = None
        step_count: int = 0
        task_name: str = ""
        difficulty: str = "easy"
        bugs_remaining: int = 0
        total_bugs: int = 0
        commands_used: List[str] = field(default_factory=list)
        score: float = 0.0
        max_steps: int = 15

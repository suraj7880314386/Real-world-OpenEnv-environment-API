"""
Inference Script — Data Pipeline Debugging Environment
=======================================================
MANDATORY VARIABLES:
    API_BASE_URL   The API endpoint for the LLM.
    MODEL_NAME     The model identifier to use for inference.
    HF_TOKEN       Your Hugging Face / API key.
    LOCAL_IMAGE_NAME  Docker image name (optional, if using from_docker_image)

STDOUT FORMAT:
    [START] task=<task_name> env=<benchmark> model=<model_name>
    [STEP]  step=<n> action=<action_str> reward=<0.00> done=<true|false> error=<msg|null>
    [END]   success=<true|false> steps=<n> score=<score> rewards=<r1,r2,...,rn>
"""

import asyncio
import os
import textwrap
import json
from typing import List, Optional

from openai import OpenAI

# ─── Environment Imports ───
# Try OpenEnv client first, fall back to HTTP for standalone
try:
    from data_pipeline_env import DataPipelineEnv, PipelineAction
    USE_OPENENV_CLIENT = True
except ImportError:
    USE_OPENENV_CLIENT = False

# ─── Configuration ───
# Defaults are set only for API_BASE_URL and MODEL_NAME (not HF_TOKEN)
API_BASE_URL = os.getenv("API_BASE_URL", "<your-active-endpoint>")
MODEL_NAME = os.getenv("MODEL_NAME", "<your-active-model>")
HF_TOKEN = os.getenv("HF_TOKEN")

# Optional — if you use from_docker_image():
LOCAL_IMAGE_NAME = os.getenv("LOCAL_IMAGE_NAME")
BENCHMARK = os.getenv("PIPELINE_BENCHMARK", "data_pipeline_env")
ENV_BASE_URL = os.getenv("ENV_BASE_URL", "http://localhost:8000")

# Task to run — can be: type_mismatch_fix, schema_drift_repair, etl_pipeline_overhaul
TASK_NAME = os.getenv("PIPELINE_TASK", "type_mismatch_fix")

# Limits
MAX_STEPS = int(os.getenv("MAX_STEPS", "15"))
TEMPERATURE = 0.5
MAX_TOKENS = 500

SYSTEM_PROMPT = textwrap.dedent("""
    You are a data pipeline debugging agent. You are investigating a broken data pipeline
    and must find and fix all bugs.

    Available commands (respond with EXACTLY this JSON format):
    {
        "command": "<command_name>",
        "target": "<argument>",
        "parameters": {}
    }

    Commands:
    - inspect_schema <table_name>: View column names and types for a table
    - inspect_data <table_name>: View sample rows from a table
    - run_query <sql_query>: Run a simple SQL query (SELECT/COUNT)
    - diagnose <description>: Describe a bug you found (mention table, column, and issue)
    - fix <description>: Submit a fix (describe what to change, e.g. "cast price column to float and handle N/A values with coalesce to 0")
    - validate: Check if all bugs are fixed

    Strategy:
    1. First inspect_schema for each available table to understand the structure
    2. Then inspect_data to see actual values and spot anomalies
    3. Diagnose issues you find
    4. Apply fixes with detailed descriptions mentioning the column name, table, and action
    5. Validate to check progress

    Be thorough in your fix descriptions — mention the column name, what's wrong, and how to fix it.
    Include multiple keywords: the column name, the action (cast/rename/remove/filter), and the target type or value.
""").strip()


# ─── Logging Functions ───

def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    error_val = error if error else "null"
    done_val = str(done).lower()
    # Sanitize action string — no newlines
    action_clean = action.replace("\n", " ").replace("\r", "")[:200]
    print(
        f"[STEP] step={step} action={action_clean} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(f"[END] success={str(success).lower()} steps={steps} score={score:.2f} rewards={rewards_str}", flush=True)


# ─── LLM Interaction ───

def build_user_prompt(
    step: int,
    last_output: str,
    last_reward: float,
    pipeline_status: str,
    bugs_fixed: int,
    total_bugs: int,
    available_tables: List[str],
    hint: str,
    history: List[str],
) -> str:
    history_block = "\n".join(history[-6:]) if history else "None"
    return textwrap.dedent(f"""
        Step: {step}/{MAX_STEPS}
        Pipeline status: {pipeline_status}
        Bugs fixed: {bugs_fixed}/{total_bugs}
        Available tables: {available_tables}
        Last output: {last_output[:1000]}
        Last reward: {last_reward:.2f}
        Hint: {hint}

        Recent history:
        {history_block}

        What is your next command? Respond with JSON only.
    """).strip()


def parse_llm_response(text: str) -> dict:
    """Parse LLM response to extract command JSON."""
    text = text.strip()

    # Try direct JSON parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try extracting JSON from markdown code block
    import re
    json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try finding any JSON object in the text
    json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    # Fallback: try to interpret as a simple command
    parts = text.split(maxsplit=1)
    if parts:
        return {"command": parts[0].lower(), "target": parts[1] if len(parts) > 1 else "", "parameters": {}}

    return {"command": "validate", "target": "", "parameters": {}}


def get_model_action(client: OpenAI, user_prompt: str) -> dict:
    """Get next action from the LLM."""
    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
            stream=False,
        )
        text = (completion.choices[0].message.content or "").strip()
        return parse_llm_response(text)
    except Exception as exc:
        print(f"[DEBUG] Model request failed: {exc}", flush=True)
        return {"command": "validate", "target": "", "parameters": {}}


# ─── HTTP Fallback Client ───

class HTTPPipelineClient:
    """Simple HTTP client for when openenv-core client isn't available."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session_id = None
        import requests
        self._requests = requests

    def reset(self, task_name: str = "type_mismatch_fix"):
        resp = self._requests.post(
            f"{self.base_url}/reset",
            json={"task_name": task_name},
            timeout=30,
        )
        data = resp.json()
        self.session_id = data.get("session_id", "")
        return data.get("observation", data)

    def step(self, command: str, target: str = "", parameters: dict = None):
        resp = self._requests.post(
            f"{self.base_url}/step",
            json={
                "session_id": self.session_id,
                "command": command,
                "target": target,
                "parameters": parameters or {},
            },
            timeout=30,
        )
        data = resp.json()
        return data.get("observation", data)

    def close(self):
        pass


# ─── Main Loop ───

async def run_with_openenv_client():
    """Run using the typed OpenEnv client (preferred)."""
    llm_client = OpenAI(base_url=API_BASE_URL, api_key=HF_TOKEN)

    env = await DataPipelineEnv.from_docker_image(LOCAL_IMAGE_NAME)

    history: List[str] = []
    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False

    log_start(task=TASK_NAME, env=BENCHMARK, model=MODEL_NAME)

    try:
        result = await env.reset(task_name=TASK_NAME)
        obs = result.observation

        last_output = obs.output
        last_reward = 0.0

        for step in range(1, MAX_STEPS + 1):
            if result.done:
                break

            user_prompt = build_user_prompt(
                step=step,
                last_output=last_output,
                last_reward=last_reward,
                pipeline_status=obs.pipeline_status,
                bugs_fixed=obs.bugs_fixed,
                total_bugs=obs.total_bugs,
                available_tables=obs.available_tables,
                hint=obs.hint,
                history=history,
            )

            action_dict = get_model_action(llm_client, user_prompt)
            action = PipelineAction(
                command=action_dict.get("command", "validate"),
                target=action_dict.get("target", ""),
                parameters=action_dict.get("parameters", {}),
            )

            result = await env.step(action)
            obs = result.observation
            reward = result.reward or 0.0
            done = result.done
            error = obs.error_message if obs.error_message else None

            rewards.append(reward)
            steps_taken = step
            last_output = obs.output
            last_reward = reward

            action_str = f"{action.command}({action.target})"
            log_step(step=step, action=action_str, reward=reward, done=done, error=error)
            history.append(f"Step {step}: {action_str} -> reward {reward:+.2f} | {obs.pipeline_status}")

            if done:
                break

        # Score = fraction of bugs fixed (already in [0, 1])
        score = obs.bugs_fixed / obs.total_bugs if obs.total_bugs > 0 else 0.0
        score = min(max(score, 0.0), 1.0)
        success = score >= 0.5  # At least half the bugs fixed

    finally:
        try:
            await env.close()
        except Exception as e:
            print(f"[DEBUG] env.close() error: {e}", flush=True)
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)


def run_with_http_client():
    """Run using simple HTTP client (standalone mode)."""
    llm_client = OpenAI(base_url=API_BASE_URL, api_key=HF_TOKEN)
    env = HTTPPipelineClient(ENV_BASE_URL)

    history: List[str] = []
    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False

    log_start(task=TASK_NAME, env=BENCHMARK, model=MODEL_NAME)

    try:
        obs_data = env.reset(task_name=TASK_NAME)

        last_output = obs_data.get("output", "")
        last_reward = 0.0
        done = obs_data.get("done", False)
        pipeline_status = obs_data.get("pipeline_status", "broken")
        bugs_fixed = obs_data.get("bugs_fixed", 0)
        total_bugs = obs_data.get("total_bugs", 0)
        available_tables = obs_data.get("available_tables", [])
        hint = obs_data.get("hint", "")

        for step in range(1, MAX_STEPS + 1):
            if done:
                break

            user_prompt = build_user_prompt(
                step=step,
                last_output=last_output,
                last_reward=last_reward,
                pipeline_status=pipeline_status,
                bugs_fixed=bugs_fixed,
                total_bugs=total_bugs,
                available_tables=available_tables,
                hint=hint,
                history=history,
            )

            action_dict = get_model_action(llm_client, user_prompt)
            command = action_dict.get("command", "validate")
            target = action_dict.get("target", "")
            parameters = action_dict.get("parameters", {})

            obs_data = env.step(command=command, target=target, parameters=parameters)

            reward = obs_data.get("reward", 0.0) or 0.0
            done = obs_data.get("done", False)
            error = obs_data.get("error_message", "") or None

            last_output = obs_data.get("output", "")
            last_reward = reward
            pipeline_status = obs_data.get("pipeline_status", "broken")
            bugs_fixed = obs_data.get("bugs_fixed", 0)
            total_bugs = obs_data.get("total_bugs", 0)
            available_tables = obs_data.get("available_tables", [])
            hint = obs_data.get("hint", "")

            rewards.append(reward)
            steps_taken = step

            action_str = f"{command}({target[:100]})"
            log_step(step=step, action=action_str, reward=reward, done=done, error=error)
            history.append(f"Step {step}: {action_str} -> reward {reward:+.2f} | {pipeline_status}")

            if done:
                break

        score = bugs_fixed / total_bugs if total_bugs > 0 else 0.0
        score = min(max(score, 0.0), 1.0)
        success = score >= 0.5

    finally:
        try:
            env.close()
        except Exception:
            pass
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)


async def main():
    """Entry point — selects client mode based on available dependencies."""
    if USE_OPENENV_CLIENT and LOCAL_IMAGE_NAME:
        await run_with_openenv_client()
    else:
        run_with_http_client()


if __name__ == "__main__":
    asyncio.run(main())

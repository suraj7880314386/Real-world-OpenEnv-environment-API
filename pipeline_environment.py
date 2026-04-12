"""
Data Pipeline Debugging Environment.

Simulates real-world data pipeline issues that an AI agent must diagnose and fix.
Three tasks with increasing difficulty:
  - easy:   Single table with a type mismatch bug
  - medium: Two-table join with schema drift and null-handling bugs
  - hard:   Multi-table ETL pipeline with aggregation errors, missing joins, and data quality issues
"""

import copy
import json
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

try:
    from openenv.core.env_server.interfaces import Environment
except ImportError:
    class Environment:
        """Fallback base class for standalone usage."""
        def __init__(self, *args, **kwargs):
            pass
        def reset(self):
            raise NotImplementedError
        def step(self, action):
            raise NotImplementedError
        @property
        def state(self):
            raise NotImplementedError

try:
    from ..models import PipelineAction, PipelineObservation, PipelineState
except ImportError:
    from models import PipelineAction, PipelineObservation, PipelineState


# ─────────────────────────── Task Definitions ───────────────────────────

def _make_easy_task() -> Dict[str, Any]:
    """Single table: 'orders' has a price column stored as strings instead of floats."""
    return {
        "task_name": "type_mismatch_fix",
        "difficulty": "easy",
        "description": "The 'orders' table has a data type issue causing aggregation failures.",
        "tables": {
            "orders": {
                "schema": {
                    "order_id": "INTEGER",
                    "customer_name": "VARCHAR",
                    "product": "VARCHAR",
                    "price": "VARCHAR",  # BUG: should be FLOAT
                    "quantity": "INTEGER",
                    "order_date": "DATE",
                },
                "data": [
                    {"order_id": 1, "customer_name": "Alice", "product": "Widget A", "price": "29.99", "quantity": 2, "order_date": "2025-01-15"},
                    {"order_id": 2, "customer_name": "Bob", "product": "Widget B", "price": "49.50", "quantity": 1, "order_date": "2025-01-16"},
                    {"order_id": 3, "customer_name": "Charlie", "product": "Widget A", "price": "29.99", "quantity": 3, "order_date": "2025-01-17"},
                    {"order_id": 4, "customer_name": "Diana", "product": "Widget C", "price": "N/A", "quantity": 1, "order_date": "2025-01-18"},
                    {"order_id": 5, "customer_name": "Eve", "product": "Widget B", "price": "49.50", "quantity": 2, "order_date": "2025-01-19"},
                ],
                "expected_pipeline_output": "Total revenue should be calculable via SUM(price * quantity)",
            }
        },
        "bugs": [
            {
                "id": "easy_bug_1",
                "description": "price column is VARCHAR instead of FLOAT, blocking numeric aggregation",
                "table": "orders",
                "column": "price",
                "fix_type": "cast_column",
                "fix_keywords": ["cast", "float", "numeric", "convert", "price", "type"],
                "fixed": False,
            },
            {
                "id": "easy_bug_2",
                "description": "Row 4 has 'N/A' in price column which must be handled (coalesce to 0 or filter)",
                "table": "orders",
                "column": "price",
                "fix_type": "handle_null",
                "fix_keywords": ["null", "n/a", "coalesce", "filter", "default", "0", "remove", "clean"],
                "fixed": False,
            },
        ],
        "max_steps": 10,
    }


def _make_medium_task() -> Dict[str, Any]:
    """Two tables: 'users' and 'events' with schema drift and null handling bugs."""
    return {
        "task_name": "schema_drift_repair",
        "difficulty": "medium",
        "description": "A user analytics pipeline joining 'users' and 'events' is producing incorrect results due to schema changes and data quality issues.",
        "tables": {
            "users": {
                "schema": {
                    "user_id": "INTEGER",
                    "name": "VARCHAR",
                    "email": "VARCHAR",
                    "signup_date": "DATE",
                    "country": "VARCHAR",
                },
                "data": [
                    {"user_id": 1, "name": "Alice", "email": "alice@example.com", "signup_date": "2024-06-01", "country": "US"},
                    {"user_id": 2, "name": "Bob", "email": "bob@example.com", "signup_date": "2024-07-15", "country": "UK"},
                    {"user_id": 3, "name": "Charlie", "email": "charlie@example.com", "signup_date": "2024-08-20", "country": "US"},
                    {"user_id": 4, "name": "Diana", "email": None, "signup_date": "2024-09-10", "country": "DE"},
                ],
            },
            "events": {
                "schema": {
                    "event_id": "INTEGER",
                    "uid": "INTEGER",  # BUG: renamed from user_id, breaking join
                    "event_type": "VARCHAR",
                    "timestamp": "VARCHAR",  # BUG: should be TIMESTAMP
                    "revenue": "FLOAT",
                },
                "data": [
                    {"event_id": 101, "uid": 1, "event_type": "purchase", "timestamp": "2025-01-10 14:30:00", "revenue": 99.99},
                    {"event_id": 102, "uid": 2, "event_type": "purchase", "timestamp": "2025-01-11 09:15:00", "revenue": 149.00},
                    {"event_id": 103, "uid": 1, "event_type": "refund", "timestamp": "2025-01-12 11:00:00", "revenue": -99.99},
                    {"event_id": 104, "uid": 5, "event_type": "purchase", "timestamp": "invalid-date", "revenue": 75.00},  # BUG: uid 5 doesn't exist + invalid timestamp
                    {"event_id": 105, "uid": 3, "event_type": "purchase", "timestamp": "2025-01-15 16:45:00", "revenue": 200.00},
                ],
            },
        },
        "bugs": [
            {
                "id": "med_bug_1",
                "description": "events.uid should be events.user_id — column was renamed causing broken join",
                "table": "events",
                "column": "uid",
                "fix_type": "rename_column",
                "fix_keywords": ["rename", "uid", "user_id", "alias", "column", "join"],
                "fixed": False,
            },
            {
                "id": "med_bug_2",
                "description": "events.timestamp is VARCHAR instead of TIMESTAMP type",
                "table": "events",
                "column": "timestamp",
                "fix_type": "cast_column",
                "fix_keywords": ["cast", "timestamp", "datetime", "convert", "type", "parse"],
                "fixed": False,
            },
            {
                "id": "med_bug_3",
                "description": "Event 104 references uid=5 which doesn't exist in users (orphan record) and has invalid timestamp",
                "table": "events",
                "column": "uid",
                "fix_type": "remove_orphan",
                "fix_keywords": ["orphan", "remove", "delete", "filter", "invalid", "uid=5", "foreign key", "referential", "clean"],
                "fixed": False,
            },
        ],
        "max_steps": 15,
    }


def _make_hard_task() -> Dict[str, Any]:
    """Multi-table ETL: products, inventory, sales, with aggregation, join, and data quality bugs."""
    return {
        "task_name": "etl_pipeline_overhaul",
        "difficulty": "hard",
        "description": "A retail analytics ETL pipeline across 'products', 'inventory', and 'sales' is producing wildly incorrect dashboard numbers. Multiple interacting bugs need to be found and fixed.",
        "tables": {
            "products": {
                "schema": {
                    "product_id": "INTEGER",
                    "name": "VARCHAR",
                    "category": "VARCHAR",
                    "base_price": "FLOAT",
                    "active": "BOOLEAN",
                },
                "data": [
                    {"product_id": 1, "name": "Laptop Pro", "category": "Electronics", "base_price": 1299.99, "active": True},
                    {"product_id": 2, "name": "Wireless Mouse", "category": "Electronics", "base_price": 29.99, "active": True},
                    {"product_id": 3, "name": "Standing Desk", "category": "Furniture", "base_price": 599.00, "active": False},  # inactive but still in sales
                    {"product_id": 4, "name": "Monitor 27in", "category": "Electronics", "base_price": 449.99, "active": True},
                    {"product_id": 5, "name": "Keyboard", "category": "Electronics", "base_price": 79.99, "active": True},
                ],
            },
            "inventory": {
                "schema": {
                    "product_id": "INTEGER",
                    "warehouse": "VARCHAR",
                    "stock_count": "INTEGER",
                    "last_updated": "DATE",
                },
                "data": [
                    {"product_id": 1, "warehouse": "WH-East", "stock_count": 50, "last_updated": "2025-01-20"},
                    {"product_id": 2, "warehouse": "WH-East", "stock_count": 200, "last_updated": "2025-01-20"},
                    {"product_id": 2, "warehouse": "WH-West", "stock_count": 150, "last_updated": "2025-01-20"},
                    {"product_id": 4, "warehouse": "WH-East", "stock_count": -10, "last_updated": "2025-01-20"},  # BUG: negative stock
                    {"product_id": 5, "warehouse": "WH-West", "stock_count": 75, "last_updated": "2025-01-20"},
                    {"product_id": 3, "warehouse": "WH-East", "stock_count": 0, "last_updated": "2024-06-01"},  # stale data for inactive product
                ],
            },
            "sales": {
                "schema": {
                    "sale_id": "INTEGER",
                    "prod_id": "INTEGER",  # BUG: should be product_id
                    "quantity_sold": "INTEGER",
                    "sale_price": "VARCHAR",  # BUG: should be FLOAT
                    "sale_date": "DATE",
                    "discount_pct": "FLOAT",
                },
                "data": [
                    {"sale_id": 1001, "prod_id": 1, "quantity_sold": 5, "sale_price": "1199.99", "sale_date": "2025-01-21", "discount_pct": 0.08},
                    {"sale_id": 1002, "prod_id": 2, "quantity_sold": 30, "sale_price": "24.99", "sale_date": "2025-01-21", "discount_pct": 0.17},
                    {"sale_id": 1003, "prod_id": 3, "quantity_sold": 2, "sale_price": "499.00", "sale_date": "2025-01-22", "discount_pct": 0.17},
                    {"sale_id": 1004, "prod_id": 4, "quantity_sold": 0, "sale_price": "449.99", "sale_date": "2025-01-22", "discount_pct": 0.0},  # BUG: zero quantity sale
                    {"sale_id": 1005, "prod_id": 6, "quantity_sold": 10, "sale_price": "9.99", "sale_date": "2025-01-23", "discount_pct": 0.0},  # BUG: prod_id 6 doesn't exist
                ],
            },
        },
        "bugs": [
            {
                "id": "hard_bug_1",
                "description": "sales.prod_id should be sales.product_id — column name mismatch breaks join with products table",
                "table": "sales",
                "column": "prod_id",
                "fix_type": "rename_column",
                "fix_keywords": ["rename", "prod_id", "product_id", "alias", "column", "join"],
                "fixed": False,
            },
            {
                "id": "hard_bug_2",
                "description": "sales.sale_price is VARCHAR instead of FLOAT, blocking revenue calculations",
                "table": "sales",
                "column": "sale_price",
                "fix_type": "cast_column",
                "fix_keywords": ["cast", "float", "numeric", "convert", "sale_price", "type"],
                "fixed": False,
            },
            {
                "id": "hard_bug_3",
                "description": "inventory has negative stock (-10) for product_id=4 which is physically impossible",
                "table": "inventory",
                "column": "stock_count",
                "fix_type": "fix_negative",
                "fix_keywords": ["negative", "stock", "zero", "abs", "clamp", "floor", "constraint", "fix", "-10"],
                "fixed": False,
            },
            {
                "id": "hard_bug_4",
                "description": "Sale 1004 has quantity_sold=0 which is a nonsensical record",
                "table": "sales",
                "column": "quantity_sold",
                "fix_type": "remove_invalid",
                "fix_keywords": ["zero", "quantity", "remove", "delete", "filter", "invalid", "nonsense"],
                "fixed": False,
            },
            {
                "id": "hard_bug_5",
                "description": "Sale 1005 references prod_id=6 which doesn't exist in products (orphan)",
                "table": "sales",
                "column": "prod_id",
                "fix_type": "remove_orphan",
                "fix_keywords": ["orphan", "remove", "delete", "filter", "prod_id=6", "foreign", "referential", "doesn't exist", "missing"],
                "fixed": False,
            },
        ],
        "max_steps": 20,
    }


TASK_REGISTRY = {
    "type_mismatch_fix": _make_easy_task,
    "schema_drift_repair": _make_medium_task,
    "etl_pipeline_overhaul": _make_hard_task,
}


# ─────────────────────────── Environment ───────────────────────────

class PipelineEnvironment(Environment):
    """Data Pipeline Debugging Environment."""

    CONCURRENCY_SAFE = True

    def __init__(self, task_name: str = "type_mismatch_fix"):
        super().__init__()
        self._default_task = task_name
        self._task: Dict[str, Any] = {}
        self._state = PipelineState()
        self._tables: Dict[str, Any] = {}
        self._bugs: List[Dict[str, Any]] = []

    def reset(self, task_name: str = None) -> PipelineObservation:
        task_key = task_name or self._default_task
        if task_key not in TASK_REGISTRY:
            task_key = "type_mismatch_fix"

        self._task = TASK_REGISTRY[task_key]()
        self._tables = copy.deepcopy(self._task["tables"])
        self._bugs = copy.deepcopy(self._task["bugs"])

        self._state = PipelineState(
            episode_id=str(uuid.uuid4()),
            step_count=0,
            task_name=self._task["task_name"],
            difficulty=self._task["difficulty"],
            bugs_remaining=len(self._bugs),
            total_bugs=len(self._bugs),
            commands_used=[],
            score=0.0,
            max_steps=self._task.get("max_steps", 15),
        )

        return PipelineObservation(
            done=False,
            reward=0.0,
            output=f"Pipeline debugging session started.\nTask: {self._task['description']}\nDifficulty: {self._task['difficulty']}\nAvailable tables: {list(self._tables.keys())}",
            pipeline_status="broken",
            bugs_found=0,
            bugs_fixed=0,
            total_bugs=len(self._bugs),
            available_tables=list(self._tables.keys()),
            hint=f"Use 'inspect_schema' or 'inspect_data' on a table to start investigating.",
        )

    def step(self, action: PipelineAction) -> PipelineObservation:
        self._state.step_count += 1
        self._state.commands_used.append(action.command)

        cmd = action.command.lower().strip()
        target = action.target.strip()

        # Check step limit
        if self._state.step_count > self._state.max_steps:
            return self._make_terminal_observation("Step limit reached. Episode over.")

        handler = {
            "inspect_schema": self._handle_inspect_schema,
            "inspect_data": self._handle_inspect_data,
            "run_query": self._handle_run_query,
            "diagnose": self._handle_diagnose,
            "fix": self._handle_fix,
            "validate": self._handle_validate,
        }.get(cmd)

        if handler is None:
            return self._make_observation(
                output=f"Unknown command: '{cmd}'. Valid commands: inspect_schema, inspect_data, run_query, diagnose, fix, validate",
                reward=-0.05,
                error=f"Unknown command: {cmd}",
            )

        return handler(target, action.parameters)

    @property
    def state(self) -> PipelineState:
        return self._state

    # ──── Command Handlers ────

    def _handle_inspect_schema(self, target: str, params: Dict) -> PipelineObservation:
        if target not in self._tables:
            return self._make_observation(
                output=f"Table '{target}' not found. Available: {list(self._tables.keys())}",
                reward=0.0,
                error=f"Table not found: {target}",
            )
        schema = self._tables[target]["schema"]
        schema_str = "\n".join(f"  {col}: {dtype}" for col, dtype in schema.items())
        return self._make_observation(
            output=f"Schema for '{target}':\n{schema_str}",
            reward=0.05,
        )

    def _handle_inspect_data(self, target: str, params: Dict) -> PipelineObservation:
        if target not in self._tables:
            return self._make_observation(
                output=f"Table '{target}' not found. Available: {list(self._tables.keys())}",
                reward=0.0,
                error=f"Table not found: {target}",
            )
        data = self._tables[target]["data"]
        limit = params.get("limit", 5)
        rows = data[:limit]
        rows_str = json.dumps(rows, indent=2, default=str)
        return self._make_observation(
            output=f"Data from '{target}' ({len(rows)} of {len(data)} rows):\n{rows_str}",
            reward=0.05,
        )

    def _handle_run_query(self, target: str, params: Dict) -> PipelineObservation:
        """Simulate a simple query — mostly for the agent to test hypotheses."""
        query = target.lower()

        # Simple COUNT support
        count_match = re.search(r"count.*from\s+(\w+)", query)
        if count_match:
            table = count_match.group(1)
            if table in self._tables:
                count = len(self._tables[table]["data"])
                return self._make_observation(output=f"COUNT(*) from {table}: {count}", reward=0.05)

        # Simple SELECT support
        select_match = re.search(r"select.*from\s+(\w+)", query)
        if select_match:
            table = select_match.group(1)
            if table in self._tables:
                data = self._tables[table]["data"][:3]
                return self._make_observation(output=f"Query result:\n{json.dumps(data, indent=2, default=str)}", reward=0.05)

        return self._make_observation(
            output=f"Query executed. Note: This environment supports basic COUNT and SELECT queries for investigation. Use 'fix' to apply corrections.",
            reward=0.02,
        )

    def _handle_diagnose(self, target: str, params: Dict) -> PipelineObservation:
        """Agent describes a suspected bug. Reward if it matches an unfixed bug."""
        diagnosis = target.lower()
        reward = 0.0
        matched_bugs = []

        for bug in self._bugs:
            if bug["fixed"]:
                continue
            # Check if the diagnosis mentions enough keywords
            matches = sum(1 for kw in bug["fix_keywords"] if kw.lower() in diagnosis)
            if matches >= 2:
                matched_bugs.append(bug)
                reward += 0.15

        if matched_bugs:
            descriptions = "\n".join(f"  - {b['description']}" for b in matched_bugs)
            return self._make_observation(
                output=f"Good diagnosis! You identified {len(matched_bugs)} issue(s):\n{descriptions}",
                reward=reward,
            )
        else:
            return self._make_observation(
                output="Diagnosis doesn't match any known unfixed bugs. Keep investigating.",
                reward=0.0,
                hint="Try inspecting schemas and data more carefully.",
            )

    def _handle_fix(self, target: str, params: Dict) -> PipelineObservation:
        """Agent submits a fix. Match against unfixed bugs."""
        fix_text = target.lower()
        reward = 0.0
        fixed_bugs = []

        for bug in self._bugs:
            if bug["fixed"]:
                continue
            matches = sum(1 for kw in bug["fix_keywords"] if kw.lower() in fix_text)
            # Require stronger match for fix than for diagnose
            threshold = 3 if len(bug["fix_keywords"]) > 5 else 2
            if matches >= threshold:
                bug["fixed"] = True
                fixed_bugs.append(bug)
                self._state.bugs_remaining -= 1
                reward += 0.3

        if fixed_bugs:
            descriptions = "\n".join(f"  - FIXED: {b['description']}" for b in fixed_bugs)
            all_fixed = all(b["fixed"] for b in self._bugs)

            if all_fixed:
                # Bonus for completing all fixes
                reward += 0.2
                # Efficiency bonus: fewer steps = higher bonus
                efficiency = max(0, 1.0 - (self._state.step_count / self._state.max_steps))
                reward += efficiency * 0.3
                return self._make_terminal_observation(
                    f"All bugs fixed! Pipeline is healthy.\n{descriptions}",
                    reward=reward,
                    status="fixed",
                )

            return self._make_observation(
                output=f"Fix applied successfully:\n{descriptions}\n{self._state.bugs_remaining} bug(s) remaining.",
                reward=reward,
            )
        else:
            return self._make_observation(
                output="Fix did not match any known bugs. Check your fix description and try again.",
                reward=-0.05,
                hint="Make sure your fix addresses a specific issue (e.g., 'cast price column to float', 'rename uid to user_id').",
            )

    def _handle_validate(self, target: str, params: Dict) -> PipelineObservation:
        """Run validation — returns summary of remaining issues."""
        unfixed = [b for b in self._bugs if not b["fixed"]]
        fixed = [b for b in self._bugs if b["fixed"]]

        if not unfixed:
            return self._make_terminal_observation(
                "Validation passed! All pipeline bugs have been fixed.",
                reward=0.2,
                status="fixed",
            )

        # Give hints about remaining bugs without giving away the answer
        hints = []
        for b in unfixed:
            hints.append(f"  - Issue in '{b['table']}.{b['column']}': {b['fix_type'].replace('_', ' ')}")

        return self._make_observation(
            output=f"Validation: {len(fixed)}/{len(self._bugs)} bugs fixed.\nRemaining issues:\n" + "\n".join(hints),
            reward=0.05,
        )

    # ──── Helpers ────

    def _make_observation(
        self,
        output: str,
        reward: float = 0.0,
        error: str = "",
        hint: str = "",
    ) -> PipelineObservation:
        bugs_fixed = sum(1 for b in self._bugs if b["fixed"])
        total = len(self._bugs)
        if bugs_fixed == total:
            status = "fixed"
        elif bugs_fixed > 0:
            status = "partially_fixed"
        else:
            status = "broken"

        self._state.score = bugs_fixed / total if total > 0 else 0.0

        return PipelineObservation(
            done=False,
            reward=reward,
            output=output,
            pipeline_status=status,
            bugs_found=bugs_fixed,  # simplified: found == fixed for scoring
            bugs_fixed=bugs_fixed,
            total_bugs=total,
            available_tables=list(self._tables.keys()),
            hint=hint,
            error_message=error,
        )

    def _make_terminal_observation(
        self,
        output: str,
        reward: float = 0.0,
        status: str = "broken",
    ) -> PipelineObservation:
        bugs_fixed = sum(1 for b in self._bugs if b["fixed"])
        total = len(self._bugs)

        if status == "fixed" or bugs_fixed == total:
            status = "fixed"
        elif bugs_fixed > 0:
            status = "partially_fixed"

        self._state.score = bugs_fixed / total if total > 0 else 0.0

        return PipelineObservation(
            done=True,
            reward=reward,
            output=output,
            pipeline_status=status,
            bugs_found=bugs_fixed,
            bugs_fixed=bugs_fixed,
            total_bugs=total,
            available_tables=list(self._tables.keys()),
        )

"""
Multi-Agent Supervisor using LangGraph + GenieAgent

This module defines a multi-agent supervisor that routes user questions
to the appropriate Genie space(s). It is logged with MLflow and deployed
to a Model Serving endpoint via agents.deploy().

Configuration is injected via environment variables at deploy time:
  - FIELD_OPS_SPACE_ID
  - POSTGRES_SPACE_ID
  - NETWORK_HEALTH_SPACE_ID
  - SLA_WORKFORCE_SPACE_ID
  - LLM_ENDPOINT (defaults to databricks-claude-sonnet-4-5)

IMPORTANT: All agent/LLM construction is deferred to first predict() call
so that mlflow.pyfunc.log_model() can import this file without triggering
API calls to Databricks services.
"""

import os
from typing import Generator
from uuid import uuid4

import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)


# Bounds supervisor hand-offs. Each extra hop re-runs a Genie query, the most
# expensive step in the chain.
MAX_SUPERVISOR_STEPS = int(os.environ.get("MAX_SUPERVISOR_STEPS", "8"))

# Genie returns its full result set as a markdown table -- measured at 43K
# characters for a regional SLA breakdown. Handing all of it back to the
# supervisor made synthesis a 9.8s call. The user only ever sees the summary,
# so the tail of the table costs latency and tokens to no benefit.
MAX_AGENT_OUTPUT_CHARS = int(os.environ.get("MAX_AGENT_OUTPUT_CHARS", "6000"))
MAX_AGENT_OUTPUT_LINES = int(os.environ.get("MAX_AGENT_OUTPUT_LINES", "60"))

SUPERVISOR_PROMPT = """You are an expert AI assistant for a Telco Field Service Management system built on the Databricks Data Intelligence Platform. You have two capabilities:

## Capability 1: Query Live Operational Data
Route questions about live data to specialized agents:
- FieldOpsGenie: Work orders, technicians, dispatch, scheduling, appointments, equipment, parts, field operations
- PostgresAdminGenie: Database health, connections, replication, query performance, pg_stat, vacuum, indexing
- NetworkHealthGenie: Network node health, outages, IoT telemetry, sensor data, maintenance risk, infrastructure
- SLAWorkforceGenie: SLA compliance, technician performance, workforce utilization, regional analytics, customer tiers

## Capability 2: Databricks Platform Expertise
For questions about Databricks features, architecture, best practices, configuration, APIs, or how-to guidance, answer DIRECTLY from your knowledge. You are an expert on:
- Lakebase (managed PostgreSQL): autoscaling, branches, native PG auth, connection pooling, CU scaling
- Unity Catalog: catalogs, schemas, tables, volumes, governance, lineage, tags, row/column security
- MLflow: experiment tracking, model registry, model serving, @production aliases, feature store
- Genie AI / AI BI: spaces, natural language SQL, sample questions, permissions
- Model Serving: endpoints, Foundation Model APIs, LangGraph agents, streaming
- DLT / Lakeflow: pipelines, medallion architecture, Managed Iceberg, streaming tables
- Databricks Apps: Flask deployment, OAuth, service principals, app.yaml, valueFrom secrets
- SQL Warehouse: serverless, Statement Execution API, query federation
- Spark: Structured Streaming, DataFrame API, Spark Connect, serverless compute
- Jobs & Workflows: scheduling, notebook tasks, environments, serverless compute
- Delta Lake & Iceberg: table formats, liquid clustering, deletion vectors, time travel
- Databricks CLI: repos, workspace, jobs, apps, secrets commands
- Security: IAM, network security, private link, IP access lists, secrets scopes

## This Application: Lakebase Field Service Management
This app is a full-stack Databricks demo for Telco field ops. Here's what's implemented:

**Lakebase (PostgreSQL OLTP)**: 5M+ work orders, 2500 technicians, 50K customers in autoscaling Lakebase instance. Dual rotation roles (lakebase_app_a/b). PG scoring function `compute_dispatch_scores()` for ML-optimized dispatch. SLA risk trigger on INSERT/UPDATE. 14 monitoring views + ASH sampler.

**Intelligent Dispatch**: LightGBM model (RMSE=0.016) trained on 200K historical outcomes. 5-factor scoring: skill (30pts), haversine distance (25pts), capacity (20pts), SLA urgency (15pts), tech rating (10pts). Max 8 active WOs per tech. Appointment windows on work orders.

**Service Orders**: TMF 641 pattern — parent entity for work orders. Customer request creates 1 SO with 1-3 child WOs.

**OSRM Road Routing**: Field map uses actual road distances via OSRM API (not straight lines). Shows distance in km and travel time.

**AI Memory**: Conversations persisted in Lakebase `ai_memory` schema. User identity from OAuth JWT. Full-text search across all past conversations.

**Network Health**: D3.js topology graph, correlated incidents (alarm → root cause), customer impact analysis. Gold tables from DLT Spark Streaming pipeline (Bronze→Silver→Gold Managed Iceberg).

**TMF 621 API**: Industry-standard trouble ticket REST API at /api/tmf/troubleTickets.

**Unity Catalog**: Foreign catalog mirrors Lakebase tables. Managed catalog for Iceberg. Model registry with @production alias. Column-level PII tags. Row-level security views.

**Genie AI**: 4 specialized spaces (Field Ops, Postgres Admin, Network Health, SLA & Workforce).

**RBAC**: User roles (admin/dispatcher/manager/user) with region-based data filtering.

**Cmd+K Command Bar**: AI-powered command bar on every page with contextual suggestions.

## Routing Rules:
1. If the question is about LIVE DATA (work orders, SLAs, technicians, network status), route to the appropriate Genie agent(s).
2. If the question is about the DATABRICKS PLATFORM or THIS APP'S ARCHITECTURE, answer directly from your knowledge + the app context above.
3. If the question spans both (e.g., "how does our Lakebase instance handle the 5M work orders?"), answer the platform part directly and route the data part to an agent.
4. Route to multiple agents when the question spans data domains.
5. After receiving agent responses, synthesize them into a clear, unified answer.
6. If an agent returns an error, acknowledge it and provide what you can.
"""


# Synthesis turns only need to combine results the agents already returned — the
# routing rules, capability catalogue and app specification are dead weight there.
# The routing turn keeps the full prompt so questions answered directly from
# knowledge (Capability 2) lose nothing.
SYNTHESIS_PROMPT = """You are the supervisor for a Telco Field Service Management assistant.

The specialist agents below have already returned their results. Combine them into
one clear answer for the user:
- Lead with the direct answer; keep numbers and units exactly as returned.
- Preserve tables where they aid readability.
- If an agent reported an error, say so plainly and answer with what you do have.
- Do not invent data that no agent returned.

The agents have already run and will not be called again. Do not hand off or
request more data -- write the final answer from what is present, even if a
result looks partial. Handing off here loops until the recursion cap and the
user gets nothing.
"""

# Agent names used to detect that the graph has moved past routing.
_AGENT_NAMES = {
    "FieldOpsGenie",
    "PostgresAdminGenie",
    "NetworkHealthGenie",
    "SLAWorkforceGenie",
}


def _truncate_agent_output(text):
    """Cap a Genie agent's table so synthesis reads a sample, not the whole thing.

    Truncation is line-aware so a markdown table keeps its header and whole
    rows, and the model is told the table was cut -- otherwise it will state
    sample figures as if they covered everything.
    """
    if not isinstance(text, str) or len(text) <= MAX_AGENT_OUTPUT_CHARS:
        return text

    lines = text.splitlines()
    kept, used = [], 0
    for line in lines[:MAX_AGENT_OUTPUT_LINES]:
        if used + len(line) + 1 > MAX_AGENT_OUTPUT_CHARS:
            break
        kept.append(line)
        used += len(line) + 1

    omitted = len(lines) - len(kept)
    if omitted <= 0:
        return text

    # Wording matters: an earlier version said the rows were "a sample, not the
    # complete result set", and the supervisor responded by handing off to the
    # agent again to fetch the rest -- looping until the recursion cap and
    # failing the deploy. The note must close that door explicitly.
    kept.append(
        f"\n[The agent returned {len(lines)} rows; the first {len(kept)} are shown "
        f"and {omitted} were trimmed to keep this prompt small. This is expected and "
        f"final -- do NOT call the agent again. Answer from the rows above, and if "
        f"the question needs a total the rows do not cover, say which part is not "
        f"covered.]"
    )
    return "\n".join(kept)


def _trimmed_history(msgs):
    """Return msgs with oversized agent results truncated.

    Only what the supervisor LLM reads is affected -- the untouched message is
    still what streams to the UI and gets stored, so the app can show the full
    table while synthesis works from a sample.
    """
    out = []
    for m in msgs:
        if getattr(m, "name", None) in _AGENT_NAMES:
            content = getattr(m, "content", None)
            if isinstance(content, str):
                trimmed = _truncate_agent_output(content)
                if trimmed != content:
                    try:
                        m = m.model_copy(update={"content": trimmed})
                    except Exception:
                        # Older pydantic / non-model message: fall back to a copy
                        # rather than mutating shared graph state.
                        try:
                            m = m.copy(update={"content": trimmed})
                        except Exception:
                            pass
        out.append(m)
    return out


def _supervisor_prompt(state):
    """Per-turn system prompt.

    create_supervisor accepts Callable[[state], LanguageModelInput], so the same
    supervisor node can use a different system prompt depending on where the graph
    is. Before any agent has answered we are choosing a route (or answering a
    platform question directly) and need the full context; afterwards we are only
    synthesising, and carrying ~950 tokens of routing rules and app specification
    into that second call buys nothing.
    """
    from langchain_core.messages import SystemMessage

    msgs = state.get("messages", []) if isinstance(state, dict) else getattr(state, "messages", [])
    consulted = any(getattr(m, "name", None) in _AGENT_NAMES for m in msgs)
    system = SYNTHESIS_PROMPT if consulted else SUPERVISOR_PROMPT
    return [SystemMessage(content=system)] + _trimmed_history(msgs)


def _user_workspace_client():
    """WorkspaceClient bound to the *invoking user* (on-behalf-of-user auth).

    The serving endpoint's managed service principal cannot be granted Unity
    Catalog access — it is hidden from SCIM, so
    `PATCH .../permissions/table/...` fails with "Could not find principal", and
    it is not covered by `account users`. Genie then refuses every query with
    "No access to '<catalog>.field_service.<table>'".

    With OBO the call runs as whoever invoked the endpoint (a signed-in user, or
    the app's service principal when the request comes from the Flask app) —
    identities that already hold SELECT on these tables.

    Returns None if the OBO credential strategy is unavailable, in which case the
    agent falls back to default (system) auth.
    """
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.credentials_provider import ModelServingUserCredentials

        return WorkspaceClient(credentials_strategy=ModelServingUserCredentials())
    except Exception as e:  # pragma: no cover - depends on serving runtime
        print(f"OBO credentials unavailable, falling back to system auth: {e}")
        return None


def _build_supervisor(user_client=None):
    """Build the LangGraph supervisor.

    Built per request rather than cached: the OBO client resolves the *current*
    invocation's user, so a cached supervisor would keep serving every later
    request with the first caller's credentials.
    """
    from databricks_langchain import ChatDatabricks
    from databricks_langchain.genie import GenieAgent
    from langgraph_supervisor import create_supervisor

    field_ops_id = os.environ.get("FIELD_OPS_SPACE_ID", "")
    postgres_id = os.environ.get("POSTGRES_SPACE_ID", "")
    network_health_id = os.environ.get("NETWORK_HEALTH_SPACE_ID", "")
    sla_workforce_id = os.environ.get("SLA_WORKFORCE_SPACE_ID", "")
    llm_endpoint = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")

    agents_list = []

    if field_ops_id:
        agent = GenieAgent(
            genie_space_id=field_ops_id,
            client=user_client,
            genie_agent_name="FieldOpsGenie",
            description=(
                "Handles questions about work orders, technicians, dispatch, "
                "scheduling, appointments, equipment, parts, regions, "
                "completion rates, and field service operations."
            ),
        )
        agent.name = "FieldOpsGenie"
        agents_list.append(agent)

    if postgres_id:
        agent = GenieAgent(
            genie_space_id=postgres_id,
            client=user_client,
            genie_agent_name="PostgresAdminGenie",
            description=(
                "Handles questions about PostgreSQL database health, connections, "
                "replication, query performance, pg_stat metrics, vacuum, indexing, "
                "WAL, checkpoints, locks, and cache hit ratios."
            ),
        )
        agent.name = "PostgresAdminGenie"
        agents_list.append(agent)

    if network_health_id:
        agent = GenieAgent(
            genie_space_id=network_health_id,
            client=user_client,
            genie_agent_name="NetworkHealthGenie",
            description=(
                "Handles questions about network node health, outage analysis, "
                "IoT device telemetry, sensor readings, maintenance risk predictions, "
                "regional network performance, and infrastructure monitoring."
            ),
        )
        agent.name = "NetworkHealthGenie"
        agents_list.append(agent)

    if sla_workforce_id:
        agent = GenieAgent(
            genie_space_id=sla_workforce_id,
            client=user_client,
            genie_agent_name="SLAWorkforceGenie",
            description=(
                "Handles questions about SLA compliance rates, technician performance "
                "metrics, workforce utilization, regional work order distribution, "
                "customer tier analysis, and service level trends."
            ),
        )
        agent.name = "SLAWorkforceGenie"
        agents_list.append(agent)

    if not agents_list:
        raise ValueError("No Genie space IDs configured — cannot build supervisor")

    llm = ChatDatabricks(endpoint=llm_endpoint)

    return create_supervisor(
        agents=agents_list,
        model=llm,
        prompt=_supervisor_prompt,
        add_handoff_messages=False,
        output_mode="full_history",
    ).compile()


class MultiGenieSupervisor(ResponsesAgent):
    """Wraps the LangGraph supervisor as a ResponsesAgent for MLflow serving.

    Lazily initializes the supervisor on first predict() call to avoid
    import-time API calls that break mlflow.pyfunc.log_model().
    """

    def __init__(self):
        pass

    def _supervisor_for_request(self):
        """Build a supervisor bound to the current caller.

        Deliberately NOT cached. Under on-behalf-of-user auth the Genie client
        carries the invoking user's credentials, so reusing an instance across
        requests would run every later caller's questions as the first caller —
        a cross-user data leak. Construction is local object wiring (no API
        calls), so the per-request cost is small.
        """
        return _build_supervisor(user_client=_user_workspace_client())

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        cc_msgs = to_chat_completions_input([i.model_dump() for i in request.input])
        first_message = True
        seen_ids = set()

        supervisor = self._supervisor_for_request()

        # A stable id for the token stream so the client can attach the deltas to one
        # growing message.
        delta_item_id = str(uuid4())

        # recursion_limit bounds how many times the supervisor may hand off. Without
        # it a supervisor that re-queries the same agent runs the expensive Genie step
        # twice (observed: the same agent routed at 56s and again at 92s), doubling
        # latency for no extra information.
        run_config = {"recursion_limit": MAX_SUPERVISOR_STEPS}

        # "messages" yields LLM token chunks as they are produced; "updates" yields the
        # per-node results this method already relied on. Streaming tokens is what lets
        # the UI show words within a couple of seconds instead of a ~20s blank wait.
        for mode, payload in supervisor.stream(
            {"messages": cc_msgs}, stream_mode=["updates", "messages"], config=run_config
        ):
            if mode == "messages":
                chunk, meta = payload if isinstance(payload, tuple) else (payload, {})
                # Only the supervisor's own tokens are the user-facing answer; agent
                # nodes emit tool chatter that would be noise mid-stream.
                if (meta or {}).get("langgraph_node") != "supervisor":
                    continue
                text = getattr(chunk, "content", "") or ""
                if isinstance(text, list):
                    # Some providers return content blocks rather than a plain string.
                    text = "".join(
                        b.get("text", "") for b in text if isinstance(b, dict)
                    )
                if text:
                    yield ResponsesAgentStreamEvent(
                        type="response.output_text.delta",
                        item_id=delta_item_id,
                        delta=text,
                    )
                continue

            events = payload
            new_msgs = [
                msg
                for v in events.values()
                if v is not None
                for msg in v.get("messages", [])
                if msg.id not in seen_ids
            ]
            if first_message:
                seen_ids.update(msg.id for msg in new_msgs[: len(cc_msgs)])
                new_msgs = new_msgs[len(cc_msgs):]
                first_message = False
            else:
                seen_ids.update(msg.id for msg in new_msgs)

            node_name = tuple(events.keys())[0]
            yield ResponsesAgentStreamEvent(
                type="response.output_item.done",
                item=self.create_text_output_item(
                    text=f"<agent>{node_name}</agent>",
                    id=str(uuid4()),
                ),
            )
            if new_msgs:
                yield from output_to_responses_items_stream(new_msgs)


# -- Register with MLflow -----------------------------------------------------
mlflow.langchain.autolog()
AGENT = MultiGenieSupervisor()
mlflow.models.set_model(AGENT)

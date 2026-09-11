"""
Genie AI + Agent Supervisor API Blueprint.

This blueprint handles two major subsystems:

1. **Genie API** -- Direct natural language Q&A with Databricks AI/BI Genie
   spaces. Supports listing spaces, fetching sample questions, and executing
   queries with polling for completion.

2. **Agent Supervisor API** -- Multi-agent orchestration via a LangGraph agent
   deployed on Databricks Model Serving. Includes synchronous ask, SSE
   streaming (Server-Sent Events), endpoint status discovery, and inference
   trace retrieval from both the inference table (via SQL Statement API) and
   MLflow (as fallback).

Route groups
============
* ``/api/genie/spaces``                -- List available Genie spaces
* ``/api/genie/spaces/<key>/questions`` -- Fetch sample questions for a space
* ``/api/genie/ask``                   -- Execute a question against a Genie space
* ``/api/agent/ask``                   -- Synchronous agent invocation
* ``/api/agent/ask-stream``            -- SSE streaming agent invocation
* ``/api/agent/status``                -- Agent endpoint health/readiness
* ``/api/agent/traces``                -- Inference trace retrieval

Dependencies
------------
* ``shared.get_workspace_client`` -- Databricks SDK client for Genie, Model Serving, MLflow
* ``shared.GENIE_SPACES`` -- Space configuration from env vars
* ``shared.AGENT_ENDPOINT_NAME`` -- Agent endpoint name from env vars
* ``shared.log_error`` -- Ring-buffer error logger
"""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import threading
import time

from flask import Blueprint, Response, jsonify, request

from shared import get_workspace_client, get_pool, get_current_user, get_role_from_groups, get_user_databricks_groups, GENIE_SPACES, AGENT_ENDPOINT_NAME, log_error

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blueprint definition
# ---------------------------------------------------------------------------
genie_agent_bp = Blueprint("genie_agent", __name__)

# ---------------------------------------------------------------------------
# Agent configuration (module-level)
# ---------------------------------------------------------------------------
# Mutable reference so _discover_agent_endpoint can update it at runtime
_agent_endpoint_name = AGENT_ENDPOINT_NAME

# Inference table for trace retrieval (set via env var or default)
AGENT_INFERENCE_TABLE = os.environ.get(
    "AGENT_INFERENCE_TABLE",
    "dba-lakebase-network.agents.multi_genie_supervisor_payload",
)

# Per-user conversation cap. Keeps ai_memory from growing unbounded (each AI
# Supervisor question can start a conversation). When a user exceeds this many
# conversations, the oldest are pruned in the background. Messages are removed
# automatically via ON DELETE CASCADE on ai_memory.messages.
MAX_CONVERSATIONS_PER_USER = int(os.environ.get("MAX_CONVERSATIONS_PER_USER", "50"))

# Max conversation age (days). Even under the per-user cap, ancient threads from
# light users expire so history behaves like a consumer chat app. Set to 0 to
# disable age-based expiry and rely on the cap alone.
MAX_CONVERSATION_AGE_DAYS = int(os.environ.get("MAX_CONVERSATION_AGE_DAYS", "180"))


def _get_agent_endpoint():
    """Return the current agent endpoint name (may have been auto-discovered)."""
    return _agent_endpoint_name


# ═══════════════════════════════════════════════════════════════════════════
# Genie Space Listing & Sample Questions
# ═══════════════════════════════════════════════════════════════════════════

@genie_agent_bp.route("/api/genie/spaces")
def list_genie_spaces():
    """Return all configured Genie spaces with their IDs and descriptions."""
    return jsonify(GENIE_SPACES)


@genie_agent_bp.route("/api/genie/spaces/<space_key>/questions")
def get_space_questions(space_key):
    """Fetch sample questions for a specific Genie space.

    Retrieves the serialized space definition from the Genie API and extracts
    the ``sample_questions`` from the config section.
    """
    try:
        if space_key not in GENIE_SPACES:
            return jsonify({"error": f"Invalid space key: {space_key}"}), 400

        space_id = GENIE_SPACES[space_key]["id"]
        w = get_workspace_client()

        # Fetch space with serialized config (requires CAN_EDIT permission)
        response = w.api_client.do(
            method="GET",
            path=f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
        )

        if "serialized_space" in response:
            try:
                serialized = json.loads(response["serialized_space"])
                sample_questions = serialized.get("config", {}).get("sample_questions", [])
                questions = []
                for sq in sample_questions:
                    if "question" in sq and sq["question"]:
                        # question can be a string or a list
                        q = sq["question"][0] if isinstance(sq["question"], list) else sq["question"]
                        questions.append(q)
                return jsonify({"questions": questions})
            except json.JSONDecodeError as e:
                log_error("parse_serialized_space", e)
                return jsonify({"questions": []})

        return jsonify({"questions": []})

    except Exception as e:
        log_error("get_space_questions", e)
        return jsonify({"questions": []})


# ═══════════════════════════════════════════════════════════════════════════
# Genie Ask (Direct Query)
# ═══════════════════════════════════════════════════════════════════════════

@genie_agent_bp.route("/api/genie/ask", methods=["POST"])
def genie_ask():
    """Execute a natural language question against a Genie space.

    Supports both new conversations and continuing an existing one (via
    ``conversation_id``). Delegates to ``_query_genie_space_internal``, which
    uses the SDK's native Genie wait-helpers for documented polling behavior.
    """
    try:
        data = request.json
        question = data.get("question")
        conversation_id = data.get("conversation_id")
        space_key = data.get("space_key", "postgres")

        if space_key not in GENIE_SPACES:
            return jsonify({"error": f"Invalid space key: {space_key}"}), 400

        log.info(f"GENIE: space={space_key}, question='{question[:80]}'")

        result = _query_genie_space_internal(space_key, question, conversation_id)

        status = result.get("status")
        if status == "COMPLETED":
            return jsonify({
                "conversation_id": result.get("conversation_id"),
                "message_id": result.get("message_id"),
                "status": status,
                "content": result.get("content") or "No response generated",
                "query_info": result.get("query_info"),
                "query_result": result.get("query_result"),
                "suggested_questions": result.get("suggested_questions", []),
                "space_key": space_key,
                "error": None,
            })

        # Non-COMPLETED terminal states (FAILED / CANCELLED / QUERY_RESULT_EXPIRED / TIMEOUT)
        error_msg = result.get("error") or f"Genie query {str(status).lower()}"
        log.warning(f"GENIE {status}: {error_msg[:300]}")
        return jsonify({
            "conversation_id": result.get("conversation_id"),
            "message_id": result.get("message_id"),
            "status": status,
            "content": "",
            "error": error_msg,
        })

    except Exception as e:
        log_error("genie_ask", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Internal Genie Query (shared by both genie/ask and agent/ask)
# ═══════════════════════════════════════════════════════════════════════════

def _query_genie_space_internal(space_key, question, conversation_id=None):
    """Reusable Genie query -- called by both /api/genie/ask and /api/agent/ask.

    Uses the Databricks SDK's native Genie wait-helpers (``w.genie``) instead of
    a hand-rolled polling loop. The SDK handles the documented poll cadence and
    the full status lifecycle (SUBMITTED -> FILTERING_CONTEXT -> ... -> COMPLETED)
    internally, blocking until a terminal state (default 1200s cap).

    Passing ``conversation_id`` continues an existing conversation; otherwise a
    new one is started.

    Returns a dict with keys: status, content, query_info, query_result,
    suggested_questions, conversation_id, message_id, error.
    """
    space_id = GENIE_SPACES[space_key]["id"]
    if not space_id:
        return {"status": "FAILED", "error": f'Genie space "{space_key}" not configured'}

    w = get_workspace_client()

    try:
        if conversation_id:
            msg = w.genie.create_message_and_wait(space_id, conversation_id, question)
        else:
            msg = w.genie.start_conversation_and_wait(space_id, question)
    except Exception as e:
        log_error("genie_wait", e)
        return {"status": "FAILED", "error": str(e)}

    status = str(msg.status.value if msg.status else "").upper()
    conv_id = msg.conversation_id
    message_id = msg.message_id or msg.id

    if status != "COMPLETED":
        # Terminal non-success (FAILED / CANCELLED / QUERY_RESULT_EXPIRED) or,
        # rarely, a timeout that returned a non-terminal message.
        error_msg = None
        if msg.error:
            error_msg = getattr(msg.error, "error", None) or str(msg.error)
        if not error_msg:
            for att in (msg.attachments or []):
                if att.text and att.text.content:
                    error_msg = att.text.content
                    break
        return {
            "status": status or "UNKNOWN",
            "error": error_msg or f"Genie query {status.lower() or 'did not complete'}",
            "conversation_id": conv_id,
            "message_id": message_id,
        }

    # COMPLETED -- extract attachments (query info, text, suggested questions)
    query_info = None
    response_text = None
    query_result_data = None
    suggested_questions = []

    for att in (msg.attachments or []):
        if att.query:
            query_info = {
                "description": att.query.description or "",
                "query": att.query.query or "",
                "statement_id": att.query.statement_id or "",
                "attachment_id": att.attachment_id or "",
            }
            if att.attachment_id:
                try:
                    qr = w.genie.get_message_attachment_query_result(
                        space_id, conv_id, message_id, att.attachment_id
                    )
                    # Preserve the wire shape the frontend already consumes.
                    query_result_data = qr.as_dict() if qr else None
                except Exception as e:
                    log_error("genie_query_result", e)
        if att.text and att.text.content:
            response_text = att.text.content
        if att.suggested_questions:
            sq = att.suggested_questions.as_dict()
            suggested_questions = sq.get("questions", []) or []

    return {
        "status": "COMPLETED",
        "content": response_text or "",
        "query_info": query_info,
        "query_result": query_result_data,
        "suggested_questions": suggested_questions,
        "conversation_id": conv_id,
        "message_id": message_id,
        "error": None,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Agent Supervisor: Parse Output
# ═══════════════════════════════════════════════════════════════════════════

def _parse_agent_output(data):
    """Parse agent ``<agent>`` tags and extract text from ResponsesAgent output.

    Returns (answer_text, spaces_consulted_list).
    """
    spaces_consulted = []
    text_parts = []

    if "output" in data:
        for item in data["output"]:
            if isinstance(item, dict) and item.get("type") == "message":
                for c in item.get("content", []):
                    text = c.get("text", "")
                    # Extract <agent>Name</agent> routing tags
                    agent_matches = re.findall(r"<agent>(\w+)</agent>", text)
                    for agent_name in agent_matches:
                        if agent_name not in ("supervisor", "__start__", "__end__") and agent_name not in spaces_consulted:
                            spaces_consulted.append(agent_name)
                    # Strip agent tags from display text
                    clean_text = re.sub(r"<agent>\w+</agent>\s*", "", text).strip()
                    if clean_text:
                        text_parts.append(clean_text)
    elif "choices" in data and data["choices"]:
        # OpenAI-compatible response format
        content = data["choices"][0].get("message", {}).get("content", "")
        text_parts.append(content)

    answer = "\n".join(text_parts) if text_parts else str(data)
    return answer, spaces_consulted


# ═══════════════════════════════════════════════════════════════════════════
# Agent Auto-Discovery
# ═══════════════════════════════════════════════════════════════════════════

def _discover_agent_endpoint():
    """Auto-discover the agent endpoint if not set via env var.

    Searches all serving endpoints for one matching the ``genie_supervisor``
    naming pattern. Updates the module-level ``_agent_endpoint_name``.
    """
    global _agent_endpoint_name
    if _agent_endpoint_name:
        return _agent_endpoint_name
    try:
        w = get_workspace_client()
        for ep in w.serving_endpoints.list():
            if ep.name and "genie_supervisor" in ep.name:
                _agent_endpoint_name = ep.name
                log.info(f"Auto-discovered agent endpoint: {_agent_endpoint_name}")
                return _agent_endpoint_name
    except Exception as e:
        log.warning(f"Agent endpoint discovery failed: {e}")
    return ""


# ═══════════════════════════════════════════════════════════════════════════
# Agent Status
# ═══════════════════════════════════════════════════════════════════════════

@genie_agent_bp.route("/api/agent/status")
def agent_status():
    """Check agent endpoint status -- called by supervisor page on load and refresh."""
    endpoint_name = _discover_agent_endpoint()
    if not endpoint_name:
        return jsonify({
            "deployed": False,
            "message": "No agent endpoint found. Run 08_create_agent.py to deploy.",
        })
    try:
        w = get_workspace_client()
        ep = w.serving_endpoints.get(endpoint_name)
        ready = str(ep.state.ready).split(".")[-1] if ep.state else "UNKNOWN"

        # Extract LLM model from endpoint config (dynamic — reads from serving endpoint)
        llm_model = "unknown"
        try:
            config = ep.config
            if config and config.served_entities:
                for entity in config.served_entities:
                    env_vars = entity.environment_vars or {}
                    if "LLM_ENDPOINT" in env_vars:
                        llm_model = env_vars["LLM_ENDPOINT"]
                        break
            if llm_model == "unknown":
                llm_model = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")
        except Exception:
            llm_model = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")

        return jsonify({
            "deployed": True,
            "endpoint_name": endpoint_name,
            "state": ready,
            "ready": ready == "READY",
            "llm_model": llm_model,
        })
    except Exception as e:
        return jsonify({
            "deployed": False,
            "endpoint_name": endpoint_name,
            "error": str(e)[:200],
        })


# ═══════════════════════════════════════════════════════════════════════════
# Agent Ask (Synchronous)
# ═══════════════════════════════════════════════════════════════════════════

@genie_agent_bp.route("/api/agent/ask", methods=["POST"])
def agent_ask():
    """Multi-agent supervisor: routes all questions through the deployed LangGraph agent."""
    try:
        data = request.json
        question = data.get("question", "")
        conversation_id = data.get("conversation_id")
        endpoint_name = _get_agent_endpoint()

        if not endpoint_name:
            return jsonify({
                "error": "Agent supervisor is not deployed. Set AGENT_ENDPOINT_NAME in app.yaml after running 08_create_agent.py.",
            }), 503

        # Memory: get or create conversation, persist user message
        conversation_id = _ensure_conversation(conversation_id, question)
        memory_ok = _save_message(conversation_id, "user", question)
        input_messages = _build_input_with_history(conversation_id, question)

        log.info(f"SUPERVISOR: agent endpoint '{endpoint_name}' for: {question[:80]} (conv={conversation_id})")

        w = get_workspace_client()
        resp = w.api_client.do(
            "POST",
            f"/serving-endpoints/{endpoint_name}/invocations",
            body={"input": input_messages},
        )

        answer, spaces_consulted = _parse_agent_output(resp)

        # Persist assistant response
        if answer:
            memory_ok = _save_message(conversation_id, "assistant", answer, spaces_consulted) and memory_ok

        return jsonify({
            "routing_explanation": f"Routed via AI Agent Supervisor (LangGraph + GenieAgent) on endpoint '{endpoint_name}'.",
            "spaces_consulted": spaces_consulted,
            "agent_endpoint": endpoint_name,
            "conversation_id": conversation_id,
            # Report what actually happened so the UI's "Memory Persist" step
            # reflects the write instead of always claiming success.
            "memory_persisted": bool(conversation_id) and memory_ok,
            "results": [{
                "space_key": "supervisor",
                "space_name": "AI Supervisor",
                "status": "COMPLETED",
                "answer": answer,
                "source": "agent_endpoint",
            }],
        })
    except Exception as e:
        log_error("agent_ask", e)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# Agent Ask Stream (SSE)
# ═══════════════════════════════════════════════════════════════════════════

@genie_agent_bp.route("/api/agent/ask-stream", methods=["POST"])
def agent_ask_stream():
    """SSE endpoint: streams agent reasoning events in real-time.

    Each SSE event contains a JSON object with:
      - type: 'thinking' | 'routing' | 'agent_start' | 'agent_response' | 'synthesis' | 'done' | 'error'
      - agent: agent name (for routing/agent events)
      - text: message content (for response/synthesis events)
      - spaces_consulted: list of all agents consulted (in 'done' event)

    Falls back to non-streaming invocation if the endpoint does not support
    streaming (HTTP != 200 on the streaming request).
    """
    import requests as http_requests

    data = request.json
    question = data.get("question", "")
    conversation_id = data.get("conversation_id")
    endpoint_name = _get_agent_endpoint()

    if not endpoint_name:
        def _err_gen():
            yield f"data: {json.dumps({'type': 'error', 'text': 'Agent not deployed'})}\n\n"
        return Response(_err_gen(), mimetype="text/event-stream")

    # Get or create conversation for memory persistence
    conversation_id = _ensure_conversation(conversation_id, question)

    # Persist user message immediately
    # Tracked so the 'done' event can report whether memory actually persisted,
    # rather than the UI always showing "Saved Q&A to Lakebase ai_memory".
    memory_ok = _save_message(conversation_id, "user", question)

    def _produce_events():
        # memory_ok is assigned above and reassigned below (`... and memory_ok`).
        # Without nonlocal, that assignment makes it local to _produce_events() and
        # the read raises "cannot access local variable 'memory_ok' where it is not
        # associated with a value", which surfaced as an error bubble in the UI.
        nonlocal memory_ok
        try:
            w = get_workspace_client()

            # Host + auth headers for the raw streaming request (the SDK has no
            # streaming call). w.config.token is empty when the app authenticates as
            # an OAuth service principal — there is no static PAT — so building
            # "Bearer {token}" by hand produced
            #   401 Credential was not sent or was of an unsupported type
            # and every request silently fell back to non-streaming.
            # config.authenticate() returns properly formed headers and refreshes.
            host = w.config.host.rstrip("/")
            auth_headers = w.config.authenticate() or {}

            # Get the LLM model name dynamically
            _llm_model = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")

            # Emit conversation ID + model info so frontend can track it
            yield f"data: {json.dumps({'type': 'conversation', 'conversation_id': conversation_id, 'llm_model': _llm_model})}\n\n"
            yield f"data: {json.dumps({'type': 'thinking', 'text': 'Supervisor analyzing your question...'})}\n\n"

            # Build input with conversation history
            input_messages = _build_input_with_history(conversation_id, question)

            url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
            headers = {**auth_headers, "Content-Type": "application/json"}
            payload = {
                "input": input_messages,
                "stream": True,
            }

            spaces_consulted = []
            current_agent = None
            text_parts = []
            synthesis_text = ""

            with http_requests.post(url, json=payload, headers=headers, stream=True, timeout=120) as r:
                if r.status_code != 200:
                    # Streaming not supported -- fall back to non-streaming.
                    # Logged because a silent fallback looks identical to streaming
                    # that simply produced nothing, and the two need different fixes.
                    body_preview = ""
                    try:
                        body_preview = r.text[:300]
                    except Exception:
                        pass
                    log.warning(
                        f"Agent streaming unavailable (HTTP {r.status_code}); "
                        f"falling back to non-streaming. Body: {body_preview}"
                    )
                    yield f"data: {json.dumps({'type': 'thinking', 'text': 'Routing to specialized agents...'})}\n\n"

                    resp = w.api_client.do(
                        "POST",
                        f"/serving-endpoints/{endpoint_name}/invocations",
                        body={"input": input_messages},
                    )
                    answer, spaces_consulted = _parse_agent_output(resp)

                    # Emit routing events for each consulted agent
                    for agent_name in spaces_consulted:
                        yield f"data: {json.dumps({'type': 'routing', 'agent': agent_name, 'text': f'Consulting {agent_name}...'})}\n\n"
                        time.sleep(0.1)

                    yield f"data: {json.dumps({'type': 'synthesis', 'text': answer})}\n\n"
                    if answer and conversation_id:
                        memory_ok = _save_message(conversation_id, "assistant", answer, spaces_consulted) and memory_ok
                    yield f"data: {json.dumps({'type': 'done', 'spaces_consulted': spaces_consulted, 'conversation_id': conversation_id, 'memory_persisted': bool(conversation_id) and memory_ok})}\n\n"
                    return

                # Parse SSE stream from Model Serving
                buffer = ""
                for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
                    if not chunk:
                        continue
                    buffer += chunk
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line or not line.startswith("data:"):
                            continue
                        line_data = line[5:].strip()
                        if line_data == "[DONE]":
                            continue
                        try:
                            event = json.loads(line_data)
                        except json.JSONDecodeError:
                            continue

                        # Token-level deltas: forward immediately so the UI can render
                        # words as they are produced instead of waiting ~20s for the
                        # completed item.
                        if event.get("type") == "response.output_text.delta":
                            delta = event.get("delta") or ""
                            if delta:
                                yield f"data: {json.dumps({'type': 'delta', 'text': delta})}\n\n"
                            continue

                        # Parse ResponsesAgent stream events
                        if event.get("type") == "response.output_item.done":
                            item = event.get("item", {})
                            text = ""
                            if item.get("type") == "message":
                                for c in item.get("content", []):
                                    text += c.get("text", "")
                            elif "text" in item:
                                text = item["text"]

                            # The agent emits "<agent>NODE</agent>" as its own event to
                            # mark which graph node speaks next; the node's actual text
                            # arrives in the following event(s). So a marker only moves
                            # current_agent -- it never carries user-facing content.
                            agent_matches = re.findall(r"<agent>(\w+)</agent>", text)
                            if agent_matches:
                                for agent_name in agent_matches:
                                    if agent_name in ("__start__", "__end__"):
                                        continue
                                    current_agent = agent_name
                                    if agent_name == "supervisor":
                                        # Progress only. Sending placeholder prose here
                                        # made the UI render "Supervisor synthesizing
                                        # final answer..." as the answer itself.
                                        if spaces_consulted:
                                            yield f"data: {json.dumps({'type': 'synthesis'})}\n\n"
                                        continue
                                    if agent_name not in spaces_consulted:
                                        spaces_consulted.append(agent_name)
                                    yield f"data: {json.dumps({'type': 'routing', 'agent': agent_name, 'text': f'Routing to {agent_name}...'})}\n\n"
                                continue

                            clean_text = text.strip()
                            if not clean_text:
                                continue

                            if current_agent in (None, "supervisor") and spaces_consulted:
                                # Supervisor speaking after agents answered: this is the
                                # final synthesis, and it is what the user should see.
                                synthesis_text = clean_text
                                yield f"data: {json.dumps({'type': 'synthesis', 'text': clean_text})}\n\n"
                            else:
                                text_parts.append(clean_text)
                                yield f"data: {json.dumps({'type': 'agent_response', 'agent': current_agent or 'supervisor', 'text': clean_text})}\n\n"

            # Persist the synthesised answer when there is one; falling back to the
            # concatenated parts would store raw agent output alongside it and feed
            # that noise back as history on the next turn.
            final_answer = synthesis_text or "\n".join(text_parts)
            if final_answer and conversation_id:
                memory_ok = _save_message(conversation_id, "assistant", final_answer, spaces_consulted) and memory_ok
            yield f"data: {json.dumps({'type': 'done', 'spaces_consulted': spaces_consulted, 'conversation_id': conversation_id, 'memory_persisted': bool(conversation_id) and memory_ok})}\n\n"

        except Exception as e:
            log_error("agent_ask_stream", e)
            # If streaming failed entirely, try non-streaming fallback
            try:
                w = get_workspace_client()
                resp = w.api_client.do(
                    "POST",
                    f"/serving-endpoints/{endpoint_name}/invocations",
                    body={"input": input_messages},
                )
                answer, spaces_consulted = _parse_agent_output(resp)
                for agent_name in spaces_consulted:
                    yield f"data: {json.dumps({'type': 'routing', 'agent': agent_name})}\n\n"
                yield f"data: {json.dumps({'type': 'synthesis', 'text': answer})}\n\n"
                if answer and conversation_id:
                    memory_ok = _save_message(conversation_id, "assistant", answer, spaces_consulted) and memory_ok
                yield f"data: {json.dumps({'type': 'done', 'spaces_consulted': spaces_consulted, 'conversation_id': conversation_id, 'memory_persisted': bool(conversation_id) and memory_ok})}\n\n"
            except Exception as e2:
                yield f"data: {json.dumps({'type': 'error', 'text': str(e2)})}\n\n"

    def generate():
        # Heartbeat wrapper. The deployed agent's GenieAgent node blocks for ~20s
        # while it polls Genie, during which _produce_events() yields nothing. An
        # idle SSE connection with no bytes for that long gets dropped by an
        # intermediary (browser/proxy/Apps gateway); the client stream fetch then
        # rejects, falls back to /api/agent/ask, and chokes on the gateway's HTML
        # error page ("Unexpected token '<', <!DOCTYPE ...").
        #
        # Fix: run the event producer in a worker thread feeding a queue, and emit
        # an SSE comment heartbeat (": ping" — ignored by the client's data:-only
        # parser) whenever no real event arrives within HEARTBEAT_SECS. The
        # connection never goes idle, so it is never dropped.
        HEARTBEAT_SECS = 5
        q: "queue.Queue" = queue.Queue()
        _DONE = object()

        def _pump():
            try:
                for chunk in _produce_events():
                    q.put(chunk)
            except Exception as e:  # producer should handle its own errors; last resort
                q.put(f"data: {json.dumps({'type': 'error', 'text': str(e)})}\n\n")
            finally:
                q.put(_DONE)

        t = threading.Thread(target=_pump, daemon=True)
        t.start()

        while True:
            try:
                item = q.get(timeout=HEARTBEAT_SECS)
            except queue.Empty:
                # No real event yet — keep the connection warm.
                yield ": ping\n\n"
                continue
            if item is _DONE:
                break
            yield item

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ═══════════════════════════════════════════════════════════════════════════
# Agent Traces (Inference Table + MLflow Fallback)
# ═══════════════════════════════════════════════════════════════════════════

def _build_reasoning_chain(raw_spans):
    """Transform raw MLflow spans into a narrative reasoning story for the UI.

    Instead of showing every raw span, we produce high-level story beats:
      1. Supervisor analyzes the question
      2. Supervisor decides to consult [Agent] because [reason]
      3. [Agent] queries the Genie space (took Xs)
      4. Supervisor synthesizes the final answer

    Multiple rounds of the same agent are collapsed into one beat.
    """
    if not raw_spans:
        return []

    # Agent friendly names and domain descriptions
    AGENT_DESCRIPTIONS = {
        "FieldOpsGenie": ("Field Service Operations", "work orders, technicians, dispatch, and scheduling"),
        "PostgresAdminGenie": ("PostgresAdmin", "database health, connections, query performance, and replication"),
        "NetworkHealthGenie": ("Network Health", "network nodes, outages, IoT telemetry, and maintenance risk"),
        "SLAWorkforceGenie": ("SLA & Workforce", "SLA compliance, technician performance, and regional analytics"),
    }

    # Parse spans into structured data
    span_data = []
    for s in raw_spans:
        name = s.get("name", "")
        attrs = s.get("attributes", {})
        span_type = attrs.get("mlflow.spanType", "")
        status = s.get("status", {})
        status_code = status.get("status_code", "OK") if isinstance(status, dict) else str(status)
        start_ns = s.get("start_time_unix_nano", 0)
        end_ns = s.get("end_time_unix_nano", 0)
        duration_ms = round((end_ns - start_ns) / 1e6) if start_ns and end_ns else 0

        # Token usage
        tokens = {}
        try:
            tokens = json.loads(attrs.get("mlflow.chat.tokenUsage", "{}"))
        except Exception:
            pass

        # Extract LLM output content
        output_text = ""
        sql_query = ""
        if span_type == "CHAT_MODEL":
            try:
                out = json.loads(attrs.get("mlflow.spanOutputs", "{}"))
                if isinstance(out, dict):
                    output_text = out.get("content", "") or ""
                    for tc in out.get("tool_calls", []):
                        if isinstance(tc, dict) and tc.get("name", "").startswith("transfer_to_"):
                            output_text = tc["name"]
            except Exception:
                pass

        # Extract SQL from Genie spans
        if name in ("ask_question", "get_query_result", "execute_query"):
            try:
                out = json.loads(attrs.get("mlflow.spanOutputs", "{}"))
                if isinstance(out, str):
                    if "SELECT" in out.upper():
                        sql_query = out.strip()
                elif isinstance(out, dict):
                    sql_query = out.get("query", "") or out.get("sql", "") or ""
                    if not sql_query:
                        desc = out.get("description", "")
                        if desc:
                            output_text = desc
            except Exception:
                pass
            # Also check span inputs for SQL
            if not sql_query:
                try:
                    inp = json.loads(attrs.get("mlflow.spanInputs", "{}"))
                    if isinstance(inp, dict):
                        sql_query = inp.get("query", "") or inp.get("sql", "") or ""
                except Exception:
                    pass

        span_data.append({
            "name": name, "type": span_type, "status": status_code,
            "duration_ms": duration_ms, "tokens": tokens, "output": output_text,
            "sql": sql_query,
        })

    # Build narrative from span data
    story = []
    total_llm_tokens = 0
    agents_seen = []
    agent_query_count = {}
    agent_total_time = {}
    agent_errors = {}
    agent_sql = {}
    supervisor_llm_calls = 0
    genie_total_time = 0

    # First pass: gather statistics
    for sd in span_data:
        if sd["type"] == "CHAT_MODEL":
            total_llm_tokens += sd["tokens"].get("total_tokens", 0)
            supervisor_llm_calls += 1

        # Track agent routing (transfer_to_* tool calls)
        if sd["name"].startswith("transfer_to_"):
            agent_key = sd["name"].replace("transfer_to_", "")
            for real_name in AGENT_DESCRIPTIONS:
                if real_name.lower() == agent_key:
                    agent_key = real_name
                    break
            if agent_key not in agents_seen:
                agents_seen.append(agent_key)
            agent_query_count[agent_key] = agent_query_count.get(agent_key, 0) + 1

        # Track Genie query time and SQL
        if sd["name"] == "ask_question":
            genie_total_time += sd["duration_ms"]
            if agents_seen:
                last_agent = agents_seen[-1]
                agent_total_time[last_agent] = agent_total_time.get(last_agent, 0) + sd["duration_ms"]
                if sd["status"] != "OK":
                    agent_errors[last_agent] = True
                if sd.get("sql") and last_agent not in agent_sql:
                    agent_sql[last_agent] = sd["sql"]

    # Calculate total duration from root span
    total_duration_ms = 0
    for sd in span_data:
        if sd["name"] in ("predict", "predict_stream"):
            total_duration_ms = sd["duration_ms"]
            break

    # Step 1: Supervisor receives the question
    story.append({
        "type": "narration", "icon": "brain",
        "text": "Supervisor analyzed the question and evaluated which specialized agents could best answer it.",
        "detail": f"Considered {len(AGENT_DESCRIPTIONS)} available agents",
        "duration_ms": None,
    })

    # Step 2: For each agent consulted
    for agent_key in agents_seen:
        friendly_name, domain = AGENT_DESCRIPTIONS.get(agent_key, (agent_key, ""))
        query_count = agent_query_count.get(agent_key, 1)
        query_time = agent_total_time.get(agent_key, 0)
        had_error = agent_errors.get(agent_key, False)
        time_str = f"{query_time / 1000:.1f}s" if query_time else ""

        # Routing decision
        story.append({
            "type": "routing", "icon": "route",
            "text": f"Decided to consult **{friendly_name}** agent",
            "detail": f"This agent specializes in {domain}" if domain else "",
            "duration_ms": None,
        })

        # Agent execution result
        sql = agent_sql.get(agent_key, "")
        if had_error:
            story.append({
                "type": "error", "icon": "alert",
                "text": f"{friendly_name} encountered a permission error while querying",
                "detail": f'{query_count} {"queries" if query_count > 1 else "query"} attempted' + (f" over {time_str}" if time_str else ""),
                "duration_ms": query_time, "sql": sql,
            })
        else:
            plural = f"Ran {query_count} queries" if query_count > 1 else "Queried the Genie space"
            story.append({
                "type": "agent", "icon": "search",
                "text": f"{friendly_name} retrieved data successfully",
                "detail": f"{plural}" + (f" in {time_str}" if time_str else ""),
                "duration_ms": query_time, "sql": sql,
            })

    # Step 3: Synthesis
    if agents_seen:
        if len(agents_seen) > 1:
            agent_names = [AGENT_DESCRIPTIONS.get(a, (a, ""))[0] for a in agents_seen]
            story.append({
                "type": "narration", "icon": "merge",
                "text": f"Supervisor combined results from {len(agents_seen)} agents into a unified answer",
                "detail": ", ".join(agent_names),
                "duration_ms": None,
            })
        else:
            story.append({
                "type": "narration", "icon": "compose",
                "text": "Supervisor composed the final answer from the agent's data",
                "detail": None, "duration_ms": None,
            })

    # Step 4: Summary stats footer
    story.append({
        "type": "summary", "icon": "stats",
        "text": "Execution complete",
        "stats": {
            "total_time": f"{total_duration_ms / 1000:.1f}s" if total_duration_ms else None,
            "llm_calls": supervisor_llm_calls,
            "total_tokens": total_llm_tokens,
            "genie_queries": sum(agent_query_count.values()),
            "genie_time": f"{genie_total_time / 1000:.1f}s" if genie_total_time else None,
        },
    })

    return story


def _agent_traces_mlflow_fallback(w, limit):
    """Fallback: use MLflow traces REST API when inference table is unavailable.

    Returns simplified trace metadata without full span details.
    """
    endpoint_name = _get_agent_endpoint()
    experiment_id = os.environ.get("AGENT_MLFLOW_EXPERIMENT_ID", "")
    if not experiment_id:
        try:
            ep = w.serving_endpoints.get(endpoint_name)
            served = ep.config.served_entities[0] if ep.config and ep.config.served_entities else None
            if served and served.environment_vars:
                experiment_id = served.environment_vars.get("MLFLOW_EXPERIMENT_ID", "")
        except Exception:
            pass

    if not experiment_id:
        return jsonify({"traces": [], "error": "No MLflow experiment configured"})

    data = w.api_client.do("GET", "/api/2.0/mlflow/traces", query={
        "experiment_ids": experiment_id,
        "max_results": str(limit),
    })

    traces = data.get("traces", []) if isinstance(data, dict) else []
    simplified = []
    for trace in traces:
        meta = {}
        for m in trace.get("request_metadata", []):
            meta[m.get("key", "")] = m.get("value", "")
        token_usage = json.loads(meta.get("mlflow.trace.tokenUsage", "{}")) if meta.get("mlflow.trace.tokenUsage") else {}
        span_stats = json.loads(meta.get("mlflow.trace.sizeStats", "{}")) if meta.get("mlflow.trace.sizeStats") else {}

        simplified.append({
            "request_id": trace.get("request_id", ""),
            "execution_time_ms": trace.get("execution_time_ms", 0),
            "status": trace.get("status", ""),
            "num_spans": span_stats.get("num_spans", 0),
            "total_tokens": token_usage.get("total_tokens", 0),
            "input_tokens": token_usage.get("input_tokens", 0),
            "output_tokens": token_usage.get("output_tokens", 0),
            "reasoning_chain": [],
            "source": "mlflow_api",
        })

    return jsonify({"traces": simplified, "source": "mlflow_api"})


@genie_agent_bp.route("/api/agent/traces", methods=["GET"])
def agent_traces():
    """Retrieve agent reasoning traces from the inference table.

    The inference table stores the full MLflow trace (including all spans)
    in the response column under ``databricks_output.trace``. This gives us
    the complete reasoning chain: supervisor routing, agent execution,
    LLM calls, tool invocations, and token usage per step.

    Falls back to the MLflow REST API if the inference table is unavailable
    or the SQL Warehouse is not configured.
    """
    try:
        limit = request.args.get("limit", "1", type=int)
        request_id = request.args.get("request_id", "")
        w = get_workspace_client()
        endpoint_name = _get_agent_endpoint()

        if not endpoint_name:
            return jsonify({"traces": []})

        # Query inference table via SQL Statement Execution API
        warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "") or os.environ.get("WAREHOUSE_ID", "")
        if not warehouse_id:
            return _agent_traces_mlflow_fallback(w, limit)

        # Backtick-quote each part for hyphenated identifiers
        table_parts = AGENT_INFERENCE_TABLE.split(".")
        quoted_table = ".".join(f"`{p}`" for p in table_parts)

        if request_id:
            # Validate request_id format (UUID-like)
            if not re.match(r"^[a-zA-Z0-9_-]+$", request_id):
                return jsonify({"error": "Invalid request_id"}), 400
            sql = f"""
                SELECT databricks_request_id, request_time, execution_duration_ms,
                       request, response
                FROM {quoted_table}
                WHERE databricks_request_id = '{request_id}'
                LIMIT 1
            """
        else:
            sql = f"""
                SELECT databricks_request_id, request_time, execution_duration_ms,
                       request, response
                FROM {quoted_table}
                WHERE status_code = 200
                ORDER BY request_time DESC
                LIMIT {min(limit, 10)}
            """

        try:
            stmt_resp = w.api_client.do("POST", "/api/2.0/sql/statements", body={
                "warehouse_id": warehouse_id,
                "statement": sql,
                "wait_timeout": "30s",
            })
        except Exception as e:
            log.warning(f"Inference table query failed: {e}")
            return _agent_traces_mlflow_fallback(w, limit)

        status = stmt_resp.get("status", {}).get("state", "")
        if status != "SUCCEEDED":
            err_msg = stmt_resp.get("status", {}).get("error", {}).get("message", "")
            log.warning(f"Inference table SQL status={status}: {err_msg}")
            return _agent_traces_mlflow_fallback(w, limit)

        columns = [c["name"] for c in stmt_resp.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = stmt_resp.get("result", {}).get("data_array", [])

        traces = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            response_str = row_dict.get("response", "{}")
            try:
                response_data = json.loads(response_str) if isinstance(response_str, str) else response_str
            except (json.JSONDecodeError, TypeError):
                continue

            # Extract trace from databricks_output
            db_output = response_data.get("databricks_output", {})
            trace_data = db_output.get("trace", {})
            trace_info = trace_data.get("info", {})
            raw_spans = trace_data.get("data", {}).get("spans", [])

            # Extract token usage from trace metadata
            trace_metadata = trace_info.get("trace_metadata", {})
            if isinstance(trace_metadata, list):
                meta_dict = {}
                for m in trace_metadata:
                    meta_dict[m.get("key", "")] = m.get("value", "")
                trace_metadata = meta_dict

            token_usage = {}
            try:
                token_usage = json.loads(trace_metadata.get("mlflow.trace.tokenUsage", "{}"))
            except Exception:
                pass

            # Build the reasoning chain narrative from spans
            spans = _build_reasoning_chain(raw_spans)

            # Extract agents consulted from output
            agents_consulted = []
            for item in response_data.get("output", []):
                if isinstance(item, dict) and item.get("type") == "message":
                    for c in item.get("content", []):
                        text = c.get("text", "")
                        for m in re.findall(r"<agent>(\w+)</agent>", text):
                            if m not in ("supervisor", "__start__", "__end__") and m not in agents_consulted:
                                agents_consulted.append(m)

            traces.append({
                "request_id": row_dict.get("databricks_request_id", ""),
                "trace_id": trace_info.get("trace_id", ""),
                "timestamp": row_dict.get("request_time", ""),
                "execution_time_ms": trace_info.get("execution_duration_ms", row_dict.get("execution_duration_ms", 0)),
                "status": trace_info.get("status", "OK"),
                "total_tokens": token_usage.get("total_tokens", 0),
                "input_tokens": token_usage.get("input_tokens", 0),
                "output_tokens": token_usage.get("output_tokens", 0),
                "num_spans": len(raw_spans),
                "agents_consulted": agents_consulted,
                "reasoning_chain": spans,
                "request_preview": trace_info.get("request_preview", ""),
                "response_preview": trace_info.get("response_preview", ""),
            })

        return jsonify({"traces": traces, "source": "inference_table"})

    except Exception as e:
        log_error("agent_traces", e)
        return jsonify({"traces": [], "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════
# Agent Long-Term Memory (Lakebase PG)
# ═══════════════════════════════════════════════════════════════════════════


def _ensure_app_user(user: dict) -> None:
    """Auto-create app_user record on first interaction (upsert)."""
    if not user.get("email") or user["email"] == "anonymous":
        return
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_memory.app_users (user_id, display_name, last_seen)
                    VALUES (%s, %s, now())
                    ON CONFLICT (user_id) DO UPDATE SET last_seen = now()
                """, (user["email"], user.get("name", "")))
                conn.commit()
    except Exception as e:
        log.warning(f"Memory: could not upsert app_user: {e}")


def _ensure_conversation(conversation_id: str | None, question: str) -> str:
    """Get existing conversation or create a new one. Returns conversation_id."""
    if conversation_id:
        return conversation_id
    try:
        user = get_current_user()
        _ensure_app_user(user)
        user_id = user.get("email", "anonymous")

        pool = get_pool()
        title = question[:100] + ("..." if len(question) > 100 else "")
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ai_memory.conversations (title, user_id) VALUES (%s, %s) RETURNING conversation_id",
                    (title, user_id)
                )
                row = cur.fetchone()
                conn.commit()
        # A new conversation may push this user over the cap — prune the
        # overflow in the background so ai_memory stays bounded.
        _prune_user_conversations_async(user_id)
        return str(row[0])
    except Exception as e:
        log.warning(f"Memory: could not create conversation: {e}")
        return ""


def _prune_user_conversations(user_id: str) -> None:
    """Expire a user's conversations by both a count cap and an age limit.

    Two independent rules keep ai_memory bounded (like a consumer chat app):
      1. Count cap — keep only the MAX_CONVERSATIONS_PER_USER most-recent
         conversations (bounds heavy users).
      2. Age limit — delete anything older than MAX_CONVERSATION_AGE_DAYS by
         last activity (expires ancient threads even for light users;
         disabled when the value is 0).

    Messages are removed automatically by the ON DELETE CASCADE on
    ai_memory.messages. Best-effort — runs in a background thread and never
    blocks the request.
    """
    if not user_id:
        return
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Rule 1: cap — delete conversations beyond the N most recent
                # (ranked by last activity). CASCADE drops their messages.
                cur.execute("""
                    DELETE FROM ai_memory.conversations
                    WHERE conversation_id IN (
                        SELECT conversation_id
                        FROM ai_memory.conversations
                        WHERE user_id = %s
                        ORDER BY updated_at DESC
                        OFFSET %s
                    )
                """, (user_id, MAX_CONVERSATIONS_PER_USER))
                deleted_cap = cur.rowcount

                # Rule 2: age — expire threads inactive longer than the limit.
                deleted_age = 0
                if MAX_CONVERSATION_AGE_DAYS > 0:
                    cur.execute("""
                        DELETE FROM ai_memory.conversations
                        WHERE user_id = %s
                          AND updated_at < now() - make_interval(days => %s)
                    """, (user_id, MAX_CONVERSATION_AGE_DAYS))
                    deleted_age = cur.rowcount

                conn.commit()
        if deleted_cap or deleted_age:
            log.info(f"Memory: pruned conversations for {user_id} "
                     f"(cap={deleted_cap}, age={deleted_age})")
    except Exception as e:
        log.warning(f"Memory: conversation prune failed for {user_id}: {e}")


def _prune_user_conversations_async(user_id: str) -> None:
    """Fire the per-user conversation prune in a daemon thread (non-blocking)."""
    if not user_id:
        return
    threading.Thread(
        target=_prune_user_conversations, args=(user_id,), daemon=True
    ).start()


def _save_message(conversation_id: str, role: str, content: str,
                  spaces_consulted: list | None = None) -> bool:
    """Persist a message to the conversation history.

    Returns True only if the row was actually written. The UI reports a
    "Memory Persist — Saved Q&A to Lakebase ai_memory" step, and it used to show
    success unconditionally: when the app's PG role lacked rights on ai_memory,
    every write failed while the UI still claimed the Q&A had been saved.
    """
    if not conversation_id or not content:
        return False
    try:
        pool = get_pool()
        token_est = len(content) // 4
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_memory.messages
                        (conversation_id, role, content, spaces_consulted, token_estimate)
                    VALUES (%s, %s, %s, %s, %s)
                """, (conversation_id, role, content,
                      spaces_consulted if spaces_consulted else None,
                      token_est))
                conn.commit()
        return True
    except Exception as e:
        log.warning(f"Memory: could not save message: {e}")
        return False


def _build_input_with_history(conversation_id: str, current_question: str,
                              max_pairs: int = 4, max_tokens: int = 2500) -> list[dict]:
    """Build the input message array with conversation history.

    Returns a list of {role, content} dicts suitable for the Model Serving
    input format. Includes the conversation summary (if any), recent messages
    (up to max_pairs Q&A pairs within max_tokens), and the current question.

    Defaults are deliberately small. Every injected pair inflates the input to
    *both* supervisor calls — the routing decision and the final synthesis — and
    routing needs almost none of it to choose among four agents. Measured: ~17.5s
    elapsed before the first routing event. Four pairs is ample for follow-up
    questions ("what about the Northeast?") without paying for a long tail of
    history on every request.
    """
    input_msgs: list[dict] = []

    if not conversation_id:
        return [{"role": "user", "content": current_question}]

    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get conversation summary
                cur.execute(
                    "SELECT summary FROM ai_memory.conversations WHERE conversation_id = %s",
                    (conversation_id,)
                )
                row = cur.fetchone()
                summary = row[0] if row and row[0] else None

                # Get recent messages (exclude the current question we just saved)
                cur.execute("""
                    SELECT role, content, token_estimate
                    FROM ai_memory.messages
                    WHERE conversation_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (conversation_id, max_pairs * 2 + 1))
                rows = list(reversed(cur.fetchall()))

                # Remove the last user message (it's the current question we just persisted)
                if rows and rows[-1][0] == 'user' and rows[-1][1] == current_question:
                    rows = rows[:-1]

        # Inject summary as system message if available
        if summary:
            input_msgs.append({
                "role": "user",
                "content": f"[Context from earlier in this conversation: {summary}]"
            })

        # Add history messages, respecting token budget
        total_tokens = 0
        history_msgs = []
        for role, content, tok_est in rows:
            tokens = tok_est or (len(content) // 4)
            if total_tokens + tokens > max_tokens:
                break
            history_msgs.append({"role": role, "content": content})
            total_tokens += tokens

        input_msgs.extend(history_msgs)

    except Exception as e:
        log.warning(f"Memory: could not fetch history: {e}")

    # Always append the current question
    input_msgs.append({"role": "user", "content": current_question})
    return input_msgs


# ── Conversation CRUD Endpoints ──────────────────────────────────────────


@genie_agent_bp.route("/api/agent/conversations")
def agent_conversations():
    """List recent conversations for the current user (admin sees all)."""
    try:
        limit = min(int(request.args.get("limit", 20)), 50)
        offset = int(request.args.get("offset", 0))
        user = get_current_user()
        user_id = user.get("email", "anonymous")

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Check if user is admin
                is_admin = False
                try:
                    cur.execute("SELECT role FROM ai_memory.app_users WHERE user_id = %s", (user_id,))
                    row = cur.fetchone()
                    is_admin = row and row[0] == 'admin'
                except Exception as e:
                    # A failed statement aborts the entire Postgres transaction —
                    # every later command then fails with "current transaction is
                    # aborted, commands ignored until end of transaction block".
                    # Swallowing the error without rolling back therefore broke the
                    # conversation list too, long after the admin check itself
                    # stopped mattering. Roll back so the rest of the request runs.
                    conn.rollback()
                    log.warning(f"Memory: admin check failed, treating as non-admin: {e}")

                if is_admin:
                    cur.execute("""
                        SELECT c.conversation_id, c.title, c.created_at, c.updated_at,
                               c.message_count, c.user_id
                        FROM ai_memory.conversations c
                        ORDER BY c.updated_at DESC
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                else:
                    cur.execute("""
                        SELECT c.conversation_id, c.title, c.created_at, c.updated_at,
                               c.message_count, c.user_id
                        FROM ai_memory.conversations c
                        WHERE c.user_id = %s OR c.user_id IS NULL
                        ORDER BY c.updated_at DESC
                        LIMIT %s OFFSET %s
                    """, (user_id, limit, offset))

                convs = [{
                    "conversation_id": str(r[0]),
                    "title": r[1],
                    "created_at": str(r[2]),
                    "updated_at": str(r[3]),
                    "message_count": r[4],
                    "user_id": r[5],
                } for r in cur.fetchall()]

                cur.execute("SELECT COUNT(*) FROM ai_memory.conversations WHERE user_id = %s OR user_id IS NULL", (user_id,))
                total = cur.fetchone()[0]

        return jsonify({"conversations": convs, "total": total, "user": user})
    except Exception as e:
        log_error("agent_conversations", e)
        return jsonify({"conversations": [], "error": str(e)})


@genie_agent_bp.route("/api/agent/conversations", methods=["POST"])
def agent_create_conversation():
    """Create a new conversation."""
    try:
        data = request.json or {}
        title = data.get("title", "New conversation")

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ai_memory.conversations (title) VALUES (%s) RETURNING conversation_id",
                    (title,)
                )
                conv_id = str(cur.fetchone()[0])
                conn.commit()

        return jsonify({"conversation_id": conv_id, "title": title})
    except Exception as e:
        log_error("agent_create_conversation", e)
        return jsonify({"error": str(e)}), 500


@genie_agent_bp.route("/api/agent/conversations/<conv_id>")
def agent_get_conversation(conv_id):
    """Get a conversation with all its messages."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT conversation_id, title, created_at, updated_at, message_count, summary
                    FROM ai_memory.conversations
                    WHERE conversation_id = %s
                """, (conv_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Conversation not found"}), 404

                conv = {
                    "conversation_id": str(row[0]),
                    "title": row[1],
                    "created_at": str(row[2]),
                    "updated_at": str(row[3]),
                    "message_count": row[4],
                    "summary": row[5],
                }

                cur.execute("""
                    SELECT message_id, role, content, spaces_consulted, created_at
                    FROM ai_memory.messages
                    WHERE conversation_id = %s
                    ORDER BY created_at
                """, (conv_id,))
                conv["messages"] = [{
                    "message_id": str(r[0]),
                    "role": r[1],
                    "content": r[2],
                    "spaces_consulted": r[3],
                    "created_at": str(r[4]),
                } for r in cur.fetchall()]

        return jsonify(conv)
    except Exception as e:
        log_error("agent_get_conversation", e)
        return jsonify({"error": str(e)}), 500


@genie_agent_bp.route("/api/agent/conversations/<conv_id>", methods=["DELETE"])
def agent_delete_conversation(conv_id):
    """Delete a conversation and all its messages (cascade)."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ai_memory.conversations WHERE conversation_id = %s",
                    (conv_id,)
                )
                deleted = cur.rowcount
                conn.commit()

        return jsonify({"deleted": deleted > 0})
    except Exception as e:
        log_error("agent_delete_conversation", e)
        return jsonify({"error": str(e)}), 500


@genie_agent_bp.route("/api/agent/user")
def agent_user_info():
    """Return the current logged-in user's identity and role. Must be fast (<2s)."""
    user = get_current_user()
    user_id = user.get("email", "anonymous")
    role = "user"
    role_source = "default"

    # Method 1: Check Databricks workspace groups (governance-driven)
    group_role = get_role_from_groups(user_id) if user_id != "anonymous" else None
    if group_role:
        role = group_role
        role_source = "databricks_group"

    # Method 2: Fall back to app_users.role in Lakebase
    if role_source == "default":
        try:
            pool = get_pool()
            with pool.connection(timeout=3) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = '2000'")
                    cur.execute("SELECT role FROM ai_memory.app_users WHERE user_id = %s", (user_id,))
                    row = cur.fetchone()
                    if row:
                        role = row[0]
                        role_source = "lakebase_app_users"
        except Exception:
            pass

    # Get group names for display
    groups = get_user_databricks_groups(user_id) if user_id != "anonymous" else []

    result = {
        "email": user_id,
        "name": user.get("name", ""),
        "role": role,
        "role_source": role_source,
        "databricks_groups": groups,
    }

    # Debug info only in dev mode
    if os.environ.get("FLASK_DEBUG", "").lower() == "true":
        try:
            result["_debug"] = {
                "auth_header_present": bool(request.headers.get("Authorization", "")),
                "forwarded_headers": {
                    k: v for k, v in request.headers
                    if k.lower().startswith("x-forwarded") or k.lower().startswith("x-real")
                },
            }
        except Exception:
            pass

    return jsonify(result)


@genie_agent_bp.route("/api/agent/memory/search")
def agent_memory_search():
    """Full-text search across all conversation messages."""
    try:
        query = request.args.get("q", "")
        if not query:
            return jsonify({"results": [], "error": "Missing ?q= parameter"}), 400

        limit = min(int(request.args.get("limit", 10)), 50)

        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM ai_memory.search_memory(%s, %s)",
                    (query, limit)
                )
                results = [{
                    "conversation_id": str(r[0]),
                    "conversation_title": r[1],
                    "message_id": str(r[2]),
                    "role": r[3],
                    "content": r[4][:300],
                    "created_at": str(r[5]),
                    "relevance": round(float(r[6]), 4),
                } for r in cur.fetchall()]

        return jsonify({"query": query, "results": results})
    except Exception as e:
        log_error("agent_memory_search", e)
        return jsonify({"results": [], "error": str(e)})

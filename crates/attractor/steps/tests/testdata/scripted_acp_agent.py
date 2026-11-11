# A scripted ACP agent that speaks the parts of the protocol the real
# products (Claude Code through `claude-code-acp`, Gemini CLI through
# `gemini --acp`) use beyond a plain text turn: a permission request for a
# tool call, `tool_call` and `tool_call_update` reports, a thought chunk, a
# plan, the session usage extension (`usage_update` and the `usage` a prompt
# response carries), and `authenticate` before `session/new`. Petri's own
# test data (`crates/fabro/acceptance/testdata/fake_acp_agent.py` is Fabro's
# copy and stays as pinned).
#
# `ACP_MODE` selects the script:
#   tools (default)  one prompt turn: a thought, a plan, an `edit` tool call
#                    that asks permission (allowed: the file is written and
#                    the call completes; rejected: the call fails), a `read`
#                    tool call that never asks, a usage update, the answer
#                    text, and a prompt response with usage.
#   auth             `session/new` is refused with `auth_required` until the
#                    client authenticates with the advertised API-key
#                    method; then a plain text turn.
#   subagents        codex-acp's native sub-agent sessions: the root
#                    announces a child (`subagent_spawned`), the child
#                    streams a tool call with a permission request, a usage
#                    update and text under its own session, the root ends
#                    it (`subagent_state_update`), reports its own usage and
#                    answers; a last child usage update follows.
#
# Records, each written when the variable is set:
#   ACP_PERMISSION          the `result` of the permission answer, as JSON
#   ACP_AUTH_RECORD         the `methodId` the client authenticated with
#   ACP_ENV_RECORD          the agent's values of ACP_ENV_RECORD_KEYS, as JSON
#   ACP_SESSION_NEW_PARAMS  the `session/new` params, as JSON
#   ACP_INITIALIZE_PARAMS   the `initialize` params, as JSON
#   ACP_WRITE_PATH          the file the edit tool writes (default hello.txt)
import json
import os
import sys

session_id = "sess-1"
authenticated = False

if os.environ.get("ACP_ENV_RECORD"):
    keys = [
        key.strip()
        for key in os.environ.get(
            "ACP_ENV_RECORD_KEYS",
            "ANTHROPIC_API_KEY,GEMINI_API_KEY,OPENAI_API_KEY,AGENT_KEY",
        ).split(",")
        if key.strip()
    ]
    snapshot = {key: os.environ[key] for key in keys if key in os.environ}
    with open(os.environ["ACP_ENV_RECORD"], "w", encoding="utf-8") as record:
        record.write(json.dumps(snapshot, sort_keys=True))
    # The value reaches the agent's own output too: Petri masks it there.
    for key, value in snapshot.items():
        print(f"{key}={value}", file=sys.stderr, flush=True)


def send(message):
    print(json.dumps(message), flush=True)


def respond(message, result):
    send({"jsonrpc": "2.0", "id": message["id"], "result": result})


def update(body, session=None):
    send({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"sessionId": session or session_id, "update": body},
    })


def text_chunk(kind, text, session=None):
    update({"sessionUpdate": kind, "content": {"type": "text", "text": text}}, session)


def record(variable, value):
    if os.environ.get(variable):
        with open(os.environ[variable], "w", encoding="utf-8") as file:
            file.write(value)


def tools_turn(message):
    text_chunk("agent_thought_chunk", "I should write the file, then read the readme.")
    update({
        "sessionUpdate": "plan",
        "entries": [
            {"content": "Write hello.txt", "priority": "high", "status": "in_progress"},
            {"content": "Read README", "priority": "low", "status": "pending"},
        ],
    })
    path = os.environ.get("ACP_WRITE_PATH", "hello.txt")
    content = "hello from acp\n"
    update({
        "sessionUpdate": "tool_call",
        "toolCallId": "call-1",
        "title": "Write hello.txt",
        "kind": "edit",
        "status": "pending",
        "rawInput": {"path": path, "content": content},
    })
    send({
        "jsonrpc": "2.0",
        "id": "permission-1",
        "method": "session/request_permission",
        "params": {
            "sessionId": session_id,
            "toolCall": {"toolCallId": "call-1", "title": "Write hello.txt", "kind": "edit"},
            "options": [
                {"optionId": "reject", "name": "Reject", "kind": "reject_once"},
                {"optionId": "once", "name": "Allow once", "kind": "allow_once"},
                {"optionId": "always", "name": "Allow always", "kind": "allow_always"},
            ],
        },
    })
    answer = json.loads(sys.stdin.readline())
    result = answer.get("result", {})
    record("ACP_PERMISSION", json.dumps(result, separators=(",", ":")))
    outcome = result.get("outcome", {})
    allowed = outcome.get("outcome") == "selected" and outcome.get("optionId") in ("once", "always")
    if allowed:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w", encoding="utf-8") as file:
            file.write(content)
        update({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "call-1",
            "status": "completed",
            "content": [{"type": "content", "content": {"type": "text", "text": f"wrote {path}"}}],
            "rawOutput": {"bytes": len(content)},
        })
    else:
        update({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "call-1",
            "status": "failed",
            "rawOutput": "permission denied",
        })
    # A read the agent treats as safe: it runs without asking.
    update({
        "sessionUpdate": "tool_call",
        "toolCallId": "call-2",
        "title": "Read README",
        "kind": "read",
        "status": "in_progress",
    })
    update({
        "sessionUpdate": "tool_call_update",
        "toolCallId": "call-2",
        "status": "completed",
        "rawOutput": "# readme",
    })
    update({
        "sessionUpdate": "usage_update",
        "used": 1200,
        "size": 200000,
        "cost": {"amount": 0.0125, "currency": "USD"},
    })
    text_chunk("agent_message_chunk", "done")
    respond(message, {
        "stopReason": "end_turn",
        "usage": {
            "totalTokens": 175,
            "inputTokens": 100,
            "outputTokens": 50,
            "thoughtTokens": 5,
            "cachedReadTokens": 20,
        },
    })


def subagents_turn(message):
    child = "sess-child"
    update({
        "sessionUpdate": "subagent_spawned",
        "subagentSessionId": child,
        "name": "Reviewer",
        "task": "Find the TODOs.",
    })
    update({
        "sessionUpdate": "tool_call",
        "toolCallId": "call-c1",
        "title": "rg TODO",
        "kind": "execute",
        "status": "pending",
    }, child)
    send({
        "jsonrpc": "2.0",
        "id": "permission-c1",
        "method": "session/request_permission",
        "params": {
            "sessionId": child,
            "toolCall": {"toolCallId": "call-c1", "title": "rg TODO", "kind": "execute"},
            "options": [{"optionId": "once", "name": "Allow once", "kind": "allow_once"}],
        },
    })
    sys.stdin.readline()
    update({"sessionUpdate": "tool_call_update", "toolCallId": "call-c1", "status": "completed"}, child)
    update({"sessionUpdate": "usage_update", "used": 90000, "size": 272000}, child)
    text_chunk("agent_message_chunk", "One TODO.", child)
    update({"sessionUpdate": "subagent_state_update", "subagentSessionId": child, "state": "completed"})
    update({"sessionUpdate": "usage_update", "used": 5000, "size": 272000})
    text_chunk("agent_message_chunk", "Reviewed.")
    # A late report from the child: it moves neither the answer nor the
    # stage's context window.
    update({"sessionUpdate": "usage_update", "used": 95000, "size": 272000}, child)
    respond(message, {"stopReason": "end_turn"})


def plain_turn(message):
    for text in ["hello ", "from acp"]:
        text_chunk("agent_message_chunk", text)
    respond(message, {"stopReason": "end_turn"})


mode = os.environ.get("ACP_MODE", "tools")

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        record("ACP_INITIALIZE_PARAMS", json.dumps(message.get("params", {}), separators=(",", ":")))
        result = {"protocolVersion": 1, "agentCapabilities": {}}
        if mode == "auth":
            result["authMethods"] = [
                {"id": "login", "name": "Log in"},
                {
                    "id": "scripted-api-key",
                    "name": "API key",
                    "_meta": {"api-key": {"provider": "scripted"}},
                },
            ]
        respond(message, result)
    elif method == "authenticate":
        method_id = message.get("params", {}).get("methodId")
        record("ACP_AUTH_RECORD", str(method_id))
        if method_id == "scripted-api-key" and os.environ.get("AGENT_KEY"):
            authenticated = True
            respond(message, {})
        else:
            send({
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32000, "message": "no key for that method"},
            })
    elif method == "session/new":
        record("ACP_SESSION_NEW_PARAMS", json.dumps(message.get("params", {}), separators=(",", ":")))
        if mode == "auth" and not authenticated:
            send({
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32000, "message": "Authentication required"},
            })
            continue
        respond(message, {"sessionId": session_id})
    elif method == "session/prompt":
        if mode == "tools":
            tools_turn(message)
        elif mode == "subagents":
            subagents_turn(message)
        else:
            plain_turn(message)
        break
    elif method == "session/cancel":
        continue
    else:
        send({
            "jsonrpc": "2.0",
            "id": message.get("id"),
            "error": {"code": -32601, "message": "method not found"},
        })

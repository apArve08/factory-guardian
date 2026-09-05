"""
Run the Factory Guardian agent once with a prompt and print its answer.
Handles the human-approval pause for the `remediate` tool (Stage 6).

Usage:
  PATH=/opt/homebrew/bin:$PATH PYTHONPATH=$PWD python scripts/ask.py "your prompt"
  echo yes | PATH=... python scripts/ask.py            # auto-approve remediation
"""

import asyncio
import sys

from dotenv import load_dotenv

load_dotenv()

from google.adk.runners import InMemoryRunner  # noqa: E402
from google.genai import types  # noqa: E402

from guardian.agent import root_agent  # noqa: E402

PROMPT = (
    sys.argv[1]
    if len(sys.argv) > 1
    else "The alert 'API latency > 5s (LINE-01)' is firing. Investigate, and if "
    "you find a fixable root cause, propose remediation and then verify recovery."
)

_CONFIRM = "adk_request_confirmation"


def _render(event):
    if not (event.content and event.content.parts):
        return
    for p in event.content.parts:
        if p.function_call and p.function_call.name != _CONFIRM:
            print(f"  → {p.function_call.name}({dict(p.function_call.args)})")
        elif p.function_response and p.function_response.name != _CONFIRM:
            print(f"  ← {str(p.function_response.response)[:200]}")
        elif p.text:
            print(p.text)


def _pending_confirmation(event):
    """Return (id, args) if this event is asking for tool approval."""
    lr = getattr(event, "long_running_tool_ids", None) or []
    if not (event.content and event.content.parts):
        return None
    for p in event.content.parts:
        fc = p.function_call
        if fc and fc.name == _CONFIRM and fc.id in lr:
            return fc.id, (fc.args or {})
    return None


async def main():
    runner = InMemoryRunner(agent=root_agent, app_name="factory-guardian")
    session = await runner.session_service.create_session(
        app_name="factory-guardian", user_id="cli"
    )

    msg = types.Content(role="user", parts=[types.Part(text=PROMPT)])
    while msg is not None:
        next_msg = None
        async for event in runner.run_async(
            user_id="cli", session_id=session.id, new_message=msg
        ):
            _render(event)
            pc = _pending_confirmation(event)
            if pc:
                fc_id, args = pc
                tc = args.get("toolConfirmation", {})
                orig = args.get("originalFunctionCall", {})
                print(f"\n[APPROVAL NEEDED] {orig.get('name', 'tool')}"
                      f"({orig.get('args', {})})")
                print(f"  {tc.get('hint', '')}")
                ans = input("  approve? [y/N]: ").strip().lower()
                confirmed = ans in ("y", "yes")
                next_msg = types.Content(
                    role="user",
                    parts=[types.Part(function_response=types.FunctionResponse(
                        id=fc_id, name=_CONFIRM,
                        response={"confirmed": confirmed},
                    ))],
                )
                print(f"  -> {'approved' if confirmed else 'rejected'}\n")
        msg = next_msg


if __name__ == "__main__":
    asyncio.run(main())

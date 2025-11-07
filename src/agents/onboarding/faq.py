import json
import re
import logging
from typing import List, Dict, Tuple

from .config import settings
import httpx
import uuid

log = logging.getLogger(__name__)

MCP_BASE = settings.MCP_BASE
MCP_JSONRPC_ENDPOINT = f"{MCP_BASE}/mcp/v1/jsonrpc"


async def _mcp_call(tool_name: str, arguments: dict, timeout: int = 10) -> dict:
    req_id = str(uuid.uuid4())
    payload = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(MCP_JSONRPC_ENDPOINT, json=payload)
        r.raise_for_status()
        resp = r.json()
        if "error" in resp:
            raise RuntimeError(resp["error"].get("message"))
        if "result" in resp and "content" in resp["result"]:
            content = resp["result"]["content"]
            if content:
                text = content[0].get("text", "")
                try:
                    return json.loads(text)
                except Exception:
                    return {"text": text}
        return resp.get("result", {})


def _load_kb() -> List[Dict]:
    import pathlib
    path = pathlib.Path(__file__).parent / "faqs.jsonl"
    entries: List[Dict] = []
    if not path.exists():
        return entries
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                continue
    return entries


KB = _load_kb()


QUESTION_RE = re.compile(r"\b(what|how|when|why|can|could|do|does|is|are|where|who|which|paid|pay|weekend|orientation|time|timings|device|commit)\b|\?", re.I)


def looks_like_question(text: str) -> bool:
    return bool(QUESTION_RE.search(text or ""))


def _score(q: str, entry: Dict) -> float:
    # Simple token overlap + phrase boosts
    ql = (q or "").lower()
    el = (entry.get("q") or "").lower() + " " + (entry.get("a") or "").lower()
    q_tokens = set(re.findall(r"[a-z0-9]+", ql))
    e_tokens = set(re.findall(r"[a-z0-9]+", el))
    overlap = len(q_tokens & e_tokens) / (len(q_tokens) + 1e-6)
    # Boost policy words
    boost = 0.0
    if any(w in el for w in ["weekday", "8", "15", "orientation", "device", "commit"]):
        boost += 0.1
    return overlap + boost


def retrieve(query: str, k: int = 3) -> List[Dict]:
    if not KB:
        return []
    scored = [(e, _score(query, e)) for e in KB]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [e for e, _ in scored[:k]]


async def compose_answer(query: str, context_entries: List[Dict]) -> str:
    """Use MCP faq.answer to compose a concise answer + bridge; fall back to KB text."""
    # Build a simple policy context for safety and clarity
    policy_ctx = (
        "weekday-only 8–15; 100% volunteer no pay; ~2 hrs/week split across days; smartphone or laptop acceptable"
    )
    try:
        resp = await _mcp_call(
            "faq.answer",
            {
                "question": query,
                "policy_context": policy_ctx,
                "kb_scope": "onboarding-basic",
                "top_k": 3,
            },
            timeout=10,
        )
        answer = (resp.get("answer") or "").strip()
        bridge = (resp.get("bridge") or "").strip()
        if answer and bridge:
            return f"{answer}\n{bridge}"
        if answer:
            return answer
    except Exception as e:
        log.warning(f"[FAQ] faq.answer failed, falling back to KB: {e}")
    # Fallback: pick the best KB entry and return its answer
    if context_entries:
        best = context_entries[0]
        return (best.get("a") or "").strip()
    return ""



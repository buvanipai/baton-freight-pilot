# api/agent.py

import json
import os

import anthropic
from dotenv import load_dotenv

load_dotenv()

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client

SYSTEM_PROMPT = """You are a logistics expert for a freight brokerage.
Given a shipment with an exception (delay, unresponsive carrier, anomaly), propose exactly 3 reroute options.
Consider ALL transport modes: truck, rail, air, water, multimodal.
Respond ONLY with a valid JSON array of exactly 3 objects, no prose, no markdown fences.
Each object must have these fields:
  carrier_alt (string): suggested alternative carrier name
  mode_alt (string): one of truck, rail, air, water, multimodal
  estimated_cost_delta_usd (float): cost change vs current (negative = cheaper)
  new_eta_hours (float): revised ETA in hours from now
  risk_level (string): one of low, medium, high
  reasoning (string): brief explanation
"""

FALLBACK = [
    {
        "carrier_alt": "XPO Logistics",
        "mode_alt": "truck",
        "estimated_cost_delta_usd": 0.0,
        "new_eta_hours": 48.0,
        "risk_level": "medium",
        "reasoning": "Standard fallback option — LLM unavailable.",
    },
    {
        "carrier_alt": "Union Pacific",
        "mode_alt": "rail",
        "estimated_cost_delta_usd": -500.0,
        "new_eta_hours": 72.0,
        "risk_level": "low",
        "reasoning": "Rail alternative — lower cost, longer transit.",
    },
    {
        "carrier_alt": "FedEx Freight",
        "mode_alt": "multimodal",
        "estimated_cost_delta_usd": 300.0,
        "new_eta_hours": 36.0,
        "risk_level": "high",
        "reasoning": "Expedited multimodal option — higher cost, faster delivery.",
    },
]


def _call_llm(prompt: str) -> str:
    message = _get_client().messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = message.content[0].text
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]  # remove first line
        raw = raw.rsplit("```", 1)[0]  # remove closing fence
    return raw.strip()


def propose_reroute(shipment_context: dict) -> list[dict]:
    prompt = (
        f"Shipment ID: {shipment_context.get('id')}\n"
        f"Origin: {shipment_context.get('origin_state')}\n"
        f"Destination: {shipment_context.get('destination_state')}\n"
        f"Current Mode: {shipment_context.get('mode')}\n"
        f"Commodity: {shipment_context.get('commodity')}\n"
        f"Status: {shipment_context.get('status')}\n"
        f"Carrier Status: {shipment_context.get('carrier_status')}\n"
        f"Freight Value USD: {shipment_context.get('freight_value_usd')}\n"
        f"Weight Tons: {shipment_context.get('weight_tons')}\n"
        f"Current ETA Hours: {shipment_context.get('eta_hours')}\n"
        f"Is Anomaly: {shipment_context.get('is_anomaly')}\n"
        f"Propose 3 reroute options."
    )

    for attempt in range(2):
        try:
            raw = _call_llm(prompt)
            print(f"[agent] Raw response: {repr(raw[:200])}") 
            options = json.loads(raw)
            if isinstance(options, list) and len(options) == 3:
                return options
        except (json.JSONDecodeError, Exception) as e:
            if attempt == 0:
                print(f"[agent] JSON parse failed (attempt 1), retrying: {e}")
            else:
                print(f"[agent] JSON parse failed (attempt 2), using fallback: {e}")

    return FALLBACK

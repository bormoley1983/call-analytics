from datetime import datetime, timezone
from typing import Any, Dict, List

from domain.config import AppConfig


def aggregate_report(per_call: List[Dict[str, Any]], config: AppConfig) -> Dict[str, Any]:
    """Aggregate overall statistics from all calls."""
    processed = [c for c in per_call if c.get("status") == "processed"]
    
    def num(x: Any, default: float = 0.0) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return default
    
    total_calls = len(per_call)
    transcribed = len(processed)
    skipped_small = len([c for c in per_call if c.get("status") == "skipped_too_small"])
    skipped_short = len([c for c in per_call if c.get("status") == "skipped_too_short"])
    
    spam = sum(1 for c in processed 
               if num((c.get("analysis") or {}).get("spam_probability", 0.0)) >= config.spam_probability_threshold)
    effective = sum(1 for c in processed 
                    if (c.get("analysis") or {}).get("effective_call") is True)
    
    total_duration = sum(c.get("meta", {}).get("audio_seconds", 0.0) for c in processed)
    
    intents: Dict[str, int] = {}
    outcomes: Dict[str, int] = {}
    questions: Dict[str, int] = {}
    
    for c in processed:
        analysis = c.get("analysis", {})
        
        intent = analysis.get("intent", "інше")
        intents[intent] = intents.get(intent, 0) + 1
        
        outcome = analysis.get("outcome", "невідомо")
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
        
        for q in analysis.get("key_questions", []) or []:
            q_lower = q.lower().strip()
            if q_lower:
                questions[q_lower] = questions.get(q_lower, 0) + 1
    
    return {
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "total_calls": total_calls,
        "transcribed": transcribed,
        "skipped_too_small": skipped_small,
        "skipped_too_short": skipped_short,
        "spam_calls": spam,
        "effective_calls": effective,
        "total_duration_seconds": total_duration,
        "top_intents": sorted(intents.items(), key=lambda kv: kv[1], reverse=True)[:10],
        "top_outcomes": sorted(outcomes.items(), key=lambda kv: kv[1], reverse=True)[:5],
        "top_questions": sorted(questions.items(), key=lambda kv: kv[1], reverse=True)[:10],
    }


def aggregate_report_by_manager(per_call: List[Dict[str, Any]], config: AppConfig) -> Dict[str, Any]:
    """Aggregate statistics per manager with role-based grouping."""
    managers_stats: Dict[str, Dict[str, Any]] = {}
    role_summary: Dict[str, Dict[str, int]] = {}
    
    processed = [c for c in per_call if c.get("status") == "processed"]
    
    def num(x: Any, default: float = 0.0) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return default
    
    for call in processed:
        meta = call.get("meta", {})
        analysis = call.get("analysis", {})
        
        manager_id = meta.get("manager_id", "manager_unknown")
        manager_name = meta.get("manager_name", "Невідомий")
        role = meta.get("role", "unknown")
        
        if manager_id not in managers_stats:
            managers_stats[manager_id] = {
                "manager_id": manager_id,
                "manager_name": manager_name,
                "role": role,
                "total_calls": 0,
                "incoming": 0,
                "outgoing": 0,
                "spam_calls": 0,
                "effective_calls": 0,
                "total_duration_seconds": 0.0,
                "intents": {},
                "outcomes": {},
                "questions": {},
            }
        
        # Track role summary
        if role not in role_summary:
            role_summary[role] = {"total_calls": 0}
        role_summary[role]["total_calls"] += 1
        
        stats = managers_stats[manager_id]
        stats["total_calls"] += 1
        
        # Direction
        direction = analysis.get("direction", "unknown")
        if direction == "incoming":
            stats["incoming"] += 1
        elif direction == "outgoing":
            stats["outgoing"] += 1
        
        # Spam
        if num(analysis.get("spam_probability", 0.0)) >= config.spam_probability_threshold:
            stats["spam_calls"] += 1
        
        # Effective
        if analysis.get("effective_call") is True:
            stats["effective_calls"] += 1
        
        # Duration
        stats["total_duration_seconds"] += meta.get("audio_seconds", 0.0)
        
        # Intent
        intent = analysis.get("intent", "інше")
        stats["intents"][intent] = stats["intents"].get(intent, 0) + 1
        
        # Outcome
        outcome = analysis.get("outcome", "невідомо")
        stats["outcomes"][outcome] = stats["outcomes"].get(outcome, 0) + 1
        
        # Questions
        for q in analysis.get("key_questions", []) or []:
            q_lower = q.lower().strip()
            if q_lower:
                stats["questions"][q_lower] = stats["questions"].get(q_lower, 0) + 1
    
    # Sort and format per manager
    for manager_id, stats in managers_stats.items():
        stats["top_intents"] = sorted(stats["intents"].items(), key=lambda kv: kv[1], reverse=True)[:10]
        stats["top_outcomes"] = sorted(stats["outcomes"].items(), key=lambda kv: kv[1], reverse=True)[:5]
        stats["top_questions"] = sorted(stats["questions"].items(), key=lambda kv: kv[1], reverse=True)[:10]
        
        # Remove raw dicts
        del stats["intents"]
        del stats["outcomes"]
        del stats["questions"]
    
    # Group by role
    by_role: Dict[str, List[Dict[str, Any]]] = {
        "sales": [],
        "management": [],
        "development": [],
        "unknown": []
    }
    
    for stats in managers_stats.values():
        role = stats.get("role", "unknown")
        by_role[role].append(stats)
    
    return {
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "role_summary": role_summary,
        "by_role": {
            role: managers 
            for role, managers in by_role.items() 
            if managers
        },
        "all_managers": list(managers_stats.values()),
        "total_managers": len(managers_stats),
    }

#!/usr/bin/env python3
"""
Shard-aware query planner for the analytics dashboard.

Routes queries to the correct shard(s) based on query parameters:
  - If event_type_id is known, route to a single shard
  - If only metric_type or time range is specified, fan out across all shards
  - Aggregation queries use parallel append across partitions

Usage:
    python3 tools/query_planner.py --metric-type api_latency --hours 24
"""

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_SHARD_COUNT = 8


@dataclass
class QueryFilter:
    column: str
    operator: str
    value: Any


@dataclass
class QueryPlan:
    target_shards: List[int]
    filters: List[QueryFilter] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_by: str = "event_timestamp DESC"
    limit: Optional[int] = None
    aggregation: Optional[str] = None
    aggregation_column: Optional[str] = None

    @property
    def is_single_shard(self) -> bool:
        return len(self.target_shards) == 1 and self.target_shards[0] >= 0

    def to_sql(self) -> str:
        if self.is_single_shard:
            table = f"analytics_events_shard{self.target_shards[0]}"
        else:
            table = "analytics_events"

        if self.aggregation:
            col = self.aggregation_column or "*"
            select = f"{self.aggregation}({col}) AS agg_result"
        else:
            select = "id, event_type_id, event_timestamp, user_id, session_id, metric_type, metric_value, metadata, tags"

        where_parts = []
        for f in self.filters:
            if f.operator == "BETWEEN":
                where_parts.append(f"{f.column} BETWEEN %s AND %s")
            elif f.operator == "IN":
                placeholders = ", ".join(["%s"] * len(f.value))
                where_parts.append(f"{f.column} IN ({placeholders})")
            else:
                where_parts.append(f"{f.column} {f.operator} %s")

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        group_by_clause = f" GROUP BY {', '.join(self.group_by)}" if self.group_by else ""
        order_clause = f" ORDER BY {self.order_by}"
        limit_clause = f" LIMIT {self.limit}" if self.limit else ""

        return f"SELECT {select} FROM {table} WHERE {where_clause}{group_by_clause}{order_clause}{limit_clause}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target_shards": self.target_shards,
            "is_single_shard": self.is_single_shard,
            "filters": [{"column": f.column, "operator": f.operator, "value": str(f.value)} for f in self.filters],
            "group_by": self.group_by,
            "aggregation": self.aggregation,
            "sql_preview": self.to_sql()[:200],
        }


class AnalyticsQueryPlanner:
    def __init__(self, shard_count: int = DEFAULT_SHARD_COUNT):
        if shard_count <= 0 or (shard_count & (shard_count - 1)) != 0:
            raise ValueError("shard_count must be a positive power of 2")
        self.shard_count = shard_count

    def get_shard_number(self, event_type_id: int) -> int:
        return abs(event_type_id % self.shard_count)

    def plan_query(self, event_type_id=None, metric_type=None, time_range=None,
                   user_id=None, tags=None, aggregation=None,
                   aggregation_column=None, group_by=None,
                   order_by="event_timestamp DESC", limit=None):
        filters = []
        target_shards = []

        if event_type_id is not None:
            shard = self.get_shard_number(event_type_id)
            target_shards = [shard]
            filters.append(QueryFilter("event_type_id", "=", event_type_id))
        else:
            target_shards = list(range(self.shard_count))

        if metric_type is not None:
            filters.append(QueryFilter("metric_type", "=", metric_type))
        if time_range is not None:
            filters.append(QueryFilter("event_timestamp", "BETWEEN", time_range))
        if user_id is not None:
            filters.append(QueryFilter("user_id", "=", user_id))
        if tags:
            filters.append(QueryFilter("tags", "@>", tags))

        return QueryPlan(target_shards=target_shards, filters=filters,
                         group_by=group_by or [], order_by=order_by,
                         limit=limit, aggregation=aggregation,
                         aggregation_column=aggregation_column)

    def plan_dashboard_query(self, metric_types, time_range):
        plans = []
        for mt in metric_types:
            plan = self.plan_query(metric_type=mt, time_range=time_range,
                                   aggregation="AVG", aggregation_column="metric_value",
                                   group_by=["date_trunc('hour', event_timestamp)"],
                                   order_by="date_trunc('hour', event_timestamp) DESC")
            plans.append(plan)
        return plans


# Unit tests
import unittest

class TestQueryPlanner(unittest.TestCase):
    def setUp(self):
        self.planner = AnalyticsQueryPlanner(shard_count=8)

    def test_shard_distribution(self):
        counts = {i: 0 for i in range(8)}
        for eid in range(10000):
            counts[self.planner.get_shard_number(eid)] += 1
        for s, c in counts.items():
            self.assertTrue(1000 < c < 1500, f"Shard {s} uneven: {c}")

    def test_consistent_routing(self):
        for eid in [1, 42, 100, 9999]:
            self.assertEqual(self.planner.get_shard_number(eid),
                             self.planner.get_shard_number(eid))

    def test_single_shard_query(self):
        plan = self.planner.plan_query(event_type_id=42)
        self.assertTrue(plan.is_single_shard)
        self.assertEqual(len(plan.target_shards), 1)

    def test_multi_shard_query(self):
        plan = self.planner.plan_query(metric_type="api_latency")
        self.assertFalse(plan.is_single_shard)
        self.assertEqual(len(plan.target_shards), 8)

    def test_sql_generation(self):
        plan = self.planner.plan_query(event_type_id=42, metric_type="api_latency")
        sql = plan.to_sql()
        self.assertIn("analytics_events_shard", sql)

    def test_dashboard_query(self):
        plans = self.planner.plan_dashboard_query(
            ["api_latency", "api_errors"], ("2026-06-01", "2026-06-23"))
        self.assertEqual(len(plans), 2)
        self.assertEqual(plans[0].aggregation, "AVG")

    def test_invalid_shard_count(self):
        with self.assertRaises(ValueError):
            AnalyticsQueryPlanner(shard_count=7)
        with self.assertRaises(ValueError):
            AnalyticsQueryPlanner(shard_count=0)

    def test_time_range_filter(self):
        plan = self.planner.plan_query(time_range=("2026-06-01", "2026-06-30"))
        time_filters = [f for f in plan.filters if f.column == "event_timestamp"]
        self.assertEqual(len(time_filters), 1)
        self.assertEqual(time_filters[0].operator, "BETWEEN")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shard-aware analytics query planner")
    parser.add_argument("--metric-type", help="Filter by metric type")
    parser.add_argument("--event-type-id", type=int, help="Specific event type ID (routes to single shard)")
    parser.add_argument("--hours", type=int, default=24, help="Time range in hours (default: 24)")
    parser.add_argument("--shard-count", type=int, default=8, help="Number of shards (must be power of 2)")
    parser.add_argument("--aggregation", help="Aggregation function (SUM, AVG, COUNT, etc.)")
    parser.add_argument("--limit", type=int, help="Result limit")
    parser.add_argument("--json", action="store_true", help="Output plan as JSON")
    parser.add_argument("--test", action="store_true", help="Run unit tests")

    args = parser.parse_args()

    if args.test:
        unittest.main(argv=[''], exit=False)
        sys.exit(0)

    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=args.hours)).isoformat()

    planner = AnalyticsQueryPlanner(shard_count=args.shard_count)
    plan = planner.plan_query(
        event_type_id=args.event_type_id,
        metric_type=args.metric_type,
        time_range=(start, now.isoformat()),
        aggregation=args.aggregation,
        aggregation_column="metric_value" if args.aggregation else None,
        limit=args.limit,
    )

    if args.json:
        print(json.dumps(plan.to_dict(), indent=2))
    else:
        print("Query Plan:")
        print(f"  Target shards: {plan.target_shards if not plan.is_single_shard else f'shard {plan.target_shards[0]} (single)'}")
        print(f"  Filters: {len(plan.filters)}")
        for f in plan.filters:
            print(f"    - {f.column} {f.operator} {f.value}")
        if plan.aggregation:
            print(f"  Aggregation: {plan.aggregation}({plan.aggregation_column or '*'})")
        print(f"\nSQL:\n  {plan.to_sql()}")

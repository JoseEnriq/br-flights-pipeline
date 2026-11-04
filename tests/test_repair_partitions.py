import pytest

from repair_partitions import WORKGROUP, run_query


class FakeAthena:
    """Answers with a fixed sequence of query states, so run_query runs without AWS."""

    def __init__(self, *states, reason=None):
        self.states = list(states)
        self.reason = reason
        self.started = []

    def start_query_execution(self, QueryString, WorkGroup):
        self.started.append((QueryString, WorkGroup))
        return {"QueryExecutionId": "q1"}

    def get_query_execution(self, QueryExecutionId):
        status = {"State": self.states.pop(0)}
        if self.reason:
            status["StateChangeReason"] = self.reason
        return {"QueryExecution": {"Status": status}}


def test_run_query_waits_until_the_statement_succeeds():
    athena = FakeAthena("QUEUED", "RUNNING", "SUCCEEDED")
    run_query(athena, "MSCK REPAIR TABLE br_flights.raw_vra", poll_seconds=0)
    assert athena.started == [("MSCK REPAIR TABLE br_flights.raw_vra", WORKGROUP)]
    assert athena.states == []


def test_run_query_raises_with_athena_reason_when_the_statement_fails():
    athena = FakeAthena("RUNNING", "FAILED", reason="Table not found")
    with pytest.raises(RuntimeError, match="FAILED: Table not found"):
        run_query(athena, "MSCK REPAIR TABLE br_flights.raw_vra", poll_seconds=0)

"""finalize_step_node verdict mapping and fan_in_node parallel merge."""


from graph_builder import make_fan_in_node, make_finalize_step_node


# ---------------------------------------------------------------------------
# finalize_step_node — final_status derivation + step_index advance
# ---------------------------------------------------------------------------

async def test_finalize_skipped(make_state):
    state = make_state(
        [{"step_id": "A"}],
        current_pre_check={"step_id": "A", "skip_step": True, "passed": True,
                           "raw_data": {}, "error": None},
    )
    out = await make_finalize_step_node()(state)
    assert out["step_index"] == 1
    assert out["step_records"][-1]["final_status"] == "skipped"


async def test_finalize_ok_from_validate(make_state):
    state = make_state(
        [{"step_id": "A"}],
        current_validate={"step_id": "A", "verdict": "ok", "spool_text": "",
                          "messages": [], "error_count": 0, "reasoning": ""},
    )
    out = await make_finalize_step_node()(state)
    assert out["step_records"][-1]["final_status"] == "ok"


async def test_finalize_failed_from_validate(make_state):
    state = make_state(
        [{"step_id": "A"}],
        current_validate={"step_id": "A", "verdict": "retry", "spool_text": "",
                          "messages": [], "error_count": 1, "reasoning": "x"},
    )
    out = await make_finalize_step_node()(state)
    assert out["step_records"][-1]["final_status"] == "failed"


async def test_finalize_defaults_ok_without_validate(make_state):
    state = make_state([{"step_id": "A"}])
    out = await make_finalize_step_node()(state)
    assert out["step_records"][-1]["final_status"] == "ok"
    # current_* are cleared for the next step
    assert out["current_validate"] is None
    assert out["current_execute"] is None


# ---------------------------------------------------------------------------
# fan_in_node — merge parallel results and skip the whole group
# ---------------------------------------------------------------------------

async def test_fan_in_merges_and_advances_past_group(make_state):
    steps = [
        {"step_id": "A", "group": "G"},
        {"step_id": "B", "group": "G"},
        {"step_id": "C", "group": None},
    ]
    state = make_state(steps, step_index=0)
    state["current_group"] = "G"
    state["parallel_step_ids"] = ["A", "B"]
    state["parallel_results"] = {
        "A": {"final_status": "ok", "pre_check": None, "execute": None,
              "poll": None, "validate": None},
        "B": {"final_status": "failed", "pre_check": None, "execute": None,
              "poll": None, "validate": None},
    }

    out = await make_fan_in_node()(state)

    # One record per parallel step, statuses preserved
    statuses = {r["step_id"]: r["final_status"] for r in out["step_records"]}
    assert statuses == {"A": "ok", "B": "failed"}
    # Index advanced past both grouped steps to the next sequential step (C @ 2)
    assert out["step_index"] == 2
    assert out["current_group"] is None
    assert out["parallel_results"] == {}

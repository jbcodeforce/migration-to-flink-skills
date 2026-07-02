"""Integration tests for offline and remote Flink SQL validation."""

import pytest

from flink_ref_fixtures import (
    assert_has_errors,
    assert_no_errors,
    load_flink_pair,
    validation_issues,
    REFERENCES_ROOT
)

pytestmark = pytest.mark.integration

def test_remote_valid_raw_classical_songs(require_deploy):
    src_dir = REFERENCES_ROOT / "flink" / "valid" / "raw_classical_songs"
    ddls, dmls, src_dir = load_flink_pair(src_dir)
    issues = validation_issues(ddls, dmls, remote=True)
    assert_no_errors(issues)


def test_remote_rejects_missing_pk(require_deploy):
    src_dir = REFERENCES_ROOT / "flink" / "invalid" / "ddl_missing_pk"
    ddls, dmls, src_dir = load_flink_pair(src_dir)
    issues = validation_issues(ddls, dmls, remote=True)
    assert_has_errors(issues, kind="ddl")




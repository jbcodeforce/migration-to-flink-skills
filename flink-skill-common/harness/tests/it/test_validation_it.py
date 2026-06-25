"""Integration tests for offline and remote Flink SQL validation."""

import pytest

from flink_fixtures import (
    assert_has_errors,
    assert_no_errors,
    load_pair,
    validation_issues,
)

pytestmark = pytest.mark.integration


def test_offline_valid_raw_classical_songs():
    ddls, dmls = load_pair("raw_classical_songs", valid=True)
    issues = validation_issues(ddls, dmls, remote=False)
    assert_no_errors(issues)


def test_offline_valid_watermark_metadata():
    ddls, dmls = load_pair("watermark_metadata", valid=True)
    issues = validation_issues(ddls, dmls, remote=False)
    assert_no_errors(issues)


def test_remote_valid_raw_classical_songs(require_deploy):
    ddls, dmls = load_pair("raw_classical_songs", valid=True)
    issues = validation_issues(ddls, dmls, remote=True)
    assert_no_errors(issues)


def test_offline_rejects_bad_syntax():
    ddls, dmls = load_pair("ddl_bad_syntax", valid=False)
    issues = validation_issues(ddls, dmls, remote=False)
    assert_has_errors(issues, kind="ddl")


def test_remote_rejects_missing_pk(require_deploy):
    ddls, dmls = load_pair("ddl_missing_pk", valid=False)
    issues = validation_issues(ddls, dmls, remote=True)
    assert_has_errors(issues, kind="ddl")


def test_offline_rejects_bad_dml():
    ddls, dmls = load_pair("dml_bad_syntax", valid=False)
    issues = validation_issues(ddls, dmls, remote=False)
    assert_has_errors(issues, kind="dml")

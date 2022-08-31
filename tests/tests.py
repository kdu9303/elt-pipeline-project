# -*- coding: utf-8 -*-
# Unit test of DAG
import os
import sys
import pytest
from airflow.models import DagBag

sys.path.append(os.path.join(os.path.dirname(__file__), "../dags"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../dags/modules"))
sys.path.append(
    os.path.join(os.path.dirname(__file__), "../dags/modules/newsscraper")
)

# Airflow variables called from DAGs under test are stubbed out
# os.environ["AIRFLOW_DATA_LAKE_BUCKET"] = ""


@pytest.fixture(params=["../dags/"])
def dag_bag(request):
    return DagBag(dag_folder=request.param, include_examples=False)


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors, "No Import Failures"


def test_three_or_less_retries(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args["retries"] <= 3

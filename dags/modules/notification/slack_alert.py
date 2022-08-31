# -*- coding: utf-8 -*-
import os
from airflow.providers.slack.operators.slack_webhook import (
    SlackWebhookOperator,
)


DAG_ID = os.path.basename(__file__).replace(".py", "")


def slack_failure_notification(context):
    slack_msg = f"""
            :red_circle: DAG Failed.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            """
    failed_alert = SlackWebhookOperator(
        task_id="slack_notification",
        http_conn_id="slack_webhook",
        message=slack_msg,
    )

    return failed_alert.execute(context=context)


def slack_success_notification(context):
    slack_msg = f"""
            :large_green_circle: DAG Succeeded.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            """
    success_alert = SlackWebhookOperator(
        task_id="slack_notification",
        http_conn_id="slack_webhook",
        message=slack_msg,
    )

    return success_alert.execute(context=context)

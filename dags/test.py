import json
import os
from functools import partial
import httpx
from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator
from httpx import RequestError

BASE_URL = "http://host.docker.internal:7778"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_agent/invoke"
openai_url = "/openai_chain/invoke"
openai_agent_without_tools_url = "/openai_agent_without_tools/invoke"

session_id = datetime.timestamp(datetime.now())

def post_request(
    post_url: str,
    formatter: callable,
    ti: any,
    data_to_send: dict = None,
):
    if not data_to_send:
        data_to_send = formatter(ti)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=600) as client:
        try:
            response = client.post(post_url, json=data_to_send)
            response.raise_for_status()
            data = response.json()
            return data
        except RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")

def task_PyOpr(
    task_id: str,
    callable_func,
    execution_timeout=timedelta(minutes=10),
    op_kwargs: dict = None,
):
    return PythonOperator(
        task_id=task_id,
        python_callable=callable_func,
        execution_timeout=execution_timeout,
        op_kwargs=op_kwargs,
    )

def agent_formatter(ti, prompt: str = "", task_ids: list = None, session_id: str = ""):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)
            task_output = data["output"]["output"]  # Output from agent
            task_output_with_id = (
                f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
            )
            results.append(task_output_with_id)
        content = "\n\n".join(results)  # Join all task results into a single string
        formatted_data = {
            "input": {"input": prompt + "\n\n INPUT:" + content},
            "config": {"configurable": {"session_id": session_id}},
        }
        return formatted_data
    else:
        pass

def openai_formatter(ti, prompt: str = None, task_ids: list = None):
    results = []
    for task_id in task_ids:
        data = ti.xcom_pull(task_ids=task_id)
        task_output = data["output"]["output"]  # Output from agent
        task_output_with_id = (
            f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
        )
        results.append(task_output_with_id)
    content = "\n\n".join(results)  # Join all task results into a single string
    if prompt:
        content = prompt + "\n\n" + content
    formatted_data = {"input": content}
    return formatted_data

def Test_merge(ti):
    result_0 = ti.xcom_pull(task_ids="test_summary")["output"]["output"]
    concatenated_result = (
        result_0
        + "\n\n"
    )
    return concatenated_result

test_summary_formatter = partial(
    agent_formatter,
    prompt= """
1. Emission reduction per vehicle(Both the title and the preceding serial number need to be retained)\n
Objective: Based on the information provided, output from the following 2 aspects.
(1)Current Emissions: Detail the current emissions CO2 emissions per car, measured in g/km, if it is g/mile, it needs to be converted into g/km.
(2)Emissions Reduction Target: Detail the target for reducing CO2 emissions per car, measured in g/km, if it is g/mile, it needs to be converted into g/km, and clarify the timepoint.\n

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "test",
    ],
    session_id=session_id,
)

agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
test_summary_agent = partial(post_request, post_url=openai_agent_without_tools_url, formatter=test_summary_formatter)

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="Test",
    default_args=default_args,
    description="test agent dag",
    schedule_interval=None,
    concurrency=1,
    tags=["test_agent"],
    catchup=False,
) as dag:

    # test task
    test_task = task_PyOpr(
        task_id="test",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "1. Provide a detailed description of the current CO2 emissions per car in the specified year, measured in g/km or g/mile. 2. Detail the target for reducing CO2 emissions per car, measured in g/km or g/mile, include the specific timepoint by which this target is to be achieved."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    # test summary task
    test_summary = PythonOperator(
        task_id="test_summary",
        python_callable=test_summary_agent,
        execution_timeout=timedelta(minutes=30),
        op_kwargs={}
    )

    # test output merge task
    test_output_merge = PythonOperator(
        task_id="test_output_merge",
        python_callable=Test_merge,
    )

    (
        [
            test_task,
        ]
        >> test_summary
        >> test_output_merge
    )

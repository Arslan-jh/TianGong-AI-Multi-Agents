import json
import os
from datetime import timedelta
from functools import partial
import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from httpx import RequestError

BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"
esg_automobile_url = "/esg_automobile_agent/invoke"
session_id = "20240222"

task_summary_prompt = """Construct a concise paragraph for each specified task, adhering to the MARKDOWN structure outlined below(change the title to the task ID):

    ### Scope_1_emissions_data
    Begin with a concluding statement regarding information disclosure, and elaborate with specific details in a continuous prose format.

    ### Scope_1_emissions_breakdown
    Begin with a concluding statement regarding information disclosure, and elaborate with specific details in a continuous prose format.
    ...
    (Proceed with further task IDs, providing their respective summaries in the same manner.)
    """

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
    # retries: int = 3,
    # retry_delay=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=10),
    op_kwargs: dict = None,
):
    return PythonOperator(
        task_id=task_id,
        python_callable=callable_func,
        # retries=retries,
        # retry_delay=retry_delay,
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

def openai_translate_formatter(ti, task_ids:list = None):
    results = []
    for task_id in task_ids:
        data = ti.xcom_pull(task_ids=task_id)
        task_output = str(data)
        results.append(task_output)
    content = "\n\n".join(results)
    prompt = "Translate the following text into native Chinese:" + "\n\n" + content
    formatted_data = {"input": prompt}
    return formatted_data

def Emissions_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Emissions_data_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Scope_emissions_data_overview")["output"]["output"]
    result_2 = ti.xcom_pull(task_ids="Scope_emissions_data_summary")["output"]["output"]
    result_3 = ti.xcom_pull(task_ids="Other_emissions_data_overview")["output"]["output"]
    result_4 = ti.xcom_pull(task_ids="Other_emissions_data_summary")["output"]["output"]

    concatenated_result = (
        " # Emissions data\n"
        + result_0
        + "\n\n"
        + "## Scope emissions data\n"
        + result_1
        + "\n\n"
        + result_2
        + "\n\n"
        + "## Other emissions data\n"
        + result_3
        + "\n\n"
        + result_4
    )
    return concatenated_result

def Targets_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Emissions_reduction_targets_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Emissions_reduction_targets_summary")["output"]["output"]

    concatenated_result = (
        "# Emissions reduction targets\n"
        + result_0
        + "\n\n"
        + result_1
    )

    return concatenated_result

def Performance_merge(ti): 
    result_0 = ti.xcom_pull(task_ids="Emissions_performance_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Emissions_performance_summary")["output"]["output"]

    concatenated_result = (
        "# Emissions performance\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def Initiatives_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Emissions_reduction_initiatives_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Emissions_reduction_initiatives_summary")["output"]["output"]

    concatenated_result = (
        "# Emissions reduction initiatives\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def Verification_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Verification_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Verification_summary")["output"]["output"]

    concatenated_result = (
        "# Verification\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def Governance_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Governance_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Governance_summary")["output"]["output"]

    concatenated_result = (
        "# Governance\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def Risks_opportunities_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Risks_opportunities_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Risks_opportunities_summary")["output"]["output"]

    concatenated_result = (
        "# Risks opportunities\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def Carbon_pricing_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Carbon_pricing_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Carbon_pricing_summary")["output"]["output"]

    concatenated_result = (
        "# Carbon pricing\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def Scope_1_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Scope_1_emissions_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Scope_1_emissions_summary")["output"]["output"]

    concatenated_result = (
        "# Scope 1 emissions\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def Scope_2_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Scope_2_emissions_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Scope_2_emissions_summary")["output"]["output"]

    concatenated_result = (
        "# Scope 2 emissions\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def Scope_3_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Scope_3_emissions_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Scope_3_emissions_summary")["output"]["output"]

    concatenated_result = (
        "# Scope 3 emissions\n"
        + result_0
        + "\n\n"
        + result_1
    )
    return concatenated_result

def merge_EN(ti):
    result_0 = ti.xcom_pull(task_ids="Emissions_data_output_merge")
    result_1 = ti.xcom_pull(task_ids="Targets_output_merge")
    result_2 = ti.xcom_pull(task_ids="Performance_output_merge")
    result_3 = ti.xcom_pull(task_ids="Initiatives_output_merge")
    result_4 = ti.xcom_pull(task_ids="Verification_output_merge")
    result_5 = ti.xcom_pull(task_ids="Governance_output_merge")
    result_6 = ti.xcom_pull(task_ids="Risks_opportunities_output_merge")
    result_7 = ti.xcom_pull(task_ids="Carbon_pricing_output_merge")
    result_8 = ti.xcom_pull(task_ids="Scope_1_output_merge")
    result_9 = ti.xcom_pull(task_ids="Scope_2_output_merge")
    result_10 = ti.xcom_pull(task_ids="Scope_3_output_merge")

    concatenated_result = (
        str(result_0)
        + "\n\n"
        + str(result_1)
        + "\n\n"
        + str(result_2)
        + "\n\n"
        + str(result_3)
        + "\n\n"
        + str(result_4)
        + "\n\n"
        + str(result_5)
        + "\n\n"
        + str(result_6)
        + "\n\n"
        + str(result_7)
        + "\n\n"
        + str(result_8)
        + "\n\n"
        + str(result_9)
        + "\n\n"
        + str(result_10)
    )

    markdown_file_name = "Automobile_ESG_compliance_report_EN.md"

    # Save the model's response to a Markdown file
    with open(markdown_file_name, "w") as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result

def merge_CN(ti):
    result_0 = ti.xcom_pull(task_ids="Emissions_data_translate")["output"]["content"]
    result_1 = ti.xcom_pull(task_ids="Targets_translate")["output"]["content"]
    result_2 = ti.xcom_pull(task_ids="Performance_translate")["output"]["content"]
    result_3 = ti.xcom_pull(task_ids="Initiatives_translate")["output"]["content"]
    result_4 = ti.xcom_pull(task_ids="Verification_translate")["output"]["content"]
    result_5 = ti.xcom_pull(task_ids="Governance_translate")["output"]["content"]
    result_6 = ti.xcom_pull(task_ids="Risks_opportunities_translate")["output"]["content"]
    result_7 = ti.xcom_pull(task_ids="Carbon_pricing_translate")["output"]["content"]
    result_8 = ti.xcom_pull(task_ids="Scope_1_translate")["output"]["content"]
    result_9 = ti.xcom_pull(task_ids="Scope_2_translate")["output"]["content"]
    result_10 = ti.xcom_pull(task_ids="Scope_3_translate")["output"]["content"]

    concatenated_result = (
        result_0 + "\n\n" + result_1 + "\n\n" + result_2 + "\n\n" + result_3 + "\n\n" + result_4 + "\n\n" + result_5 + "\n\n" + result_6 + "\n\n" + result_7 + "\n\n" + result_8 + "\n\n" + result_9 + "\n\n" + result_10
    )

    markdown_file_name = "Automobile_ESG_compliance_report_CN.md"

    # Save the model's response to a Markdown file
    with open(markdown_file_name, "w") as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result

def save_file(ti, file_name: str = "Automobile_ESG_compliance_report_CN.md"):
    data = ti.xcom_pull(task_ids="translate")["output"]["content"]
    with open(file_name, "w") as markdown_file:
        markdown_file.write(data)
    return data

# write formatter for each task
Scope_emissions_data_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Scope_1_emissions_data",
        "Scope_1_emissions_breakdown",
        "Scope_2_emissions_data",
        "Scope_2_emissions_reporting",
        "Scope_2_emissions_breakdown",
        "Scope_3_emissions_data",
    ],
    session_id=session_id,
)

Other_emissions_data_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Exclusions",
        "Biogenic_carbon_data",
        "Emissions_intensities",
        "Emissions_breakdown_by_subsidiary",
    ],
    session_id=session_id,
)   

Scope_emissions_data_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's information on Scope1, Scope2, and Scope3 emissions data, only the disclosed data needs to be focused on, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Scope_1_emissions_data",
        "Scope_1_emissions_breakdown",
        "Scope_2_emissions_data",
        "Scope_2_emissions_reporting",
        "Scope_2_emissions_breakdown",
        "Scope_3_emissions_data",
    ],
    session_id=session_id,
)

Other_emissions_data_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's information on the following additional greenhouse gas emissions indicators: Exclusions, Biogenic carbon data, Emissions intensities, and Emissions breakdown by subsidiary. All integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Exclusions",
        "Biogenic_carbon_data",
        "Emissions_intensities",
        "Emissions_breakdown_by_subsidiary",
    ],
    session_id=session_id,
)

Emissions_data_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's information on emissions date from 'Scope emissions data' part and 'Other emissions data' part. All integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Scope_emissions_data_overview",
        "Other_emissions_data_overview",
    ],
    session_id=session_id,
)

Emissions_reduction_targets_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Total_emission_reduction_target",
        "Scope_1_emissions_reduction_target",
        "Scope_2_emissions_reduction_target",
        "Scope_3_emissions_reduction_target",
        "Science_based_target",
    ],
    session_id=session_id,
)

Emissions_reduction_targets_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on emissions reduction targets, including total, Scope 1, Scope 2, and Scope 3 emissions reduction targets, as well as science-based targets, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Total_emission_reduction_target",
        "Scope_1_emissions_reduction_target",
        "Scope_2_emissions_reduction_target",
        "Scope_3_emissions_reduction_target",
        "Science_based_target",
    ],
    session_id=session_id,
)

Emissions_performance_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Completion_of_emission_reduction_targets",
    ],
    session_id=session_id,
)

Emissions_performance_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on completion of emission reduction targets, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Completion_of_emission_reduction_targets",
    ],
    session_id=session_id,
)

Emissions_reduction_initiatives_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Specific_emission_reduction_instructions",
    ],
    session_id=session_id,
)

Emissions_reduction_initiatives_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on specific emission reduction instructions, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Specific_emission_reduction_instructions",
    ],
    session_id=session_id,
)

Verification_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Scope_1_emissions_verification",
        "Scope_2_emissions_verification",
        "Scope_3_emissions_verification",
    ],
    session_id=session_id,
)

Verification_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on verification, including Scope 1, Scope 2, and Scope 3 emissions verification, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Scope_1_emissions_verification",
        "Scope_2_emissions_verification",
        "Scope_3_emissions_verification",
    ],
    session_id=session_id,
)

Governance_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Board_oversight",
        "Management_responsibility",
        "Employee_incentives",
    ],
    session_id=session_id,
)

Governance_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on governance, including Board oversight, Management responsibility, and Employee incentives, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Board_oversight",
        "Management_responsibility",
        "Employee_incentives",
    ],
    session_id=session_id,
)

Risks_opportunities_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Management_processes",
        "Risk_disclosure",
        "Opportunity_disclosure",
    ],
    session_id=session_id,
)

Risks_opportunities_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on governance, including Board oversight, Management responsibility, and Employee incentives, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Management_processes",
        "Risk_disclosure",
        "Opportunity_disclosure",
    ],
    session_id=session_id,
)

Carbon_pricing_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Carbon_pricing_systems",
        "Project_based_carbon_credits",
        "Internal_price_on_carbon",  
    ],
    session_id=session_id,
)

Carbon_pricing_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on carbon pricing, including Carbon pricing systems, Project-based carbon credits, and Internal price on carbon, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Carbon_pricing_systems",
        "Project_based_carbon_credits",
        "Internal_price_on_carbon",  
    ],
    session_id=session_id,
)

Scope_1_emissions_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Scope_1_emissions_data",
        "Scope_1_emissions_breakdown",
        "Scope_1_emissions_reduction_target",
        "Scope_1_emissions_verification",
    ],
    session_id=session_id,
)

Scope_1_emissions_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on Scope 1 emissions, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Scope_1_emissions_data",
        "Scope_1_emissions_breakdown",
        "Scope_1_emissions_reduction_target",
        "Scope_1_emissions_verification",
    ],
    session_id=session_id,
)

Scope_2_emissions_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Scope_2_emissions_data",
        "Scope_2_emissions_reporting",
        "Scope_2_emissions_breakdown",
        "Scope_2_emissions_reduction_target",
        "Scope_2_emissions_verification",
    ],
    session_id=session_id,
)

Scope_2_emissions_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on Scope 2 emissions, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Scope_2_emissions_data",
        "Scope_2_emissions_reporting",
        "Scope_2_emissions_breakdown",
        "Scope_2_emissions_reduction_target",
        "Scope_2_emissions_verification",
    ],
    session_id=session_id,
)

Scope_3_emissions_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Scope_3_emissions_data",
        "Scope_3_emissions_reduction_target",
        "Scope_3_emissions_verification",
    ],
    session_id=session_id,
)

Scope_3_emissions_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the provided information, SUMMARIZE the report's details on Scope 3 emissions, all integrated into ONE comprehensive and clear paragraph.",
    task_ids=[
        "Scope_3_emissions_data",
        "Scope_3_emissions_reduction_target",
        "Scope_3_emissions_verification",
    ],
    session_id=session_id,
)

Emissions_data_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Emissions_data_output_merge"],
)
Targets_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Targets_output_merge"],
)
Performance_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Performance_output_merge"],
)
Initiatives_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Initiatives_output_merge"],
)
Verification_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Verification_output_merge"],
)
Governance_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Governance_output_merge"],
)
Risks_opportunities_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Risks_opportunities_output_merge"],
)
Carbon_pricing_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Carbon_pricing_output_merge"],
)
Scope_1_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Scope_1_output_merge"],
)
Scope_2_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Scope_2_output_merge"],
)
Scope_3_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Scope_3_output_merge"],
)

# write agent for each task
agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
# openai = partial(post_request, post_url=openai_url, formatter=openai_formatter)
Scope_emissions_data_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Scope_emissions_data_summary_formatter
)
Other_emissions_data_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Other_emissions_data_summary_formatter
)
Scope_emissions_data_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Scope_emissions_data_overview_formatter
)
Other_emissions_data_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Other_emissions_data_overview_formatter
)
Emissions_data_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Emissions_data_overview_formatter
)
Emissions_reduction_targets_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Emissions_reduction_targets_summary_formatter
)
Emissions_reduction_targets_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Emissions_reduction_targets_overview_formatter
)
Emissions_performance_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Emissions_performance_summary_formatter
)
Emissions_performance_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Emissions_performance_overview_formatter
)
Emissions_reduction_initiatives_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Emissions_reduction_initiatives_summary_formatter
)
Emissions_reduction_initiatives_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Emissions_reduction_initiatives_overview_formatter
)
Verification_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Verification_summary_formatter
)
Verification_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Verification_overview_formatter
)
Governance_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Governance_summary_formatter
)
Governance_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Governance_overview_formatter
)
Risks_opportunities_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Risks_opportunities_summary_formatter
)
Risks_opportunities_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Risks_opportunities_overview_formatter
)
Carbon_pricing_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Carbon_pricing_summary_formatter
)
Carbon_pricing_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Carbon_pricing_overview_formatter
)
Scope_1_emissions_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Scope_1_emissions_summary_formatter
)
Scope_1_emissions_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Scope_1_emissions_overview_formatter
)
Scope_2_emissions_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Scope_2_emissions_summary_formatter
)
Scope_2_emissions_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Scope_2_emissions_overview_formatter
)
Scope_3_emissions_summary_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Scope_3_emissions_summary_formatter
)
Scope_3_emissions_overview_agent = partial(
    post_request, post_url=esg_automobile_url, formatter=Scope_3_emissions_overview_formatter
)

Emissions_data_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Emissions_data_translate_formatter
)
Targets_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Targets_translate_formatter
)
Performance_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Performance_translate_formatter
)
Initiatives_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Initiatives_translate_formatter
)
Verification_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Verification_translate_formatter
)
Governance_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Governance_translate_formatter
)
Risks_opportunities_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Risks_opportunities_translate_formatter
)
Carbon_pricing_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Carbon_pricing_translate_formatter
)
Scope_1_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Scope_1_translate_formatter
)
Scope_2_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Scope_2_translate_formatter
)
Scope_3_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Scope_3_translate_formatter
)

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="Automobile_esg_compliance",
    default_args=default_args,
    description="Automobile ESG compliance Agent DAG",
    schedule_interval=None,
    tags=["ESG_agent"],
    catchup=False,
) as dag:
    # wait_for_10_seconds = TimeDeltaSensor(
    # task_id='wait_for_10_seconds',
    # delta=timedelta(seconds=10),

# Scope 1 emissions data task
    Scope_1_emissions_data = task_PyOpr(
        task_id="Scope_1_emissions_data",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide the total gross global emissions of Scope 1 in the report, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 1 emissions breakdown task
    Scope_1_emissions_breakdown = task_PyOpr(
        task_id="Scope_1_emissions_breakdown",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Summarize the global Scope 2 emissions in metric tons of CO2 equivalent (CO2e) in the report. Detail the composition of these emissions by identifying the quantities of each greenhouse gas included. Provide a geographical breakdown, categorizing emissions by countries, regions, or specific areas, as well as by business divisions, facilities, and the types of activities generating these emissions. Conclude by citing the sources for the Global Warming Potential (GWP) values utilized in your calculations."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )   

# Scope 2 emissions data task
    Scope_2_emissions_data = task_PyOpr(
        task_id="Scope_2_emissions_data",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide the total gross global emissions of Scope 2 in the report, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions reporting Task
    Scope_2_emissions_reporting = task_PyOpr(
        task_id="Scope_2_emissions_reporting",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Describe the approach to reporting Scope 2 emissions in the report."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions breakdown Task
    Scope_2_emissions_breakdown = task_PyOpr(
        task_id="Scope_2_emissions_breakdown",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide a detailed breakdown of the total gross global Scope 2 emissions in the report. This breakdown should include data categorized by geographical location (country, area, or region), business division, specific business facility, and type of business activity."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions data Task
    Scope_3_emissions_data = task_PyOpr(
        task_id="Scope_3_emissions_data",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Detail the total global Scope 3 emissions in the report, covering all pertinent categories: Purchased goods and services, Capital goods, Fuel-and-energy-related activities (excluding Scope 1 and 2 emissions), Upstream and Downstream transportation and distribution, Waste generated in operations, Business travel, Employee commuting, Upstream and Downstream leased assets, Processing of sold products, Use of sold products, End-of-life treatment of sold products, Franchises, and Investments not related to financial services. Also, include any 'Other' categories, both upstream and downstream. It's essential to document these emissions comprehensively and provide detailed justifications for any exclusions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Exclusions Task
    Exclusions = task_PyOpr(
        task_id="Exclusions",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Check for any undisclosed sources of Scope 1, Scope 2, or Scope 3 emissions within the reporting boundary. This includes unreported facilities, greenhouse gases (GHGs), activities, or geographic areas. If exclusions are found, detail the omitted sources for Scope 1, Scope 2, or Scope 3 emissions that fall within the reporting boundary yet are absent from the report's disclosures."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Biogenic carbon data Task
    Biogenic_carbon_data = task_PyOpr(
        task_id="Biogenic_carbon_data",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Assess the relevance of biogenic carbon sources to carbon dioxide emissions in the report. If applicable, calculate and report the emissions from biogenic carbon sources, expressing the total in metric tons of CO2."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Emissions intensities Task
    Emissions_intensities = task_PyOpr(
        task_id="Emissions_intensities",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide a description of the combined gross global emissions for Scope 1 and Scope 2 for the reporting year, expressed in metric tons of CO2 equivalent (CO2e) per unit of total revenue in the chosen currency. Additionally, include any other relevant intensity metrics that align with the specifics of the business operations."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },  
    )

# Emissions breakdown by subsidiary Task
    Emissions_breakdown_by_subsidiary = task_PyOpr(
        task_id="Emissions_breakdown_by_subsidiary",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Where relevant in the report, present disaggregated emission data for each subsidiary, detailing the total Scope 1 and Scope 2 emissions separately for each entity."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Scope_emissions_data_summary = task_PyOpr(
        task_id="Scope_emissions_data_summary",
        callable_func=Scope_emissions_data_summary_agent,
    )

    Other_emissions_data_summary = task_PyOpr(
        task_id="Other_emissions_data_summary",
        callable_func=Other_emissions_data_summary_agent,
    )

    Scope_emissions_data_overview = task_PyOpr(
        task_id="Scope_emissions_data_overview",
        callable_func=Scope_emissions_data_overview_agent,
    )

    Other_emissions_data_overview = task_PyOpr(
        task_id="Other_emissions_data_overview",
        callable_func=Other_emissions_data_overview_agent,
    )

    Emissions_data_overview = task_PyOpr(
        task_id="Emissions_data_overview",
        callable_func=Emissions_data_overview_agent,
    )

# Total emission reduction target Task
    Total_emission_reduction_target = task_PyOpr(
        task_id="Total_emission_reduction_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Specify the total emission reduction target in the report, quantified in metric tons of CO2 equivalent (CO2e). Specify the target coverage area for this reduction goal. Options for target coverage include: Company-wide, Specific business division, Particular business activity, Individual site or facility, Defined country/area/region, Product-level, or Other (please specify)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 1 emissions reduction target Task
    Scope_1_emissions_reduction_target = task_PyOpr(
        task_id="Scope_1_emissions_reduction_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input":{
                    "input": "Specify the Scope 1 emissions reduction target in the report, expressed in metric tons of CO2 equivalent (CO2e). Additionally, define the target coverage for this reduction goal. The options for target coverage include: Company-wide, Business division, Business activity, Site/facility, Country/area/region, Product-level, or Other (please specify if applicable)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions reduction target Task
    Scope_2_emissions_reduction_target = task_PyOpr(
        task_id="Scope_2_emissions_reduction_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Detail the Scope 2 emissions reduction target in the report, quantified in metric tons of CO2 equivalent (CO2e). Define the target coverage area, with options including Company-wide, Business division, Business activity, Site/facility, Country/area/region, Product-level, or Other (specify if applicable). Also, indicate the accounting method used for Scope 2 emissions: Location-based or Market-based."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions reduction target Task
    Scope_3_emissions_reduction_target = task_PyOpr(
        task_id="Scope_3_emissions_reduction_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Detail the Scope 3 emissions reduction target in the report, quantified in metric tons of CO2 equivalent (CO2e). Specify the target's scope, selecting from: Company-wide, Business division, Business activity, Site/facility, Country/area/region, Product-level, or Other (with clarification). Include a comprehensive breakdown of the Scope 3 reduction target across all relevant categories: Purchased goods and services, Capital goods, Fuel-and-energy-related activities (excluding Scope 1 and 2), Upstream transportation and distribution, Waste generated in operations, Business travel, Employee commuting, Upstream leased assets, Downstream transportation and distribution, Processing of sold products, Use of sold products, End-of-life treatment of sold products, Downstream leased assets, Franchises, Investments (not related to financial services), and any additional applicable upstream or downstream categories."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Science-based target Task
    Science_based_target = task_PyOpr(
        task_id="Science_based_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Confirm if the emissions reduction target is recognized as a science-based carbon target by the Science Based Targets initiative (SBTi) in the report."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Emissions_reduction_targets_summary = task_PyOpr(
        task_id="Emissions_reduction_targets_summary",
        callable_func=Emissions_reduction_targets_summary_agent,
    )

    Emissions_reduction_targets_overview = task_PyOpr(
        task_id="Emissions_reduction_targets_overview",
        callable_func=Emissions_reduction_targets_overview_agent,
    )

# Completion of emission reduction targets Task
    Completion_of_emission_reduction_targets = task_PyOpr(
        task_id="Completion_of_emission_reduction_targets",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Provide details of the organization’s absolute emissions targets and progress against these targets in the report."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Emissions_performance_summary = task_PyOpr(
        task_id="Emissions_performance_summary",
        callable_func=Emissions_performance_summary_agent,
    )

    Emissions_performance_overview = task_PyOpr(
        task_id="Emissions_performance_overview",
        callable_func=Emissions_performance_overview_agent,
    )

# Specific emission reduction instructions Task
    Specific_emission_reduction_instructions = task_PyOpr(
        task_id="Specific_emission_reduction_instructions",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Assess if the report indicates any ongoing efforts to reduce emissions. If affirmative, describe these emission reduction actions in detail, covering both planning and execution phases. Also, provide an estimation of the anticipated CO2e emission reductions resulting from these actions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Emissions_reduction_initiatives_summary = task_PyOpr(
        task_id="Emissions_reduction_initiatives_summary",
        callable_func=Emissions_reduction_initiatives_summary_agent,
    )

    Emissions_reduction_initiatives_overview = task_PyOpr(
        task_id="Emissions_reduction_initiatives_overview",
        callable_func=Emissions_reduction_initiatives_overview_agent,
    )

# Scope 1 emissions verification Task
    Scope_1_emissions_verification = task_PyOpr(
        task_id="Scope_1_emissions_verification",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Verify if the report includes third-party verification or certification for its Scope 1 emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions verification Task
    Scope_2_emissions_verification = task_PyOpr(
        task_id="Scope_2_emissions_verification",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Verify if the report employs a third-party verification or certification process for its Scope 2 emissions, specifying whether it applies to location-based or market-based emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions verification Task
    Scope_3_emissions_verification = task_PyOpr(
        task_id="Scope_3_emissions_verification",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Establish whether the report has a third-party verification or certification process in place for its Scope 3 emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Verification_summary = task_PyOpr(
        task_id="Verification_summary",
        callable_func=Verification_summary_agent,
    )

    Verification_overview = task_PyOpr(
        task_id="Verification_overview",
        callable_func=Verification_overview_agent,
    )

# Board oversight Task
    Board_oversight = task_PyOpr(
        task_id="Board_oversight",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input": {
                    "input": "Check the report for board-level oversight of climate-related issues. If present, detail the roles and responsibilities of board members concerning climate-related matters and describe how the board oversees these issues."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Management responsibility Task
    Management_responsibility = task_PyOpr(
        task_id="Management_responsibility",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input": {
                    "input": "Assess whether the report identifies any management roles, committees, or departments tasked with handling climate change-related issues below the board level."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Employee incentives Task
    Employee_incentives = task_PyOpr(
        task_id="Employee_incentives",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input": {
                    "input": "Check the report for any incentives related to managing climate-related issues, including rewards for meeting specific objectives. If found, detail the mechanisms and frameworks established for managing and recognizing achievements in climate-related efforts."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_summary = task_PyOpr(
        task_id="Governance_summary",
        callable_func=Governance_summary_agent,
    )

    Governance_overview = task_PyOpr(
        task_id="Governance_overview",
        callable_func=Governance_overview_agent,
    )

# Management processes Task
    Management_processes = task_PyOpr(
        task_id="Management_processes",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input": {
                    "input": "Detail the methodology for identifying, assessing, and responding to climate-related risks and opportunities in the report. This should include the approach used to define short, medium, and long-term time horizons, as well as the criteria for determining material financial or strategic impacts. Additionally, explain how the organization differentiates between physical risks and transition risks in its climate risk assessment process."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Risk disclosure Task
    Risk_disclosure = task_PyOpr(
        task_id="Risk_disclosure",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input": {
                    "input": "Detail any identified climate-related risks that could significantly affect the financial stability or strategic direction in the report. This should encompass risks across the value chain, including upstream and downstream supply chain aspects, direct operations, investments, and other relevant areas. Emphasize on outlining how these risks are determined to be substantial in their potential impact."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Opportunity disclosure Task
    Opportunity_disclosure = task_PyOpr(
        task_id="Opportunity_disclosure",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input": {
                    "input":"Elaborate on any identified climate-related risks that possess the potential to materially affect the finances or strategic direction in the report. Focus on specifying the actual financial impact these risks could have on the business, detailing their nature, potential severity, and the financial implications."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Risks_opportunities_summary = task_PyOpr(
        task_id="Risks_opportunities_summary",
        callable_func=Risks_opportunities_summary_agent,
    )

    Risks_opportunities_overview = task_PyOpr(
        task_id="Risks_opportunities_overview",
        callable_func=Risks_opportunities_overview_agent,
    )

# Carbon pricing systems Task
    Carbon_pricing_systems = task_PyOpr(
        task_id="Carbon_pricing_systems",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Confirm if the operations or activities fall under any carbon pricing systems, like Emissions Trading System (ETS), Cap & Trade, or Carbon Tax in the report. If applicable, disclose which carbon pricing regulations impact the operations and provide a detailed description of the emissions trading system and its implications."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Project-based carbon credits Task
    Project_based_carbon_credits = task_PyOpr(
        task_id="Project_based_carbon_credits",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Determine if the report mentions the cancellation of any project-based carbon credits in the reporting year. If so, provide details on these canceled project-based carbon credits, including the projects' characteristics and size, the volume of credits canceled, and the reasons for their cancellation."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Internal price on carbon Task
    Internal_price_on_carbon = task_PyOpr(
        task_id="Internal_price_on_carbon",
        callable_func=agent,
        op_kwargs={
            "data_to_send":{
                "input":{
                    "input": "Assess whether the report mentions the use of an internal carbon price. If affirmative, describe in detail how this internal carbon price is implemented across the organization, covering its role in decision-making processes, strategy formulation, and other operational activities."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Carbon_pricing_summary = task_PyOpr(
        task_id="Carbon_pricing_summary",
        callable_func=Carbon_pricing_summary_agent,
    )

    Carbon_pricing_overview = task_PyOpr(
        task_id="Carbon_pricing_overview",
        callable_func=Carbon_pricing_overview_agent,
    )

    Scope_1_emissions_summary = task_PyOpr(
        task_id="Scope_1_emissions_summary",
        callable_func=Scope_1_emissions_summary_agent,
    )

    Scope_1_emissions_overview = task_PyOpr(
        task_id="Scope_1_emissions_overview",
        callable_func=Scope_1_emissions_overview_agent,
    )

    Scope_2_emissions_summary = task_PyOpr(
        task_id="Scope_2_emissions_summary",
        callable_func=Scope_2_emissions_summary_agent,
    )

    Scope_2_emissions_overview = task_PyOpr(
        task_id="Scope_2_emissions_overview",
        callable_func=Scope_2_emissions_overview_agent,
    )

    Scope_3_emissions_summary = task_PyOpr(
        task_id="Scope_3_emissions_summary",
        callable_func=Scope_3_emissions_summary_agent,
    )

    Scope_3_emissions_overview = task_PyOpr(
        task_id="Scope_3_emissions_overview",
        callable_func=Scope_3_emissions_overview_agent,
    )


    Emissions_data_output_merge = PythonOperator(
        task_id="Emissions_data_output_merge",
        python_callable=Emissions_merge,
    )

    Targets_output_merge = PythonOperator(
        task_id="Targets_output_merge",
        python_callable=Targets_merge,
    )

    Performance_output_merge = PythonOperator(
        task_id="Performance_output_merge",
        python_callable=Performance_merge,
    )

    Initiatives_output_merge = PythonOperator(
        task_id="Initiatives_output_merge",
        python_callable=Initiatives_merge,
    )

    Verification_output_merge = PythonOperator(
        task_id="Verification_output_merge",
        python_callable=Verification_merge,
    )

    Governance_output_merge = PythonOperator(
        task_id="Governance_output_merge",
        python_callable=Governance_merge,
    )

    Risks_opportunities_output_merge = PythonOperator(
        task_id="Risks_opportunities_output_merge",
        python_callable=Risks_opportunities_merge,
    )

    Carbon_pricing_output_merge = PythonOperator(
        task_id="Carbon_pricing_output_merge",
        python_callable=Carbon_pricing_merge,
    )

    Scope_1_output_merge = PythonOperator(
        task_id="Scope_1_output_merge",
        python_callable=Scope_1_merge,
    )

    Scope_2_output_merge = PythonOperator(
        task_id="Scope_2_output_merge",
        python_callable=Scope_2_merge,
    )

    Scope_3_output_merge = PythonOperator(
        task_id="Scope_3_output_merge",
        python_callable=Scope_3_merge,
    )

    Emissions_data_translate = task_PyOpr(
        task_id="Emissions_data_translate",
        callable_func=Emissions_data_translate_agent,
    )

    Targets_translate = task_PyOpr(
        task_id="Targets_translate",
        callable_func=Targets_translate_agent,
    )

    Performance_translate = task_PyOpr(
        task_id="Performance_translate",
        callable_func=Performance_translate_agent,
    )

    Initiatives_translate = task_PyOpr(
        task_id="Initiatives_translate",
        callable_func=Initiatives_translate_agent,
    )

    Verification_translate = task_PyOpr(
        task_id="Verification_translate",
        callable_func=Verification_translate_agent,
    )

    Governance_translate = task_PyOpr(
        task_id="Governance_translate",
        callable_func=Governance_translate_agent,
    )

    Risks_opportunities_translate = task_PyOpr(
        task_id="Risks_opportunities_translate",
        callable_func=Risks_opportunities_translate_agent,
    )

    Carbon_pricing_translate = task_PyOpr(
        task_id="Carbon_pricing_translate",
        callable_func=Carbon_pricing_translate_agent,
    )

    Scope_1_translate = task_PyOpr(
        task_id="Scope_1_translate",
        callable_func=Scope_1_translate_agent,
    )

    Scope_2_translate = task_PyOpr(
        task_id="Scope_2_translate",
        callable_func=Scope_2_translate_agent,
    )

    Scope_3_translate = task_PyOpr(
        task_id="Scope_3_translate",
        callable_func=Scope_3_translate_agent,
    )

    Merge_EN = task_PyOpr(
        task_id="Merge_EN",
        callable_func=merge_EN,
    )

    Merge_CN = task_PyOpr(
        task_id="Merge_CN",
        callable_func=merge_CN,
    )

    # Chain tasks
    (
        [
            Scope_1_emissions_data,
            Scope_1_emissions_breakdown,
            Scope_2_emissions_data,
            Scope_2_emissions_reporting,
            Scope_2_emissions_breakdown,
            Scope_3_emissions_data,
        ] 
            >> Scope_emissions_data_summary
            >> Scope_emissions_data_overview
    )

    (
        [
            Exclusions,
            Biogenic_carbon_data,
            Emissions_intensities,
            Emissions_breakdown_by_subsidiary,
        ] 
            >> Other_emissions_data_summary
            >> Other_emissions_data_overview
    )

    [Scope_emissions_data_overview, Other_emissions_data_overview] >> Emissions_data_overview

    (
        [
            Emissions_data_overview,
            Scope_emissions_data_summary,
            Other_emissions_data_summary,
            Scope_emissions_data_overview,
            Other_emissions_data_overview,
        ]
            >> Emissions_data_output_merge
            >> Emissions_data_translate
    )

    (
        [
            Total_emission_reduction_target,
            Scope_1_emissions_reduction_target,
            Scope_2_emissions_reduction_target,
            Scope_3_emissions_reduction_target,
            Science_based_target,
        ]   
        >> Emissions_reduction_targets_summary
        >> Emissions_reduction_targets_overview
    )

    (
        [
            Emissions_reduction_targets_summary,
            Emissions_reduction_targets_overview,
        ]
            >> Targets_output_merge
            >> Targets_translate
    )

    (
        [
            Completion_of_emission_reduction_targets,
        ]   
            >> Emissions_performance_summary
            >> Emissions_performance_overview
    )

    (
        [  
            Emissions_performance_summary,
            Emissions_performance_overview,
        ] 
        >> Performance_output_merge
        >> Performance_translate
    )

    (
        [
            Specific_emission_reduction_instructions,
        ]   
        >> Emissions_reduction_initiatives_summary
        >> Emissions_reduction_initiatives_overview
    )

    (
        [
            Emissions_reduction_initiatives_summary,
            Emissions_reduction_initiatives_overview,
        ] 
        >> Initiatives_output_merge
        >> Initiatives_translate
    )

    (
        [
            Scope_1_emissions_verification,
            Scope_2_emissions_verification,
            Scope_3_emissions_verification,
        ]   
        >> Verification_summary
        >> Verification_overview
    )

    (
        [
            Verification_summary,
            Verification_overview,
        ] 
        >> Verification_output_merge
        >> Verification_translate
    )

    (
        [
            Board_oversight,
            Management_responsibility,
            Employee_incentives,
        ]   
        >> Governance_summary
        >> Governance_overview
    )

    (
        [
            Governance_summary,
            Governance_overview,
        ]   
        >> Governance_output_merge
        >> Governance_translate
    )

    (
        [
            Management_processes,
            Risk_disclosure,
            Opportunity_disclosure,
        ]   
        >> Risks_opportunities_summary
        >> Risks_opportunities_overview
    )

    (
        [
           Risks_opportunities_summary,
           Risks_opportunities_overview,
        ]   
        >> Risks_opportunities_output_merge
        >> Risks_opportunities_translate
    )

    (
        [
            Carbon_pricing_systems,
            Project_based_carbon_credits,
            Internal_price_on_carbon,
        ]   
        >> Carbon_pricing_summary
        >> Carbon_pricing_overview
    )

    (
        [
            Carbon_pricing_summary,
            Carbon_pricing_overview,
        ]   
        >> Carbon_pricing_output_merge
        >> Carbon_pricing_translate
    )

    (
        [
            Scope_1_emissions_data,
            Scope_1_emissions_breakdown,
            Scope_1_emissions_reduction_target,
            Scope_1_emissions_verification,
        ] 
        >> Scope_1_emissions_summary
        >> Scope_1_emissions_overview
    )

    (
        [
            Scope_1_emissions_summary,
            Scope_1_emissions_overview, 
        ] 
        >> Scope_1_output_merge
        >> Scope_1_translate
    )

    (
        [
            Scope_2_emissions_data,
            Scope_2_emissions_reporting,
            Scope_2_emissions_breakdown,
            Scope_2_emissions_reduction_target,
            Scope_2_emissions_verification,
        ] 
        >> Scope_2_emissions_summary
        >> Scope_2_emissions_overview
    )

    (
        [
            Scope_2_emissions_summary,
            Scope_2_emissions_overview,   
        ] 
        >> Scope_2_output_merge
        >> Scope_2_translate
    )

    (
        [
            Scope_3_emissions_data,
            Scope_3_emissions_reduction_target,
            Scope_3_emissions_verification,
        ] 
        >> Scope_3_emissions_summary
        >> Scope_3_emissions_overview
    )

    (
        [
            Scope_3_emissions_summary,
            Scope_3_emissions_overview, 
        ] 
        >> Scope_3_output_merge
        >> Scope_3_translate
    )

    [
        Emissions_data_output_merge,
        Targets_output_merge,
        Performance_output_merge,
        Initiatives_output_merge,
        Verification_output_merge,
        Governance_output_merge,
        Risks_opportunities_output_merge,
        Carbon_pricing_output_merge,
        Scope_1_output_merge,
        Scope_2_output_merge,
        Scope_3_output_merge,
    ]  >> Merge_EN
   
    [
        Emissions_data_translate,
        Targets_translate,
        Performance_translate,
        Initiatives_translate,
        Verification_translate,
        Governance_translate,
        Risks_opportunities_translate,
        Carbon_pricing_translate,
        Scope_1_translate,
        Scope_2_translate,
        Scope_3_translate,
    ] >> Merge_CN
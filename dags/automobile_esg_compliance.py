import json
import os
from datetime import timedelta
from functools import partial
import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from httpx import RequestError
BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"
automobile_esg_url = "/automobile_esg_agent/invoke"

session_id = "20240119"

task_summary_prompt = """Based on the input, provide a summarized paragraph for each task. You must follow the structure in Markdown format like below (change the title to the task ID):

    ### Emissions_data_1
    A concise summary of the disclosure status in the task.
    ### Emissions_data_2
    A concise summary of the disclosure status in the task.
    ...
    (Continue with additional task IDs and their summaries)
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
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=120) as client:
        try:
            response = client.post(post_url, json=data_to_send)
            response.raise_for_status()
            data = response.json()
            return data

        except RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")

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

def task_PyOpr(
    task_id: str,
    callable_func,
    retries: int = 3,
    retry_delay=timedelta(seconds=3),
    execution_timeout=timedelta(minutes=10),
    op_kwargs: dict = None,
):
    return PythonOperator(
        task_id=task_id,
        python_callable=callable_func,
        retries=retries,
        retry_delay=retry_delay,
        execution_timeout=execution_timeout,
        op_kwargs=op_kwargs,
    )

def automobile_esg_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Emissions_data_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Scope_1_emissions_overview")["output"]["output"]
    result_2 = ti.xcom_pull(task_ids="Scope_2_emissions_overview")["output"]["output"]
    result_3 = ti.xcom_pull(task_ids="Scope_3_emissions_overview")["output"]["output"]
    result_4 = ti.xcom_pull(task_ids="Emissions_reduction_targets_overview")["output"]["output"]
    result_5 = ti.xcom_pull(task_ids="Emissions_performance_overview")["output"]["output"]
    result_6 = ti.xcom_pull(task_ids="Emissions_reduction_initiatives_overview")["output"]["output"]
    result_7 = ti.xcom_pull(task_ids="Verification_overview")["output"]["output"]
    result_8 = ti.xcom_pull(task_ids="Governance_overview")["output"]["output"]
    result_9 = ti.xcom_pull(task_ids="Risks_opportunities_overview")["output"]["output"]
    result_10 = ti.xcom_pull(task_ids="Carbon_pricing_overview")["output"]["output"]
    results = [result_0, result_1, result_2, result_3, result_4, result_5, result_6, result_7, result_8, result_9, result_10]
    concatenated_result = "\n\n".join(results)
    # print(concatenated_result)
    
    markdown_file_name = 'Automobile_ESG_Summary_Output.md'

    # Save the model's response to a Markdown file
    with open(markdown_file_name, 'w') as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result

Emissions_data_openai_formatter = partial(
    openai_formatter,
    prompt="""Based on the following upstream outputs, Provide a summary for each task related to the organization's disclosure of emissions data. You must follow the structure below:
    # Emissions data
    ## Emissions data 1
    A concise summary of the disclosure in the task.
    ## Emissions data 2
    A concise summary of the disclosure in the task.
    ...
    (Continue with additional task IDs and their summaries)

    Upstream outputs:
    """,
    task_ids=[
        "Emissions_data_1",
        "Emissions_data_2",
        "Emissions_data_3",
        "Emissions_data_4",
        "Emissions_data_5",
        "Emissions_data_6",
    ],
)

Emissions_data_overview_formatter = partial(
    agent_formatter,
        prompt="Based on the input, SUMMARIZE the organization's information on emissions data, into ONE paragraph.",
    task_ids=[
        "Emissions_data_summary",
    ],
    session_id=session_id,
)

Emissions_data_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Emissions_data_1",
        "Emissions_data_2",
        "Emissions_data_3",
        "Emissions_data_4",
        "Emissions_data_5",
        "Emissions_data_6",
        "Emissions_data_7",
        "Emissions_data_8",
        "Emissions_data_9",
        "Emissions_data_10",
    ],
    session_id=session_id,
)

Scope_1_emissions_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE the organization's information on Scope 1 emissions, into ONE paragraph.",
    task_ids=[
        "Scope_1_emissions_summary",
    ],
    session_id=session_id,
)

Scope_1_emissions_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Emissions_data_1",
        "Emissions_data_2",
        "Emissions_reduction_targets_2",
        "Verification_1",
    ],
    session_id=session_id,
)

Scope_2_emissions_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE the organization's information on Scope 2 emissions, into ONE paragraph.",
    task_ids=[
        "Scope_2_emissions_summary",
    ],
    session_id=session_id,
)

Scope_2_emissions_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Emissions_data_3",
        "Emissions_data_4",
        "Emissions_data_5",
        "Emissions_reduction_targets_3",
        "Verification_2",
    ],
    session_id=session_id,
)

Scope_3_emissions_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE the organization's information on Scope 3 emissions, into ONE paragraph.",
    task_ids=[
        "Scope_3_emissions_summary",
    ],
    session_id=session_id,
)

Scope_3_emissions_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Emissions_data_6",
        "Emissions_reduction_targets_4",
        "Verification_3",
    ],
    session_id=session_id,
)

Emissions_reduction_targets_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE the organization's information on Emissions reduction targets, into ONE paragraph.",
    task_ids=[
        "Emissions_reduction_targets_summary",
    ],
    session_id=session_id,
)

Emissions_reduction_targets_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Emissions_reduction_targets_1",
        "Emissions_reduction_targets_2",
        "Emissions_reduction_targets_3",
        "Emissions_reduction_targets_4",
        "Emissions_reduction_targets_5",
    ],
    session_id=session_id,
)

Emissions_performance_overview_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Emissions_performance_1",
    ],
    session_id=session_id,
)

Emissions_reduction_initiatives_overview_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Emissions_reduction_initiatives_1",
    ],
    session_id=session_id,
)

Verification_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE the organization's information on Verification, into ONE paragraph.",
    task_ids=[
        "Verification_summary",
    ],
    session_id=session_id,
)

Verification_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Verification_1",
        "Verification_2",
        "Verification_3",
    ],
    session_id=session_id,
)

Governance_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE the organization's information on Governance, into ONE paragraph.",
    task_ids=[
        "Governance_summary",
    ],
    session_id=session_id,
)

Governance_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Governance_1",
        "Governance_2",
        "Governance_3",
    ],
    session_id=session_id,
)

Risks_opportunities_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE the organization's information on Risk opportunities, into ONE paragraph.",
    task_ids=[
        "Risks_opportunities_summary",
    ],
    session_id=session_id,
)

Risks_opportunities_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Risks_opportunities_1",
        "Risks_opportunities_2",
        "Risks_opportunities_3",
    ],
    session_id=session_id,
)

Carbon_pricing_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE the organization's information on Carbon pricing, into ONE paragraph.",
    task_ids=[
        "Carbon_pricing_summary",
    ],
    session_id=session_id,
)

Carbon_pricing_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Carbon_pricing_1",
        "Carbon_pricing_2",
        "Carbon_pricing_3",
    ],
    session_id=session_id,
)

agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
# openai = partial(post_request, post_url=openai_url, formatter=openai_formatter)
Emissions_data_openai_formatter = partial(
    post_request, post_url=openai_url, formatter=Emissions_data_openai_formatter
)
Emissions_data_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Emissions_data_overview_formatter
)
Emissions_data_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Emissions_data_summary_formatter
)
Scope_1_emissions_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Scope_1_emissions_overview_formatter
)
Scope_1_emissions_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Scope_1_emissions_summary_formatter
)
Scope_2_emissions_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Scope_2_emissions_overview_formatter
)
Scope_2_emissions_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Scope_2_emissions_summary_formatter
)
Scope_3_emissions_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Scope_3_emissions_overview_formatter
)
Scope_3_emissions_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Scope_3_emissions_summary_formatter
)
Emissions_reduction_targets_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Emissions_reduction_targets_overview_formatter
)
Emissions_reduction_targets_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Emissions_reduction_targets_summary_formatter
)
Emissions_performance_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Emissions_performance_overview_formatter
)
Emissions_reduction_initiatives_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Emissions_reduction_initiatives_overview_formatter
)
Verification_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Verification_overview_formatter
)
Verification_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Verification_summary_formatter
)
Governance_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Governance_overview_formatter
)
Governance_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Governance_summary_formatter
)
Risks_opportunities_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Risks_opportunities_overview_formatter
)
Risks_opportunities_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Risks_opportunities_summary_formatter
)
Carbon_pricing_overview_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Carbon_pricing_overview_formatter
)
Carbon_pricing_summary_agent = partial(
    post_request, post_url=automobile_esg_url, formatter=Carbon_pricing_summary_formatter
)

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="Automobile_ESG_compliance",
    default_args=default_args,
    description="Automobile ESG compliance Agent DAG",
    schedule_interval=None,
    tags=["Automobile_ESG_agent"],
    catchup=False,
) as dag:
    # wait_for_10_seconds = TimeDeltaSensor(
    # task_id='wait_for_10_seconds',
    # delta=timedelta(seconds=10),

    
# Scope 1 emissions data task
    Scope_1_emissions_data = task_PyOpr(
        task_id="Emissions_data_1",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Provide the total gross global emissions of Scope 1, measured in metric tons of CO2 equivalent (CO2e), for the specified organization."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 1 emissions breakdown task
    Scope_1_emissions_breakdown = task_PyOpr(
        task_id="Emissions_data_2",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Detail the organization's total global Scope 2 emissions, expressed in metric tons of CO2 equivalent (CO2e). Additionally, break down the organization's total Scope 2 emissions by providing data for each type of greenhouse gas involved. Include emissions distribution across different geographical locations (countries, areas, or regions), business divisions, facilities, and activities. Also, specify the source for each Global Warming Potential (GWP) value used in these calculations."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )   

# Scope 2 emissions data task
    Scope_2_emissions_data = task_PyOpr(
        task_id="Emissions_data_3",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Provide the total gross global emissions of Scope 2, measured in metric tons of CO2 equivalent (CO2e), for the specified organization."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions reporting Task
    Scope_2_emissions_reporting = task_PyOpr(
        task_id="Emissions_data_4",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Describe the organization's approach to reporting Scope 2 emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions breakdown Task
    Scope_2_emissions_breakdown = task_PyOpr(
        task_id="Emissions_data_5",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Provide a detailed breakdown of the organization's total gross global Scope 2 emissions. This breakdown should include data categorized by geographical location (country, area, or region), business division, specific business facility, and type of business activity."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions data Task
    Scope_3_emissions_data = task_PyOpr(
        task_id="Emissions_data_6",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Detail the organization's gross global Scope 3 emissions, encompassing all relevant categories: Purchased goods and services, Capital goods, Fuel-and-energy-related activities not included in Scope 1 or 2, Upstream and Downstream transportation and distribution, Waste generated in operations, Business travel, Employee commuting, Upstream and Downstream leased assets, Processing of sold products, Use of sold products, End-of-life treatment of sold products, Franchises, and Investments not applicable to financial services. Include any additional 'Other' categories, both upstream and downstream. It's crucial to not only account for these emissions but also to provide explicit explanations for any exclusions in the reporting."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Exclusions Task
    Exclusions = task_PyOpr(
        task_id="Emissions_data_7",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Identify if there are any sources of Scope 1, Scope 2, or Scope 3 emissions within the organization's chosen reporting boundary that are not included in the organization's emissions disclosure. These sources may include specific facilities, greenhouse gases (GHGs), activities, or geographic locations. If such exclusions exist, please provide detailed information about these sources for Scope 1, Scope 2, or Scope 3 emissions that are within the reporting boundary but omitted from the disclosure."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Biogenic carbon data Task
    Biogenic_carbon_data = task_PyOpr(
        task_id="Emissions_data_8",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Determine whether carbon dioxide emissions from biogenic carbon sources are relevant to the organization. If relevant, quantify the emissions from these biogenic carbon sources, reporting the amount in metric tons of CO2."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Emissions intensities Task
    Emissions_intensities = task_PyOpr(
        task_id="Emissions_data_9",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Provide a description of the organization's combined gross global emissions for Scope 1 and Scope 2 for the reporting year, expressed in metric tons of CO2 equivalent (CO2e) per unit of total revenue in the chosen currency. Additionally, include any other relevant intensity metrics that align with the specifics of the organization's business operations."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Emissions breakdown by subsidiary Task
    Emissions_breakdown_by_subsidiary = task_PyOpr(
        task_id="Emissions_data_10",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "The organization is required to provide disaggregated emission data for its subsidiaries, where applicable. This should include a detailed breakdown of the total Scope 1 and Scope 2 emissions for each individual subsidiary."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Total emission reduction target Task
    Total_emission_reduction_target = task_PyOpr(
        task_id="Emissions_reduction_targets_1",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input": {
                    "input": "Specify the organization's total emission reduction target, quantified in metric tons of CO2 equivalent (CO2e). Specify the target coverage area for this reduction goal. Options for target coverage include: Company-wide, Specific business division, Particular business activity, Individual site or facility, Defined country/area/region, Product-level, or Other (please specify)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 1 emissions reduction target Task
    Scope_1_emissions_reduction_target = task_PyOpr(
        task_id="Emissions_reduction_targets_2",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send": {
                "input":{
                    "input": "Specify the organization's Scope 1 emissions reduction target, expressed in metric tons of CO2 equivalent (CO2e). Additionally, define the target coverage for this reduction goal. The options for target coverage include: Company-wide, Business division, Business activity, Site/facility, Country/area/region, Product-level, or Other (please specify if applicable)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions reduction target Task
    Scope_2_emissions_reduction_target = task_PyOpr(
        task_id="Emissions_reduction_targets_3",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Detail the organization's Scope 2 emissions reduction target, quantified in metric tons of CO2 equivalent (CO2e). Define the target coverage area, with options including Company-wide, Business division, Business activity, Site/facility, Country/area/region, Product-level, or Other (specify if applicable). Also, indicate the accounting method used for Scope 2 emissions: Location-based or Market-based."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions reduction target Task
    Scope_3_emissions_reduction_target = task_PyOpr(
        task_id="Emissions_reduction_targets_4",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Provide the specifics of the organization's Scope 3 emissions reduction target, expressed in metric tons of CO2 equivalent (CO2e). Clarify the target coverage, choosing from the following options: Company-wide, Business division, Business activity, Site/facility, Country/area/region, Product-level, or Other (please specify if applicable). Additionally, include a detailed breakdown of the Scope 3 emissions reduction target, covering all applicable categories: Purchased goods and services, Capital goods, Fuel-and-energy-related activities (not included in Scope 1 or 2), Upstream transportation and distribution, Waste generated in operations, Business travel, Employee commuting, Upstream leased assets, Downstream transportation and distribution, Processing of sold products, Use of sold products, End-of-life treatment of sold products, Downstream leased assets, Franchises, Investments (excluding financial services), and any other relevant upstream or downstream categories."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Science-based target Task
    Science_based_target = task_PyOpr(
        task_id="Emissions_reduction_targets_5",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Confirm if the organization's emissions reduction target is recognized as a science-based carbon target by the Science Based Targets initiative (SBTi)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Completion of emission reduction targets Task
    Completion_of_emission_reduction_targets = task_PyOpr(
        task_id="Emissions_performance_1",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Provide details of the organization’s absolute emissions targets and progress against these targets."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Specific emission reduction instructions Task
    Specific_emission_reduction_instructions = task_PyOpr(
        task_id="Emissions_reduction_initiatives_1",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Determine whether the organization is currently undertaking any actions to reduce emissions. If so, provide a detailed description of these actions, including those in the preparation and/or implementation stages. Additionally, estimate the CO2e emission reductions anticipated from these actions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 1 emissions verification Task
    Scope_1_emissions_verification = task_PyOpr(
        task_id="Verification_1",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Confirm whether the organization utilizes a third-party verification or certification process for its Scope 1 emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions verification Task
    Scope_2_emissions_verification = task_PyOpr(
        task_id="Verification_2",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Verify if the organization employs a third-party verification or certification process for its Scope 2 emissions, specifying whether it applies to location-based or market-based emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions verification Task
    Scope_3_emissions_verification = task_PyOpr(
        task_id="Verification_3",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Establish whether the organization has a third-party verification or certification process in place for its Scope 3 emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Board oversight Task
    Board_oversight = task_PyOpr(
        task_id="Governance_1",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input": {
                    "input": "Confirm if there is oversight of climate-related issues at the board level within the organization. If affirmative, provide detailed information on the roles and responsibilities of each board member concerning climate-related issues and elaborate on the specifics of the board's oversight regarding these issues."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Management responsibility Task
    Management_responsibility = task_PyOpr(
        task_id="Governance_2",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input": {
                    "input": "Determine if the organization has established management positions, committees, or relevant departments that are responsible for addressing climate change-related issues below the board level."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Employee incentives Task
    Employee_incentives = task_PyOpr(
        task_id="Governance_3",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input": {
                    "input": "Verify if the organization offers incentives for managing climate-related issues, including reward systems upon achieving specified goals. If such incentives exist, please elaborate on the mechanisms and structures in place for managing and rewarding climate-related initiatives and achievements."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Management processes Task
    Management_processes = task_PyOpr(
        task_id="Risks_opportunities_1",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input": {
                    "input": "Detail the organization's methodology for identifying, assessing, and responding to climate-related risks and opportunities. This should include the approach used to define short, medium, and long-term time horizons, as well as the criteria for determining material financial or strategic impacts. Additionally, explain how the organization differentiates between physical risks and transition risks in its climate risk assessment process."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Risk disclosure Task
    Risk_disclosure = task_PyOpr(
        task_id="Risks_opportunities_2",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input": {
                    "input": "Detail any identified climate-related risks that could significantly affect the organization's financial stability or strategic direction. This should encompass risks across the value chain, including upstream and downstream supply chain aspects, direct operations, investments, and other relevant areas. Emphasize on outlining how these risks are determined to be substantial in their potential impact."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Opportunity disclosure Task
    Opportunity_disclosure = task_PyOpr(
        task_id="Risks_opportunities_3",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input": {
                    "input":"Elaborate on any identified climate-related risks that possess the potential to materially affect the organization's finances or strategic direction. Focus on specifying the actual financial impact these risks could have on the business, detailing their nature, potential severity, and the financial implications for the organization."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Carbon pricing systems Task
    Carbon_pricing_systems = task_PyOpr(
        task_id="Carbon_pricing_1",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Confirm if the organization's operations or activities fall under any carbon pricing systems, like Emissions Trading System (ETS), Cap & Trade, or Carbon Tax. If applicable, disclose which carbon pricing regulations impact the organization's operations and provide a detailed description of the emissions trading system and its implications for the organization."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Project-based carbon credits Task
    Project_based_carbon_credits = task_PyOpr(
        task_id="Carbon_pricing_2",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Ascertain whether the organization canceled any project-based carbon credits during the reporting year. If affirmative, detail the specifics of these project-based carbon credits that were canceled, including the nature and scale of the projects, the amount of credits canceled, and the rationale behind the cancellation."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Internal price on carbon Task
    Internal_price_on_carbon = task_PyOpr(
        task_id="Carbon_pricing_3",
        callable_func=agent,
        op_kwargs={
            "post_url": f"{BASE_URL}/your_endpoint",
            "data_to_send":{
                "input":{
                    "input": "Determine if the organization employs an internal carbon price. If so, provide comprehensive details on how this internal carbon price is utilized within the organization, including its application in decision-making, strategy development, and other relevant operational aspects."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Emissions_data_overview = task_PyOpr(
        task_id="Emissions_data_overview",
        callable_func = Emissions_data_overview_agent,
    )

    Emissions_data_summary = task_PyOpr(
        task_id="Emissions_data_summary",
        callable_func = Emissions_data_summary_agent,
    )    

    Scope_1_emissions_overview = task_PyOpr(
        task_id="Scope_1_emissions_overview",
        callable_func = Scope_1_emissions_overview_agent,
    )

    Scope_1_emissions_summary = task_PyOpr(
        task_id="Scope_1_emissions_summary",
        callable_func = Scope_1_emissions_summary_agent,
    )   

    Scope_2_emissions_overview = task_PyOpr(
        task_id="Scope_2_emissions_overview",
        callable_func = Scope_2_emissions_overview_agent,
    )

    Scope_2_emissions_summary = task_PyOpr(
        task_id="Scope_2_emissions_summary",
        callable_func = Scope_2_emissions_summary_agent,
    )

    Scope_3_emissions_overview = task_PyOpr(
        task_id="Scope_3_emissions_overview",
        callable_func = Scope_3_emissions_overview_agent,
    )

    Scope_3_emissions_summary = task_PyOpr(
        task_id="Scope_3_emissions_summary",
        callable_func = Scope_3_emissions_summary_agent,
    )

    Emissions_reduction_targets_overview = task_PyOpr(
        task_id="Emissions_reduction_targets_overview",
        callable_func = Emissions_reduction_targets_overview_agent,
    )

    Emissions_reduction_targets_summary = task_PyOpr(
        task_id="Emissions_reduction_targets_summary",
        callable_func = Emissions_reduction_targets_summary_agent,
    )

    Emissions_performance_overview = task_PyOpr(
        task_id="Emissions_performance_overview",
        callable_func = Emissions_performance_overview_agent,
    )

    Emissions_reduction_initiatives_overview = task_PyOpr(
        task_id="Emissions_reduction_initiatives_overview",
        callable_func = Emissions_reduction_initiatives_overview_agent,
    )

    Verification_overview = task_PyOpr(
        task_id="Verification_overview",
        callable_func = Verification_overview_agent,
    )

    Verification_summary = task_PyOpr(
        task_id="Verification_summary",
        callable_func = Verification_summary_agent,
    )

    Governance_overview = task_PyOpr(
        task_id="Governance_overview",
        callable_func = Governance_overview_agent,
    )

    Governance_summary = task_PyOpr(
        task_id="Governance_summary",
        callable_func = Governance_summary_agent,
    )

    Risks_opportunities_overview = task_PyOpr(
        task_id="Risks_opportunities_overview",
        callable_func = Risks_opportunities_overview_agent,
    )

    Risks_opportunities_summary = task_PyOpr(
        task_id="Risks_opportunities_summary",
        callable_func = Risks_opportunities_summary_agent,
    )

    Carbon_pricing_overview = task_PyOpr(
        task_id="Carbon_pricing_overview",
        callable_func = Carbon_pricing_overview_agent,
    )

    Carbon_pricing_summary = task_PyOpr(
        task_id="Carbon_pricing_summary",
        callable_func = Carbon_pricing_summary_agent,
    )

    Automobile_ESG_Merge = PythonOperator(
        task_id="automobile_esg_merge",
        python_callable = automobile_esg_merge,
    )

[
    Scope_1_emissions_data,
    Scope_1_emissions_breakdown,
    Scope_2_emissions_data,
    Scope_2_emissions_reporting,
    Scope_2_emissions_breakdown,
    Scope_3_emissions_data,
    Exclusions,
    Biogenic_carbon_data,
    Emissions_intensities,
    Emissions_breakdown_by_subsidiary,
] >> Emissions_data_summary

[
    Emissions_data_summary
] >> Emissions_data_overview

[
    Scope_1_emissions_data,
    Scope_1_emissions_breakdown,
    Scope_1_emissions_reduction_target,
    Scope_1_emissions_verification,
] >> Scope_1_emissions_summary

[
    Scope_1_emissions_summary
] >> Scope_1_emissions_overview

[
    Scope_2_emissions_data,
    Scope_2_emissions_reporting,
    Scope_2_emissions_breakdown,
    Scope_2_emissions_reduction_target,
    Scope_2_emissions_verification,
] >> Scope_2_emissions_summary

[
    Scope_2_emissions_summary
] >> Scope_2_emissions_overview

[
    Scope_3_emissions_data,
    Scope_3_emissions_reduction_target,
    Scope_3_emissions_verification,
] >> Scope_3_emissions_summary

[
    Scope_3_emissions_summary
] >> Scope_3_emissions_overview

[
    Total_emission_reduction_target,
    Scope_1_emissions_reduction_target,
    Scope_2_emissions_reduction_target,
    Scope_3_emissions_reduction_target,
    Science_based_target,
]   >> Emissions_reduction_targets_summary

[
    Emissions_reduction_targets_summary,
]   >> Emissions_reduction_targets_overview

[
    Completion_of_emission_reduction_targets,
]   >> Emissions_performance_overview

[
    Specific_emission_reduction_instructions,
]   >> Emissions_reduction_initiatives_overview

[
    Scope_1_emissions_verification,
    Scope_2_emissions_verification,
    Scope_3_emissions_verification,
]   >> Verification_summary

[
    Verification_summary,
]   >> Verification_overview

[
    Board_oversight,
    Management_responsibility,
    Employee_incentives,
]   >> Governance_summary

[
    Governance_summary,
]   >> Governance_overview

[
    Management_processes,
    Risk_disclosure,
    Opportunity_disclosure,
]   >> Risks_opportunities_summary

[
    Risks_opportunities_summary,
]   >> Risks_opportunities_overview

[
    Carbon_pricing_systems,
    Project_based_carbon_credits,
    Internal_price_on_carbon,
]   >> Carbon_pricing_summary

[
    Carbon_pricing_summary,
]   >> Carbon_pricing_overview

[
    Emissions_data_overview,
    Scope_1_emissions_overview,
    Scope_2_emissions_overview,
    Scope_3_emissions_overview,
    Emissions_reduction_targets_overview,
    Emissions_performance_overview,
    Emissions_reduction_initiatives_overview,
    Verification_overview,
    Governance_overview,
    Risks_opportunities_overview,
    Carbon_pricing_overview,
] >> Automobile_ESG_Merge
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
session_id = "20240318"

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
    # retry_delay=timedelta(seconds=3),
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
            "input": {"input":prompt + "\n\n INPUT:" + content},
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
    prompt = "Translate the following texts related to esg reports into native Chinese:" + "\n\n" + content
    formatted_data = {"input": prompt}
    return formatted_data

def Emissions_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Scope_emissions_summary")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Other_emissions_summary")["output"]["output"]
    concatenated_result = (
        "1 Emissions\n"
        + "1.1 Scope emissions data\n"
        + result_0
        + "\n\n"
        + "1.2 Other emissions data\n"
        + result_1
        + "\n\n"
    )
    return concatenated_result

def Emissions_breakdown_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Scope_1_emissions_breakdown_summary")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Scope_2_emissions_breakdown_summary")["output"]["output"]
    result_2 = ti.xcom_pull(task_ids="Scope_3_emissions_breakdown_summary")["output"]["output"]
    concatenated_result = (
        "2 Emissions breakdown\n"
        + "2.1 Scope 1 emissions breakdown\n"
        + result_0
        + "\n\n"
        + "2.2 Scope 2 emissions breakdown\n"
        + result_1
        + "\n\n"
        + "2.3 Scope 3 emissions breakdown\n"
        + result_2
        + "\n\n"
    )
    return concatenated_result

def Target_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Target_summary")["output"]["output"]
    concatenated_result = (
        "3 Target\n"
        + "3.1 Emissions reduction target\n"
        + result_0
        + "\n\n"
    )
    return concatenated_result

def Initiatives_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Initiatives_overview")["output"]["output"]
    concatenated_result = (
        "4 Initiatives\n"
        + "4.1 Reduction instructions\n"
        + result_0
        + "\n\n"
    )
    return concatenated_result

def Verification_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Verification_summary")["output"]["output"]
    concatenated_result = (
        "5 Verification\n"
        + "5.1 Scope verification\n"
        + result_0
        + "\n\n"
    )
    return concatenated_result

def Governance_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Governance_summary")["output"]["output"]
    concatenated_result = (
        "6 Governance\n"
        + result_0
        + "\n\n"
    )
    return concatenated_result

def Risks_opportunities_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Risks_opportunities_summary")["output"]["output"]
    concatenated_result = (
        "7 Risks and opportunities\n"
        + result_0
        + "\n\n"
    )
    return concatenated_result

def Carbon_pricing_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Carbon_pricing_summary")["output"]["output"]
    concatenated_result = (
        "8 Carbon pricing\n"
        + result_0
        + "\n\n"
    )
    return concatenated_result


def merge_EN(ti):
    result_0 = ti.xcom_pull(task_ids="Emissions_output_merge")
    result_1 = ti.xcom_pull(task_ids="Emissions_breakdown_output_merge")
    result_2 = ti.xcom_pull(task_ids="Target_output_merge")
    result_3 = ti.xcom_pull(task_ids="Initiatives_output_merge")
    result_4 = ti.xcom_pull(task_ids="Verification_output_merge")
    result_5 = ti.xcom_pull(task_ids="Governance_output_merge")
    result_6 = ti.xcom_pull(task_ids="Risks_opportunities_output_merge")
    result_7 = ti.xcom_pull(task_ids="Carbon_pricing_output_merge")
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
    )

    markdown_file_name = "Automobile_esg_compliance_report_EN.md"

    # Save the model's response to a Markdown file
    with open(markdown_file_name, "w") as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result

def merge_CN(ti):
    result_0 = ti.xcom_pull(task_ids="Emissions_translate")["output"]["content"]
    result_1 = ti.xcom_pull(task_ids="Emissions_breakdown_translate")["output"]["content"]
    result_2 = ti.xcom_pull(task_ids="Target_translate")["output"]["content"]
    result_3 = ti.xcom_pull(task_ids="Initiatives_translate")["output"]["content"]
    result_4 = ti.xcom_pull(task_ids="Verification_translate")["output"]["content"]
    result_5 = ti.xcom_pull(task_ids="Governance_translate")["output"]["content"]
    result_6 = ti.xcom_pull(task_ids="Risks_opportunities_translate")["output"]["content"]
    result_7 = ti.xcom_pull(task_ids="Carbon_pricing_translate")["output"]["content"]
    
    concatenated_result = (result_0 + "\n\n" + result_1 + "\n\n" + result_2 + "\n\n" + result_3 + "\n\n" + result_4 + "\n\n" + result_5 + "\n\n" + result_6 + "\n\n" + result_7 + "\n\n"
    )
    markdown_file_name = "Automobile_esg_compliance_report_CN.md"

    # Save the model's response to a Markdown file
    with open(markdown_file_name, "w") as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result

def save_file(ti, file_name: str = "Automobile_esg_compliance_report_CN.md"):
    data = ti.xcom_pull(task_ids="Translate")["output"]["content"]
    with open(file_name, "w") as markdown_file:
        markdown_file.write(data)
    return data

Rules_prompt = """Must strictly abide by the following 4 rules prompt guidelines.
1. Summarize [Specific company name]'s data across various categories for [specific year], output must AVOIDE extraneous details to ensure conciseness and a professional tone.
2. If the data is not disclosed or no relevant data is found, simply output: [Specific Task ID] data for [Specific Company Name] was not disclosed in [Specific Year].
3. The output result MUST NOT contains "#", "-", "*".
4. Further details should be directed to an "Additional Information" section. The focus should be on relaying integral but previously unmentioned aspects of the report, ensuring a comprehensive overview is provided."""

# Summary formatters
Scope_emissions_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
1.1.1 Total emissions data (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format. 
Example: "[Specific company name]'s Total greenhouse gas emissions in [specific year] are [X metric tons CO2e]".\n

1.1.2 Scope 1 emissions data (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format. 
Example: "[Specific company name]'s Total Scope 1 greenhouse gas emissions in [specific year] are [X metric tons CO2e]". \n

1.1.3 Scope 2 emissions data (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format. 
Example: "[Specific company name]'s Total Scope 2 greenhouse gas emissions in [specific year] are [X metric tons CO2e]". \n

1.1.4 Scope 3 emissions data (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format. 
Example: "[Specific company name]'s Total Scope 3 greenhouse gas emissions in [specific year] are [X metric tons CO2e]". \n

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Total_emissions_data",
        "Scope_1_emissions_data",
        "Scope_2_emissions_data",
        "Scope_3_emissions_data",
    ],
    session_id=session_id,
)

Other_emissions_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
1.2.1 Biogenic carbon data (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format. 
Example: "[Specific company name]'s biogenic carbon emissions in [specific year] are [X metric tons CO2e]".\n

1.2.2 Subsidiary (Both the title and the preceding serial number need to be retained)\n
Objective: Disclose scope 1 and 2 emission data of subsidiaries. 
Example:"
(1) Apple, Inc. (Parent Company), Ticker Symbol: TSLA, Scope 1 Emissions: 50,000 metric tons of CO2 equivalent (CO2e), Scope 2 Emissions: 120,000 metric tons of CO2e.
(1.1)SolarCity Corporation (Subsidiary), CUSIP Number: 123456789, Scope 1 Emissions: 1,000 metric tons of CO2e, Scope 2 Emissions: 5,000 metric tons of CO2e.
(1.2)Gigafactory 1 (Operational Entity), LEI Number: 549300LXAVMOP3LP, Scope 1 Emissions: 10,000 metric tons of CO2e, Scope 2 Emissions: 20,000 metric tons of CO2e."\n

1.2.3 Emissions performance (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text should be output in this format. 
Example: "Scope 1 Emissions Comparison: Reporting Year: [X metric tons CO2e], Previous Year: [Y metric tons CO2e], Change and Trends: [Analysis]
Scope 2 Emissions Comparison: Reporting Year: [A metric tons CO2e], Previous Year: [B metric tons CO2e], Change and Trends: [Analysis]."\n

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Biogenic_carbon_data",
        "Subsidiary",
        "Emissions_performance",
    ],
    session_id=session_id,
)

Scope_1_emissions_breakdown_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
2.1.1 Scope 1 emissions by GHGs (Both the title and the preceding serial number need to be retained)\n
Objective: Report scope 1 emissions in metric tons of CO2e for each greenhouse gas: CO2, CH4, N2O, HFCs, PFCs, SF6, NF3, other ghgs(includes any additional greenhouse gases not categorized above).\n
Example: "Apple, Inc. 
CO2: 45,000 metric tons CO2e,
CH4: 500 metric tons CO2e,
N2O: 250 metric tons CO2e, 
HFCs: 150 metric tons CO2e, 
PFCs: 100 metric tons CO2e, 
SF6: 75 metric tons CO2e, 
NF3: 50 metric tons CO2e, 
Other GHGs: 200 metric tons CO2e"\n

2.1.2 Scope 1 emissions by Region (Both the title and the preceding serial number need to be retained)\n
Objective: Detail scope 1 emissions for each region in metric tons of CO2e: United States, China, European Union, Other Countries/Areas/Regions (include emissions data for any regions not initially listed).\n
Example: "Apple, Inc. 
United States: 40,000 metric tons of CO2e, 
China: 30,000 metric tons of CO2e, 
European Union: 20,000 metric tons of CO2e, 
Other Countries/Areas/Regions: 10,000 metric tons of CO2e."\n

2.1.3 Scope 1 emissions by Business (Both the title and the preceding serial number need to be retained)\n
Objective: Report scope 1 emissions in metric tons of CO2e for: Business Division, Facility, Activity Type, [Additional Division/Facility/Activity] (detail any other relevant entities contributing to the organization's Scope 1 emissions).\n
Example: "Apple, Inc. 
(1)Automotive Division:
(1.1)Manufacturing Facilities: 
Fremont Factory: 30,000 metric tons of CO2e, Shanghai Gigafactory: 25,000 metric tons of CO2e, Berlin Gigafactory: 20,000 metric tons of CO2e.
(1.2)Vehicle Testing Activities
Crash Test Sites: 500 metric tons of CO2e, Road Test Drives: 2,000 metric tons of CO2e
(2)Energy Products Division:
(2.1)Solar Panel Manufacturing:
Buffalo Gigafactory: 5,000 metric tons of CO2e
(2.2)Battery Production
Nevada Gigafactory: 15,000 metric tons of CO2e\n
(3)Research & Development Activities
(3.1)R&D Facilities
Palo Alto Headquarters: 1,000 metric tons of CO2e
(3.2)Prototype Development
Lab Experiments: 300 metric tons of CO2e"\n

Additional Information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Scope_1_ghgs",
        "Scope_1_region",
        "Scope_1_business",
    ],
    session_id=session_id,
)

Scope_2_emissions_breakdown_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
2.2.1 Scope 2 emissions by Reporting Method (Both the title and the preceding serial number need to be retained)\n
Objective: Describe emissions according to the: Location-Based Method, Market-Based Method. Provide clear definitions or values for each method used.\n
Example: "GlobalTech Solutions.
(1)Location-Based Method: GlobalTech Solutions operates in various regions, each with its own energy mix for electricity generation. The Location-Based Method calculates Scope 2 emissions by using regional or national average emission factors for the grid electricity consumed. For instance, in Region A, where GlobalTech has a significant operation, the average grid emission factor is 500 grams of CO2 per kilowatt-hour (gCO2/kWh). In the reporting year, GlobalTech consumed 10,000 MWh of electricity in Region A. Applying the location-based method, GlobalTech's Scope 2 emissions for Region A are calculated as:
(2)Market-Based Method: In contrast, the Market-Based Method reflects GlobalTech's choices and actions to procure electricity from specific sources with potentially lower emission factors. This method considers contracts for renewable energy, green tariffs, and renewable energy certificates (RECs) that the company purchases. For example, GlobalTech Solutions purchased RECs corresponding to 8,000 MWh of its electricity consumption in Region A, which is sourced from wind power with an emission factor of 0 gCO2/kWh. The remaining 2,000 MWh, not covered by RECs, still uses the regional average emission factor."\n

2.2.2 Scope 2 emissions by Region (Both the title and the preceding serial number need to be retained)\n
Objective: Detail emissions in metric tons of CO2e for: United States, China, European Union, Other Countries/Areas/Regions. Include emissions data for any regions not initially listed under "Other Countries/Areas/Regions". \n
Example: "Apple, Inc. 
United States: 40,000 metric tons of CO2e, 
China: 30,000 metric tons of CO2e, 
European Union: 20,000 metric tons of CO2e, 
Other Countries/Areas/Regions: 10,000 metric tons of CO2e."\n

2.2.3 Scope 2 emissions by Business (Both the title and the preceding serial number need to be retained)\n
Report emissions in metric tons of CO2e for: Business Division, Facility, Activity Type, [Any additional Division/Facility/Activity].\n
Example: "Apple, Inc. 
(1)Automotive Division:
(1.1)Manufacturing Facilities: 
Fremont Factory: 30,000 metric tons of CO2e, Shanghai Gigafactory: 25,000 metric tons of CO2e, Berlin Gigafactory: 20,000 metric tons of CO2e.
(1.2)Vehicle Testing Activities
Crash Test Sites: 500 metric tons of CO2e, Road Test Drives: 2,000 metric tons of CO2e
(2)Energy Products Division:
(2.1)Solar Panel Manufacturing:
Buffalo Gigafactory: 5,000 metric tons of CO2e
(2.2)Battery Production
Nevada Gigafactory: 15,000 metric tons of CO2e
(3)Research & Development Activities
(3.1)R&D Facilities
Palo Alto Headquarters: 1,000 metric tons of CO2e
(3.2)Prototype Development
Lab Experiments: 300 metric tons of CO2e"\n

Additional Information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Scope_2_reporting",
        "Scope_2_region",
        "Scope_2_business",
    ],
    session_id=session_id,
)

Scope_3_emissions_breakdown_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
2.3.1 Scope 3 Calculation methodology (Both the title and the preceding serial number need to be retained)\n
Objective: Report scope 3 calculation methodology.\n

2.3.2 Scope 3 emissions data (Both the title and the preceding serial number need to be retained)\n
Objective: Reporting Scope 3 emissions data for [Specific company name] in [Specific year], retain the given structure and title formats. Each category should list emissions in metric tons of CO2e.\n
Example: "Apple, Inc. 
(1)Purchased Goods and Services: Emissions: 500,000 metric tons of CO2e
(2)Capital Goods: Emissions: 80,000 metric tons of CO2e
(3)Fuel- and Energy-Related Activities (Not Included in Scope 1 or 2): Emissions: 30,000 metric tons of CO2e
(4)Upstream Transportation and Distribution: Emissions: 100,000 metric tons of CO2e
(5)Waste Generated in Operations: Emissions: 5,000 metric tons of CO2e
(6)Business Travel: Emissions: 15,000 metric tons of CO2e
(7)Employee Commuting: Emissions: 20,000 metric tons of CO2e
(8)Upstream Leased Assets: Emissions: Not applicable for Apple in 2023.
(9)Downstream Transportation and Distribution: Emissions: 50,000 metric tons of CO2e
(10)Processing of Sold Products: Emissions: 200,000 metric tons of CO2e
(11)Use of Sold Products: Emissions: 2,000,000 metric tons of CO2e
(12)End-of-Life Treatment of Sold Products: Emissions: 250,000 metric tons of CO2e
(13)Downstream Leased Assets: Emissions: Not disclosed.
(14)Franchises: Emissions: Not applicable for Apple in 2023.
(15)Investments: Emissions: 100,000 metric tons of CO2e"
If emissions data for multiple categories are not disclosed: "[Specific company name]'s Scope 3 emissions data for the following categories was not disclosed in [Specific year]: Purchased Goods and Services, Capital Goods, Fuel-and-Energy-Related Activities, etc."\n

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Scope_3_calculation_methodology",
        "Scope_3_emissions_breakdown"
    ],
    session_id=session_id,
)

Target_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
3.1.1 Total emissions reduction target (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format.\n
Example: "Total emissions reduction target: [Value in metric tons CO2e]."  
Total emissions reduction target include: Entire Company, Specific Business Division, Certain Business Activity, Single Site or Facility, Specified Country/Area/Region, Product-Specific, Other Category. \n

3.1.2 Scope 1 emissions reduction target (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format. \n
Example: "Scope 1 emissions reduction target: [Value in metric tons CO2e]." 
Scope 1 emissions reduction target include: Entire Company, Specific Business Division, Certain Business Activity, Single Site or Facility, Specified Country/Area/Region, Product-Specific, Other Category. \n

3.1.3 Scope 2 emissions reduction target (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format. \n
Example: "Scope 2 emissions reduction target: [Value in metric tons CO2e]." 
Scope 2 emissions reduction target include: Entire Company, Specific Business Division, Certain Business Activity, Single Site or Facility, Specified Country/Area/Region, Product-Specific, Other Category. \n

3.1.4 Scope 3 emissions reduction target (Both the title and the preceding serial number need to be retained)\n
Standard output format example, text must be output strictly in this format. \n
Example: "Scope 3 emissions reduction target: [Value in metric tons CO2e]." 
Scope 3 emissions reduction target include: Entire Company, Specific Business Division, Certain Business Activity, Single Site or Facility, Specified Country/Area/Region, Product-Specific, Other Category.\n

3.1.5 Science-based target (Both the title and the preceding serial number need to be retained)\n
Objective: Indicate the validation status of the emissions reduction target.
Confirmed: Validated by the Science Based Targets initiative (SBTi), demonstrating alignment with climate science and global climate action.
Not Confirmed: Targets set but not validated by the SBTi, possibly due to ongoing validation, failure to meet SBTi criteria, or non-participation in the SBTi process.\n

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Total_emissions_reduction_target",
        "Scope_1_emissions_reduction_target",
        "Scope_2_emissions_reduction_target",
        "Scope_3_emissions_reduction_target",
        "Science_based_target",
    ],
    session_id=session_id,
)

Initiatives_overview_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
4.1.1 Specific emissions reduction instructions (Both the title and the preceding serial number need to be retained)\n
Objective: Detail the steps being taken to reduce emissions, including actions during the planning and execution phases, and the expected carbon reductions.\n
Example: "Apple, Inc.
(1)Planning Phase: 
(1.1)In-Depth Carbon Footprint Assessment: Apple undertook an exhaustive analysis of its carbon footprint, pinpointing primary sources of emissions across vehicle production, facility operations, and the broader supply chain. This rigorous assessment served as the foundation for our tailored emissions reduction strategy.
(1.2)Engagement with Stakeholders: Apple's strategy has been enriched by active dialogue with a diverse group of stakeholders, including consumers, shareholders, and environmental advocates. Their insights have been instrumental in refining our emissions reduction roadmap.
(2)Execution Phase:
(2.1)Apple Solar Initiative: Apple has significantly invested in renewable energy, including the installation of over 1 million solar panels across our facilities worldwide and entering into power purchase agreements for wind energy that exceed 500 MW. Impact: These initiatives have led to a quantifiable reduction of over 2 million metric tons of CO2e annually.
(2.2)Facility Energy Optimization:Apple has implemented energy-saving measures across all facilities, including LED lighting upgrades, HVAC system optimizations, and the introduction of energy-efficient manufacturing processes. Impact: These improvements have resulted in estimated emissions savings of 300,000 metric tons of CO2e annually.
(2.3)Fleet Electrification: : Apple has fully electrified its service and delivery fleet, replacing over 5,000 conventional vehicles with electric models. Impact: This transition has contributed to a reduction of approximately 50,000 metric tons of CO2e emissions annually.
(2.4)Sustainable Supply Chain Initiatives:Apple has worked with suppliers to adopt sustainable sourcing criteria and improve logistics efficiency, reducing overall supply chain emissions. Impact: These strategies have led to an estimated reduction of 400,000 metric tons of CO2e annually."\n

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Specific_emissions_reduction_instructions",
    ],
    session_id=session_id,
)

Verification_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
5.1.1 Scope 1 Emissions Verification (Both the title and the preceding serial number need to be retained)\n
Objective: This section should contain verification details or a statement regarding the absence of data for Scope 1 emissions. Must contain "Verification Body", "Certification Standards", "Verification Statement", and "Cross-verification and Stakeholder Feedback".\n
Example: "
(1)Verification Body: The third-party verification for Apple's Scope 1 emissions in 2022 was conducted by GreenVerify Inc., a globally recognized environmental consulting firm with expertise in GHG emissions verification.
(2)Certification Standards: The verification process adhered to the ISO 14064-3 standard, which provides guidance for the verification and validation of greenhouse gas assertions. GreenVerify Inc. also utilized The Greenhouse Gas Protocol's Corporate Accounting and Reporting Standard to ensure a comprehensive evaluation.
(3)Verification Statement: GreenVerify Inc. has issued a statement confirming that Apple's reported Scope 1 emissions for 2022 are accurate, complete, and conform to the specified standards. The verification process involved a thorough review of Apple's emission calculation methodologies, data collection processes, and emission factor sourcing.
(4)Cross-verification and Stakeholder Feedback: In addition to third-party verification, Apple engages in cross-verification practices by comparing reported emissions against internal records and past reports for consistency. We also welcome and incorporate feedback from stakeholders, including environmental groups, investors, and regulatory bodies, to continually enhance the accuracy and reliability of our environmental reporting."\n

5.1.2 Scope 2 Emissions Verification (Both the title and the preceding serial number need to be retained)\n
Objective: This section should contain verification details or a statement regarding the absence of data for Scope 2 emissions. Must contain "Verification Body", "Certification Standards", "Verification Statement", and "Cross-verification and Stakeholder Feedback".\n
Example: "
(1)Verification Body: The third-party verification for Apple's Scope 2 emissions in 2022 was conducted by GreenVerify Inc., a globally recognized environmental consulting firm with expertise in GHG emissions verification.
(2)Certification Standards: The verification process adhered to the ISO 14064-3 standard, which provides guidance for the verification and validation of greenhouse gas assertions. GreenVerify Inc. also utilized The Greenhouse Gas Protocol's Corporate Accounting and Reporting Standard to ensure a comprehensive evaluation.
(3)Verification Statement: GreenVerify Inc. has issued a statement confirming that Apple's reported Scope 2 emissions for 2022 are accurate, complete, and conform to the specified standards. The verification process involved a thorough review of Apple's emission calculation methodologies, data collection processes, and emission factor sourcing.
(4)Cross-verification and Stakeholder Feedback: In addition to third-party verification, Apple engages in cross-verification practices by comparing reported emissions against internal records and past reports for consistency. We also welcome and incorporate feedback from stakeholders, including environmental groups, investors, and regulatory bodies, to continually enhance the accuracy and reliability of our environmental reporting."

5.1.3 Scope 3 Emissions Verification (Both the title and the preceding serial number need to be retained)\n
Objective: This section should contain verification details or a statement regarding the absence of data for Scope 3 emissions. Must contain "Verification Body", "Certification Standards", "Verification Statement", and "Cross-verification and Stakeholder Feedback".\n
Example: "
(1)Verification Body: The third-party verification for Apple's Scope 3 emissions in 2022 was conducted by GreenVerify Inc, a globally recognized environmental consulting firm with expertise in GHG emissions verification.
(2)Certification Standards: The verification process adhered to the ISO 14064-3 standard, which provides guidance for the verification and validation of greenhouse gas assertions. GreenVerify Inc. also utilized The Greenhouse Gas Protocol's Corporate Accounting and Reporting Standard to ensure a comprehensive evaluation.
(3)Verification Statement: GreenVerify Inc. has issued a statement confirming that Apple's reported Scope 3 emissions for 2022 are accurate, complete, and conform to the specified standards. The verification process involved a thorough review of Apple's emission calculation methodologies, data collection processes, and emission factor sourcing.
(4)Cross-verification and Stakeholder Feedback: In addition to third-party verification, Apple engages in cross-verification practices by comparing reported emissions against internal records and past reports for consistency. We also welcome and incorporate feedback from stakeholders, including environmental groups, investors, and regulatory bodies, to continually enhance the accuracy and reliability of our environmental reporting."\n

Additional Information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Scope_1_emissions_reduction",
        "Scope_2_emissions_reduction",
        "Scope_3_emissions_reduction",
    ],
    session_id=session_id,
)

Governance_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
6.1.1 Board oversight (Both the title and the preceding serial number need to be retained)\n
Objective: Ascertain the presence of board-level oversight concerning climate-related issues within the organization. Board Member Responsibilities: For each relevant board member, detail their specific roles and responsibilities in relation to climate-related issues. Include how these duties are integrated into their overall strategic duties and decision-making processes. Oversight Mechanisms: Describe the mechanisms and strategies employed by the board to monitor, address, and integrate climate-related risks and opportunities into the company's operations and policies.\n
Example: "Apple, Inc.
(1)Elon Musk (CEO and Product Architect): Tasked with steering Apple towards sustainability and climate innovation. Embeds climate considerations within strategic planning and product development, and provides regular updates on sustainability achievements.
(2)Robyn Denholm (Chair of the Board): Leads board discussions on climate risks and opportunities, ensuring integration of climate considerations into governance. Heads the Sustainability Committee for direct oversight of climate strategies."\n

6.2.1 Management responsibility (Both the title and the preceding serial number need to be retained)\n
Objective: This assessment aims to identify and detail the roles, committees, or departments within Apple that are tasked with handling climate change-related issues. If oversight exists, provide a comprehensive description of this oversight. \n
Example: "
(1)Management Roles and Committees: Environmental, Social, and Governance (ESG) Committee: Apple has established an ESG Committee responsible for overseeing the company's approach to environmental issues, including climate change. This committee works closely with various departments to integrate climate-related considerations into Apple's strategic planning and operational practices.
(2)Chief Sustainability Officer (CSO): Reporting directly to the CEO, the CSO is tasked with developing and implementing Apple's climate strategy. This includes identifying risks and opportunities related to Apple's operations, supply chain, and product lifecycle, as well as setting company-wide sustainability targets.
(3)Areas of Responsibility
(3.1)Operations: The CSO oversees initiatives aimed at reducing the carbon footprint of Apple's manufacturing and administrative operations, such as transitioning to renewable energy sources and improving energy efficiency.
(3.2)Investing Activities: The Investment Committee, guided by the Chief Investment Officer (CIO), evaluates and incorporates climate risks and opportunities into Apple's investment strategies, ensuring alignment with the company's sustainability goals.
(3.3)Banking Activities: While Apple's core business does not directly involve banking, its financial management strategies, overseen by the Chief Financial Officer (CFO), include considerations of climate-related financial risks and opportunities.
(3.4)Insurance Underwriting Activities: As Apple ventures into insurance for its vehicles, the underwriting process incorporates climate risks, particularly in relation to the impacts of severe weather events on insurance claims and pricing.
(3.5)Supply Chain: The Operations team, led by the Chief Operations Officer (COO), is responsible for optimizing the supply chain from a sustainability perspective, including engaging suppliers on climate issues.
(4)Reporting Systems
(4.1)Board of Directors Report: The ESG Committee and the CSO provide regular updates on climate-related issues directly to the Board of Directors, ensuring that strategic decisions reflect Apple's commitment to addressing climate change.
(4.2)CEO Reporting System: The CSO reports to the CEO, offering insights into the company's climate strategy and its implementation across various departments.
(4.3)Risk-CRO Reporting System: Climate-related risks are assessed and managed in conjunction with the Chief Risk Officer (CRO), with a focus on identifying and mitigating potential impacts on Apple's operations and financial performance.
(4.4)Finance-CFO Reporting Line: The CFO receives reports on the financial implications of climate risks and opportunities, incorporating this information into financial planning and risk management strategies.
(4.5)Investment-CIO Reporting Line: The CIO is informed about climate-related investment risks and opportunities, guiding Apple's investment decisions towards sustainability.
(4.6)Operations-COO Reporting Line: The COO oversees the integration of climate considerations into operational processes and supply chain management, aiming to enhance efficiency and reduce environmental impact."\n

6.3.1 Employee incentives (Both the title and the preceding serial number need to be retained)\n
Objective: Detail the incentives and rewards provided to employees for contributing to the company's sustainability and climate goals. This includes stock options, bonuses, salary increases, or other forms of recognition tied to environmental performance.\n
Example: "
(1)Stock Options and Equity Awards: Apple is known for offering stock options and equity awards to its employees, from engineers to executives. These stock options become more valuable as the company achieves its environmental milestones, such as increasing the production of electric vehicles (EVs) or advancing solar energy products. This not only rewards employees financially but also ties their contributions directly to Apple's mission-related achievements.
(2)Salary Increases and Bonuses: Employees who contribute significantly to projects that advance Apple's sustainability goals may receive salary increases or bonuses. This includes innovations in battery technology, improvements in energy efficiency, or successful implementation of sustainability practices within the company's operations."\n

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Board_oversight",
        "Management_responsibility",
        "Employee_incentives",
    ],
    session_id=session_id,
)

Risks_opportunities_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
7.1.1 Management processes (Both the title and the preceding serial number need to be retained). Must contains"Defining Time Horizons", "Criteria for Material Impact Assessment", "Categorization of Climate Risks".\n
Example: "Apple, Inc. 
(1)Defining Time Horizons:
(1.1) Short-Term Strategy Group: Focuses on immediate actions (0-2 years) to mitigate risks and leverage opportunities arising from current climate trends and policy shifts.
(1.2) Medium-Term Planning Committee: Develops strategies for the 3-5 year horizon, preparing Apple for transitional market changes and regulatory developments.
(1.3) Long-Term Vision Council: Sets the direction for Apple's innovation and business model adaptation over a 6-10 year period, anticipating major shifts in the global climate and energy landscape.
(2)Criteria for Material Impact Assessment:
(2.1) Financial Impact Analysis Team: Evaluates potential financial implications of climate risks and opportunities, assessing their impact on revenue, costs, and investment needs.
(2.2) Strategic Impact Review Board: Considers how climate factors affect Apple's market positioning, regulatory compliance, and innovation trajectory.
(3)Categorization of Climate Risks:
(3.1) Physical Risk Task Force: Identifies and assesses risks related to physical climate phenomena, such as extreme weather events and long-term climate shifts, evaluating their potential impact on Apple's operations and supply chain.
(3.2) Transition Risk Advisory Group: Analyzes risks associated with the transition to a low-carbon economy, including regulatory changes, market dynamics, and technological advancements, and their implications for Apple's business model and product offerings."\n

7.2.1 Risk disclosure (Both the title and the preceding serial number need to be retained)\n
Objective: The evaluation spans risks across the entire value chain, including upstream and downstream supply chain aspects, direct operations, investments, and other pertinent sectors, with an emphasis on the criteria used to deem these risks substantial. Must contains"Comprehensive Risk Identification across the Value Chain", "Criteria for Determining Substantial Impact".\n
Example: "Apple, Inc. 
(1)Comprehensive Risk Identification across the Value Chain:
(1.1)Supply Chain Vulnerability Assessment: Apple's risk assessment team conducts thorough evaluations of upstream and downstream supply chain aspects to identify vulnerabilities to climate-related risks such as raw material scarcity, production disruptions due to extreme weather events, and regulatory changes affecting suppliers.
(1.2)Direct Operations Analysis: Focuses on risks directly impacting Apple's manufacturing facilities and corporate operations, including energy cost volatility and operational disruptions due to climate phenomena.
(1.3)Investment and Market Position Review: Assesses climate-related risks to Apple's investment portfolio and potential impacts on market demand for Apple's products, considering shifts in consumer preferences towards sustainability and regulatory changes promoting electric vehicles.
(2)Criteria for Determining Substantial Impact:
(2.1)Financial Impact Quantification: Apple employs models to quantify potential financial impacts of identified risks, considering factors like cost increases, potential revenue losses, and capital expenditure requirements for mitigation measures.
(2.2)Strategic Impact Analysis: Evaluates how identified risks could alter Apple's strategic direction, including impacts on competitive positioning, regulatory compliance, and alignment with sustainability objectives.
(3)Risk Determination and Prioritization:
(3.1)Risk Scoring Matrix: Utilizes a risk scoring matrix that ranks identified risks based on their likelihood and potential impact, prioritizing risks that pose significant threats to financial stability or strategic direction.
3.2)Scenario Planning: Engages in scenario planning exercises to understand the broader implications of climate risks under various future scenarios, enhancing Apple's strategic resilience and adaptability.
(4)Key Climate-Related Risks Identified:
(4.1)Physical Risks: Include increased operational and supply chain disruptions from extreme weather events and long-term shifts in climate patterns affecting raw material availability and production capacity.
(4.2)Transition Risks: Comprise regulatory risks related to the adoption of stricter emissions standards, financial risks from shifts in investment towards sustainable technologies, and market risks from changes in consumer behavior favoring low-carbon products.
(5)Management and Mitigation Strategies:
(5.1)Integrated Risk Management Framework: Apple has developed an integrated risk management framework that aligns risk mitigation strategies with business planning and operational processes, ensuring a cohesive approach to managing climate-related risks.
(5.2)Innovation and Sustainability Initiatives: Proactively invests in research and development of sustainable technologies and operational efficiencies to mitigate physical and transition risks, positioning Apple as a leader in sustainable transportation and energy solutions."\n

7.3.1 Opportunity disclosure (Both the title and the preceding serial number need to be retained)\n
Objective: This segment delves into the climate-related risks identified within operational and strategic frameworks, focusing on those with the potential to materially affect the company's financial health and strategic direction. Specifies the financial implications of these risks, shedding light on their nature, potential severity, and the economic consequences they pose to the business. Must contains"Nature and Potential Severity of Identified Risks", "Specific Financial Impact Projections", "Long-term Strategic Implications".\n
Example: "Apple, Inc.
(1)Nature and Potential Severity of Identified Risks:
(1.1)Raw Material Scarcity and Cost Volatility: The increasing scarcity of critical raw materials required for battery production (such as lithium and cobalt) due to climate change-related supply disruptions poses a significant risk. This scarcity has the potential to drive up costs and impact profit margins. The financial implications could include increased production costs and a potential increase in product prices, potentially reducing market competitiveness.
(1.2)Regulatory Compliance Costs: With governments worldwide tightening emissions standards and setting ambitious decarbonization targets, Apple faces potential increases in regulatory compliance costs. This includes costs associated with adhering to new regulations, which could materially affect operating expenses and require significant capital investments in technology upgrades or process improvements.
(1.3)Operational Disruptions from Extreme Weather Events: Climate change intensifies the frequency and severity of extreme weather events such as hurricanes, floods, and wildfires, which could directly impact Apple's manufacturing facilities and supply chain logistics. The financial implications include potential operational downtime, loss of production capacity, and increased insurance premiums, all of which could significantly impact revenue and operational costs.
(2)Specific Financial Impact Projections:
(2.1)Increased Costs and Capital Expenditure: The combined effect of raw material scarcity, regulatory compliance costs, and operational disruptions could necessitate increased capital expenditure to secure alternative material sources, invest in sustainable technologies, and enhance facility resilience. These factors could lead to a projected increase in operational costs by 10-15 percent over the next five years, assuming current climate trends continue.
(2.2)Revenue Impact from Market Dynamics: Shifts in consumer preferences towards more sustainable products and services, coupled with potential disruptions in production, could impact Apple's revenue. If market dynamics shift more rapidly towards sustainability than anticipated, and Apple faces production constraints due to material scarcity or operational disruptions, there could be a 5-10 precent revenue impact over the next decade.
(3)Long-term Strategic Implications:
(3.1)Adaptation and Innovation as Strategic Imperatives: To mitigate these financial risks, Apple is focusing on adaptation and innovation as strategic imperatives. This includes investing in research and development for alternative materials, enhancing supply chain resilience, and exploring new business models aligned with a low-carbon economy.
(3.2)Strategic Diversification: Diversifying product lines and services to reduce dependency on scarce raw materials and to adapt to changing regulatory landscapes and market demands is seen as crucial. This strategic diversification could involve expanding Apple's energy storage solutions and renewable energy product offerings, which may require reallocating financial resources and adjusting long-term strategic planning.

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Management_processes",
        "Risk_disclosure",
        "Opportunity_disclosure",
    ],
    session_id=session_id,
)

Carbon_pricing_summary_formatter = partial(
    agent_formatter,
    prompt= Rules_prompt +"""
8.1.1 Carbon pricing systems (Both the title and the preceding serial number need to be retained)\n
Objective: This analysis seeks to ascertain whether  operations or activities are subject to any carbon pricing mechanisms, such as the Emissions Trading System (ETS), Cap & Trade, or Carbon Tax. \n
Example: "Apple, Inc
(1)Exposure to Carbon Pricing Mechanisms:
1.1)Inclusion in Emissions Trading Systems (ETS): Apple's operations, particularly its manufacturing facilities and energy consumption, fall under jurisdictions with active Emissions Trading Systems. These include the European Union's ETS and California's Cap-and-Trade program, where Apple has significant operational footprints.
(1.2)Carbon Tax Compliance: In regions without ETS, such as certain parts of Asia and Europe, Apple's operations are subject to Carbon Tax regulations, which directly tax the carbon dioxide equivalent of GHG emissions produced by Apple's energy consumption and logistic operations.
(2)Detailed Description of Emissions Trading System (ETS) Impact:
(2.1)ETS Mechanism: ETS is a market-based approach used to control pollution by providing economic incentives for achieving reductions in the emissions of pollutants. Companies under an ETS are given a certain cap on the amount of greenhouse gas they can emit. They need to hold enough emission allowances to cover their emissions, which can be bought or received for free at the beginning of a trading period.
(3)Implications for Apple:
(3.1)Compliance Costs: Apple must ensure it possesses sufficient emission allowances to cover its operational emissions. If Apple's emissions exceed the allowances, it must purchase additional allowances in the marketplace, leading to increased operational costs.
(3.2)Operational Efficiency Incentives: The ETS incentivizes Apple to innovate and improve its operational efficiency and energy use to reduce its overall emissions, potentially lowering the need for purchasing additional allowances.
(3.3)Revenue from Carbon Credits: Apple can benefit financially from reducing its emissions below the allowance cap through the sale of excess allowances or earning carbon credits for overachievement in emission reductions.
(4)Strategic Implications and Actions:
(4.1)Strategic Investment in Emission Reduction: Apple is actively investing in clean energy solutions and technologies to reduce its carbon footprint, thereby minimizing its exposure to the costs associated with carbon pricing systems. This includes enhancements in manufacturing efficiency, renewable energy use, and waste reduction.
(4.2)Monitoring and Reporting System: Apple has implemented a comprehensive monitoring and reporting system to track emissions across its operations, ensuring compliance with various carbon pricing mechanisms and optimizing its strategy for managing allowances and credits.
(4.3)Policy Engagement: Apple engages with policymakers and regulatory bodies to influence the development of carbon pricing regulations, advocating for systems that support renewable energy and electric vehicle adoption."\n

8.2.1 Project-based carbon credits (Both the title and the preceding serial number need to be retained)\n
Objective: This analysis aims to ascertain whether annual sustainability report references the cancellation of any project-based carbon credits during the reporting period. \n
Example: "Apple, Inc.
(1)Mention of Carbon Credit Cancellations:
(1.1)The report explicitly mentions that Apple engaged in the cancellation of project-based carbon credits within the reporting year. This strategic move was part of Apple's broader environmental sustainability and carbon management strategy.
(2)Details on Canceled Project-Based Carbon Credits:
(2.1)Project Characteristics and Size: The canceled carbon credits were primarily associated with renewable energy projects, including wind farms and solar power installations, that Apple had invested in. These projects were characterized by their significant contributions to renewable energy capacity, with a combined generation capacity of over 500 MW across multiple regions.
(2.2)Volume of Credits Canceled: Apple canceled a total of 1 million metric tons of CO2 equivalent (tCO2e) in project-based carbon credits during the reporting period. This volume represents a substantial portion of Apple's efforts to offset its carbon footprint and contribute to global decarbonization efforts.
(3)Reasons for Cancellation:
(3.1)Direct Contribution to Sustainability Goals: The primary reason for the cancellation was Apple's strategic decision to directly apply these carbon credits towards its own sustainability goals, rather than trading them in the open market. This approach reflects Apple's commitment to achieving its environmental objectives and enhancing its sustainability profile.
(3.2)Ensuring Environmental Integrity: Another reason for the cancellation was to ensure the environmental integrity of Apple's carbon offsetting efforts. By canceling these credits, Apple aimed to guarantee that the emission reductions are permanently retired and not used by other entities to offset their emissions, thus making a genuine contribution to reducing global carbon emissions.
(4)Strategic Implications and Actions:
(4.1)Reinforcement of Apple's Environmental Commitment: The cancellation of project-based carbon credits underscores Apple's dedication to real and verifiable environmental impact, beyond compliance or market expectations. It signals to stakeholders that Apple prioritizes substantive environmental actions over merely participating in carbon trading for financial gains.
(4.2)Influence on Carbon Market Practices: Apple's approach to canceling carbon credits could influence broader market practices by setting a precedent for other corporations to follow, potentially leading to a shift towards more direct and impactful environmental actions within the corporate sector."\n

8.3.1 Internal price on carbon (Both the title and the preceding serial number need to be retained)\n
Objective: This analysis focuses on determining if sustainability report includes references to the use of an internal carbon price. If confirmed, the analysis aims to elaborate on how this internal carbon price is integrated into the organization, detailing its influence on decision-making processes, strategy development, and various operational activities.\n
Example: "Apple, Inc.
(1)Mention of Internal Carbon Pricing:
(1.1)Apple's report affirmatively mentions the implementation of an internal carbon pricing mechanism. This strategic tool is employed as part of Apple's comprehensive approach to embedding sustainability into its corporate ethos and operational practices.
(2)Implementation Details of Internal Carbon Pricing:
(2.1)Pricing Mechanism: Apple has established a robust internal carbon pricing model, set at a specific dollar amount per metric ton of CO2 equivalent emissions. This price is periodically reviewed and adjusted in line with scientific recommendations and global climate policy developments to ensure its effectiveness in incentivizing carbon reduction.
(2.2)Role in Decision-Making Processes: The internal carbon price plays a pivotal role in Apple's decision-making processes. It is applied as a cost factor in investment analysis, project evaluation, and procurement decisions. By incorporating the cost of carbon into these processes, Apple prioritizes low-carbon solutions and technologies, aligning its operational decisions with its sustainability targets.
(2.3)Influence on Strategy Formulation: The internal carbon price is integral to Apple's strategy formulation. It informs the development of Apple's long-term business strategies, including its transition to renewable energy, investment in energy efficiency, and innovation in sustainable transportation. The internal price acts as a financial tool to evaluate the feasibility and impact of strategic initiatives on Apple's carbon footprint and environmental goals.
(2.4)Operational Activities: Across operational activities, the internal carbon price is used to assess the carbon impact of Apple's manufacturing processes, supply chain operations, and logistic networks. It drives the adoption of cleaner production techniques, sustainable supply chain practices, and energy-efficient logistics solutions. Furthermore, it supports Apple's efforts in product lifecycle management, from design to end-of-life, by encouraging the integration of sustainability principles throughout the product lifecycle.
(3)Strategic Implications and Actions:
(3.1)Enhanced Sustainability Focus: The adoption of an internal carbon price reinforces Apple's commitment to sustainability, ensuring that environmental considerations are quantitatively accounted for in all aspects of its operations.
(3.2)Innovation and Efficiency: By embedding the cost of carbon into operational and strategic decision-making, Apple fosters innovation and efficiency improvements, encouraging the development of new technologies and processes that reduce carbon emissions.
(3.3)Stakeholder Engagement: The internal carbon pricing mechanism also plays a role in Apple's engagement with stakeholders, including investors, customers, and regulatory bodies. It demonstrates Apple's proactive approach to managing its environmental impact and aligning its business practices with global sustainability goals."\n

Additional information (Both the title and the preceding serial number need to be retained)\n""",
    task_ids=[
        "Carbon_pricing_systems",
        "Project_based_carbon_credits",
        "Internal_price_on_carbon",
    ],
    session_id=session_id,
)

# Translation formatters
Emissions_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Emissions_output_merge"],
)

Emissions_breakdown_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Emissions_breakdown_output_merge"],
)

Target_translate_formatter = partial(
    openai_translate_formatter,
    task_ids=["Target_output_merge"],
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


# write agent for each task
agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
# openai = partial(post_request, post_url=openai_url, formatter=openai_formatter)
Scope_emissions_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Scope_emissions_summary_formatter
)
Other_emissions_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Other_emissions_summary_formatter
)
Scope_1_emissions_breakdown_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Scope_1_emissions_breakdown_summary_formatter
)
Scope_2_emissions_breakdown_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Scope_2_emissions_breakdown_summary_formatter
)
Scope_3_emissions_breakdown_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Scope_3_emissions_breakdown_summary_formatter
)
Target_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Target_summary_formatter
)
Initiatives_overview_agent = partial(
    post_request, post_url=agent_url, formatter=Initiatives_overview_formatter
)
Verification_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Verification_summary_formatter
)
Governance_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Governance_summary_formatter
)
Risks_opportunities_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Risks_opportunities_summary_formatter
)
Carbon_pricing_summary_agent = partial(
    post_request, post_url=agent_url, formatter=Carbon_pricing_summary_formatter
)
Emissions_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Emissions_translate_formatter
)
Emissions_breakdown_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Emissions_breakdown_translate_formatter
)
Target_translate_agent = partial(
    post_request, post_url=openai_url, formatter=Target_translate_formatter
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


# Airflow operators
default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="Automobile_esg_compliance_task",
    default_args=default_args,
    description="Automibile esg agents dag",
    schedule_interval=None,
    concurrency=2,
    tags=["Automobile_esg_agent"],
    catchup=False,
) as dag:
    # wait_for_10_seconds = TimeDeltaSensor(
    # task_id='wait_for_10_seconds',
    # delta=timedelta(seconds=10),
    # )

# Total emissions data task
    Total_emissions_data = task_PyOpr(
        task_id="Total_emissions_data",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide the total gross global emissions from the report, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 1 emissions data task
    Scope_1_emissions_data = task_PyOpr(
        task_id="Scope_1_emissions_data",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide the total gross global emissions of Scope 1 from the report, measured in metric tons of CO2 equivalent (CO2e)."
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
                    "input": "Provide the total gross global emissions of Scope 2 from the report, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions data task
    Scope_3_emissions_data = task_PyOpr(
        task_id="Scope_3_emissions_data",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide the total gross global emissions of Scope 3 from the report, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Emissions summary task
    Scope_emissions_summary = task_PyOpr(
        task_id="Scope_emissions_summary",
        callable_func=Scope_emissions_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Biogenic carbon data task
    Biogenic_carbon_data = task_PyOpr(
        task_id="Biogenic_carbon_data",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide the total gross global emissions of biogenic carbon from the report, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Subsidiary task
    Subsidiary = task_PyOpr(
        task_id="Subsidiary",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Extract and summarize disaggregated emission data for each subsidiary, specifying Scope 1 and Scope 2 emissions separately. Use the following unique identifiers for each entity as applicable: ISIN code for bonds, ISIN code for equities, CUSIP number, Ticker symbol, SEDOL code, LEI number, or any other unique identifier provided. Detail the emissions data explicitly for each identifier associated with the subsidiaries mentioned in the report."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Emissions performance task
    Emissions_performance = task_PyOpr(
        task_id="Emissions_performance",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Compare the gross global emissions Scope 1 and Scope 2 for the reporting year with those from the previous year, detailing any changes or trends observed."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Other emissions summary task
    Other_emissions_summary = task_PyOpr(
        task_id="Other_emissions_summary",
        callable_func=Other_emissions_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Scope 1 emissions by GHGs task
    Scope_1_ghgs = task_PyOpr(
        task_id="Scope_1_ghgs",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Break down the total gross global Scope 1 emissions by greenhouse gas type(Including CO2, CH4, N2O, HFCs, PFCs, SF6, NF3, and other greenhouse gas), measured in metric tons of CO2 equivalent (CO2e), and provide the source of each used global warming potential (GWP)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope_1_region task
    Scope_1_region = task_PyOpr(
        task_id="Scope_1_region",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Break down the total gross global Scope 1 emissions by country/area/region, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope_1_business task
    Scope_1_business = task_PyOpr(
        task_id="Scope_1_business",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Break down the total gross global Scope 1 emissions by business division, facility, or activity, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 1 Emissions breakdown task
    Scope_1_emissions_breakdown_summary = task_PyOpr(
        task_id="Scope_1_emissions_breakdown_summary",
        callable_func=Scope_1_emissions_breakdown_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Scope 2 reporting task
    Scope_2_reporting = task_PyOpr(
        task_id="Scope_2_reporting",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Describe the approach to reporting Scope 2 emissions, clarifying whether a location-based or market-based approach is used."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 region task
    Scope_2_region = task_PyOpr(
        task_id="Scope_2_region",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Break down the total gross global Scope 2 emissions by country/area/region, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 business task
    Scope_2_business = task_PyOpr(
        task_id="Scope_2_business",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Break down the total gross global Scope 2 emissions by business division, facility, or activity, measured in metric tons of CO2 equivalent (CO2e)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 Emissions breakdown task
    Scope_2_emissions_breakdown_summary = task_PyOpr(
        task_id="Scope_2_emissions_breakdown_summary",
        callable_func=Scope_2_emissions_breakdown_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Scope 3 calculation methodology task
    Scope_3_calculation_methodology = task_PyOpr(
        task_id="Scope_3_calculation_methodology",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Provide Scope 3 emission calculation methods, include supplier-specific, hybrid, average data, spend-based, average product, average spend-based, fuel-based, distance-based, waste-type-specific, asset-specific, lessor-specific, site-specific methods, methodologies for direct and indirect use phase emissions (both requiring specification), franchise-specific, investment-specific methods, and an option to specify other methods."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions breakdown task
    Scope_3_emissions_breakdown = task_PyOpr(
        task_id="Scope_3_emissions_breakdown",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Calculate the total Scope 3 emissions and report the amount in metric tons of CO2 equivalent (CO2e). Including Purchased Goods and Services, Capital Goods, Fuel-and-Energy-Related Activities (Not Included in Scope 1 or 2), Upstream Transportation and Distribution, Waste Generated in Operations, Business Travel, Employee Commuting, Upstream Leased Assets, Downstream Transportation and Distribution, Processing of Sold Products, Use of Sold Products, End of Life Treatment of Sold Products, Downstream Leased Assets, Franchises, Investments."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 Emissions breakdown task
    Scope_3_emissions_breakdown_summary = task_PyOpr(
        task_id="Scope_3_emissions_breakdown_summary",
        callable_func=Scope_3_emissions_breakdown_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Total emissions reduction target
    Total_emissions_reduction_target = task_PyOpr(
        task_id="Total_emissions_reduction_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Indicate the report's overall target for reducing emissions, expressed in metric tons of CO2 equivalent (CO2e). Clarify the scope of this reduction target, scope options are: Entire company, a specific business division, a certain business activity, a single site or facility, a specified country/area/region, product-specific, or another category (please detail)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 1 emissions reduction target
    Scope_1_emissions_reduction_target = task_PyOpr(
        task_id="Scope_1_emissions_reduction_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Detail the Scope 1 emissions reduction objective in the report, measured in metric tons of CO2 equivalent (CO2e). Delineate the scope for this reduction target, coverage options are: Entire company, Specific business division, Certain business activity, Individual site or facility, Designated country/area/region, Product-specific, or Other (detail if relevant)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions reduction target
    Scope_2_emissions_reduction_target = task_PyOpr(
        task_id="Scope_2_emissions_reduction_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Detail the Scope 2 emissions reduction objective in the report, measured in metric tons of CO2 equivalent (CO2e). Delineate the scope for this reduction target, coverage options are: Entire company, Specific business division, Certain business activity, Individual site or facility, Designated country/area/region, Product-specific, or Other (detail if relevant)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions reduction target
    Scope_3_emissions_reduction_target = task_PyOpr(
        task_id="Scope_3_emissions_reduction_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Detail the Scope 3 emissions reduction target in the report, quantified in metric tons of CO2 equivalent (CO2e). Specify the target's scope, selecting from: Company-wide, Business division, Business activity, Site/facility, Country/area/region, Product-level, or Other (with clarification). Include a comprehensive breakdown of the Scope 3 reduction target across all relevant categories: Purchased goods and services, Capital goods, Fuel-and-energy-related activities (excluding Scope 1 and 2), Upstream transportation and distribution, Waste generated in operations, Business travel, Employee commuting, Upstream leased assets, Downstream transportation and distribution, Processing of sold products, Use of sold products, End-of-life treatment of sold products, Downstream leased assets, Franchises, Investments (not related to financial services), and any additional applicable upstream or downstream categories."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Science-based target
    Science_based_target = task_PyOpr(
        task_id="Science_based_target",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Confirm if the emissions reduction target is recognized as a science-based carbon target by the Science Based Targets initiative (SBTi) in the report."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Target summary task
    Target_summary = task_PyOpr(
        task_id="Target_summary",
        callable_func=Target_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Specific emissions reduction instructions task
    Specific_emissions_reduction_instructions = task_PyOpr(
        task_id="Specific_emissions_reduction_instructions",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Assess if the report indicates any ongoing efforts to reduce emissions. If affirmative, describe these emission reduction actions in detail, covering both planning and execution phases. Also, provide an estimation of the anticipated CO2 emission reductions resulting from these actions, measured in metric tons of CO2 equivalent (CO2e). "
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Initiatives overview task
    Initiatives_overview= task_PyOpr(
        task_id="Initiatives_overview",
        callable_func=Initiatives_overview_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Scope 1 emissions verification
    Scope_1_emissions_reduction = task_PyOpr(
        task_id="Scope_1_emissions_reduction",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Verify if the report includes third-party verification or certification for its Scope 1 emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 2 emissions verification
    Scope_2_emissions_reduction = task_PyOpr(
        task_id="Scope_2_emissions_reduction",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Verify if the report employs a third-party verification or certification process for its Scope 2 emissions, specifying whether it applies to location-based or market-based emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Scope 3 emissions verification
    Scope_3_emissions_reduction = task_PyOpr(
        task_id="Scope_3_emissions_reduction",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Establish whether the report has a third-party verification or certification process in place for its Scope 3 emissions."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Verification summary task
    Verification_summary = task_PyOpr(
        task_id="Verification_summary",
        callable_func=Verification_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Board oversight task
    Board_oversight = task_PyOpr(
        task_id="Board_oversight",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Determine whether there is oversight of climate-related issues at the board level. Specific content includes clarifying the positions and responsibilities of each person on the board regarding climate-related issues; providing details of board oversight of climate-related issues, including climate-related risks and opportunities in operations, climate-related risks and opportunities in banking operations, and investment activities. climate-related risks and opportunities, climate-related risks and opportunities in insurance underwriting activities, climate impacts from operations, climate impacts from banking operations, climate impacts from investment activities, climate impacts from insurance underwriting activities."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Management responsibility task
    Management_responsibility = task_PyOpr(
        task_id="Management_responsibility",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Assess whether the report identifies any management roles, committees, or departments tasked with handling climate change-related issues below the board level. Information needs to include positions or committees, climate-related responsibilities of this position, areas of responsibility (including risks and opportunities related to banking activities, risks and opportunities related to investing activities, risks and opportunities related to insurance underwriting activities, risks and opportunities related to operations), reporting systems (direct to Board of Directors Report, CEO Reporting System, Risk-CRO Reporting System, Finance-CFO Reporting Line, Investment-CIO Reporting Line, Operations-COO Reporting Line, Corporate Sustainability/CSR-CSO Reporting Line, Others)."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Employee incentives task
    Employee_incentives = task_PyOpr(
        task_id="Employee_incentives",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Identify any incentives detailed in the report for addressing climate-related issues, focusing on how achievements towards set goals are rewarded. Categorize the rewards into: Financial Incentives: Include bonuses, promotions, salary increases, share options, etc. Non-Financial Incentives: Cover internal company awards, annual commendations, public recognition, etc."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Governance summary task
    Governance_summary = task_PyOpr(
        task_id="Governance_summary",
        callable_func=Governance_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Management processes task
    Management_processes = task_PyOpr(
        task_id="Management_processes",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Analyze the report and outline the methodology employed for identifying, assessing, and managing climate-related risks and opportunities. This outline should include the strategy for defining time horizons (short, medium, and long-term) and the criteria used to gauge material financial or strategic impacts. Furthermore, elucidate how the organization categorizes climate risks into physical risks and transition risks within its risk assessment framework."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Risk disclosure task
    Risk_disclosure = task_PyOpr(
        task_id="Risk_disclosure",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Detail any identified climate-related risks that could significantly affect the financial stability or strategic direction in the report. This should encompass risks across the value chain, including upstream and downstream supply chain aspects, direct operations, investments, and other relevant areas. Emphasize on outlining how these risks are determined to be substantial in their potential impact."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Opportunity disclosure task
    Opportunity_disclosure = task_PyOpr(
        task_id="Opportunity_disclosure",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Elaborate on any identified climate-related risks that possess the potential to materially affect the finances or strategic direction in the report. Focus on specifying the actual financial impact these risks could have on the business, detailing their nature, potential severity, and the financial implications."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Risks and opportunities summary task
    Risks_opportunities_summary = task_PyOpr(
        task_id="Risks_opportunities_summary",
        callable_func=Risks_opportunities_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Carbon pricing systems task
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

# Project-based carbon credits task
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

# Internal price on carbon task
    Internal_price_on_carbon = task_PyOpr(
        task_id="Internal_price_on_carbon",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Assess whether the report mentions the use of an internal carbon price. If affirmative, describe in detail how this internal carbon price is implemented across the organization, covering its role in decision-making processes, strategy formulation, and other operational activities."
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

# Carbon pricing summary task
    Carbon_pricing_summary = task_PyOpr(
        task_id="Carbon_pricing_summary",
        callable_func=Carbon_pricing_summary_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Emissions output merge task
    Emissions_output_merge = PythonOperator(
        task_id="Emissions_output_merge",
        python_callable=Emissions_merge,
    )

# Emissions translate task
    Emissions_translate = task_PyOpr(
        task_id="Emissions_translate",
        callable_func=Emissions_translate_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Emissions breakdown merge task
    Emissions_breakdown_output_merge = PythonOperator(
        task_id="Emissions_breakdown_output_merge",
        python_callable=Emissions_breakdown_merge,
    )

# Emissions breakdown translate task
    Emissions_breakdown_translate = task_PyOpr(
        task_id="Emissions_breakdown_translate",
        callable_func=Emissions_breakdown_translate_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Target output merge task
    Target_output_merge = PythonOperator(
        task_id="Target_output_merge",
        python_callable=Target_merge,
    )

# Target translate task
    Target_translate = task_PyOpr(
        task_id="Target_translate",
        callable_func=Target_translate_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Initiatives output merge task
    Initiatives_output_merge = PythonOperator(
        task_id="Initiatives_output_merge",
        python_callable=Initiatives_merge,
    )

# Initiatives translate task
    Initiatives_translate = task_PyOpr(
        task_id="Initiatives_translate",
        callable_func=Initiatives_translate_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Verification output merge task
    Verification_output_merge = PythonOperator(
        task_id="Verification_output_merge",
        python_callable=Verification_merge,
    )

# Verification translate task
    Verification_translate = task_PyOpr(
        task_id="Verification_translate",
        callable_func=Verification_translate_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Governance output merge task
    Governance_output_merge = PythonOperator(
        task_id="Governance_output_merge",
        python_callable=Governance_merge,
    )

# Governance translate task
    Governance_translate = task_PyOpr(
        task_id="Governance_translate",
        callable_func=Governance_translate_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Risks and opportunities output merge task
    Risks_opportunities_output_merge = PythonOperator(
        task_id="Risks_opportunities_output_merge",
        python_callable=Risks_opportunities_merge,
    )

# Risks and opportunities translate task
    Risks_opportunities_translate = task_PyOpr(
        task_id="Risks_opportunities_translate",
        callable_func=Risks_opportunities_translate_agent,
        execution_timeout=timedelta(minutes=30),
    )

# Carbon pricing output merge task
    Carbon_pricing_output_merge = PythonOperator(
        task_id="Carbon_pricing_output_merge",
        python_callable=Carbon_pricing_merge,
    )

# Carbon pricing translate task
    Carbon_pricing_translate = task_PyOpr(
        task_id="Carbon_pricing_translate",
        callable_func=Carbon_pricing_translate_agent,
        execution_timeout=timedelta(minutes=30),
    )

    Merge_EN = task_PyOpr(
        task_id="Merge_EN",
        callable_func=merge_EN,
    )

    Merge_CN = task_PyOpr(
        task_id="Merge_CN",
        callable_func=merge_CN,
    )

    (
        [
            Total_emissions_data,
            Scope_1_emissions_data,
            Scope_2_emissions_data,
            Scope_3_emissions_data,
        ] 
            >> Scope_emissions_summary
    )

    (
        [
            Biogenic_carbon_data,
            Subsidiary,
            Emissions_performance,
        ] 
            >> Other_emissions_summary
    )

    (
        [
            Scope_emissions_summary,
            Other_emissions_summary,
        ]
            >> Emissions_output_merge
            >> Emissions_translate
    )

    (
        [
            Scope_1_ghgs,
            Scope_1_region,
            Scope_1_business,
        ] 
            >> Scope_1_emissions_breakdown_summary
    )

    (
        [
            Scope_2_reporting,
            Scope_2_region,
            Scope_2_business,
        ] 
            >> Scope_2_emissions_breakdown_summary
    )

    (
        [
            Scope_3_calculation_methodology,
            Scope_3_emissions_breakdown,
        ] 
            >> Scope_3_emissions_breakdown_summary
    )

    (
        [
            Scope_1_emissions_breakdown_summary,
            Scope_2_emissions_breakdown_summary,
            Scope_3_emissions_breakdown_summary,
        ]
            >> Emissions_breakdown_output_merge
            >> Emissions_breakdown_translate
    )

    (
        [
            Total_emissions_reduction_target,
            Scope_1_emissions_reduction_target,
            Scope_2_emissions_reduction_target,
            Scope_3_emissions_reduction_target,
            Science_based_target,
        ]
            >> Target_summary
    )

    (
        [
            Target_summary,
        ]
            >> Target_output_merge
            >> Target_translate
    )
    
    (
        [
            Specific_emissions_reduction_instructions,
        ]
            >> Initiatives_overview
    )

    (
        [
            Initiatives_overview,
        ]
            >> Initiatives_output_merge
            >> Initiatives_translate
    )

    (
        [
            Scope_1_emissions_reduction,
            Scope_2_emissions_reduction,
            Scope_3_emissions_reduction,
        ]
            >> Verification_summary
    )

    (
        [
            Verification_summary,
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
    )

    (
        [
            Governance_summary,
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
    )

    (
        [
            Risks_opportunities_summary,
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
    )

    (
        [
            Carbon_pricing_summary,
        ]
            >> Carbon_pricing_output_merge
            >> Carbon_pricing_translate
    )


    [
        Emissions_output_merge,
        Emissions_breakdown_output_merge,
        Target_output_merge,
        Initiatives_output_merge,
        Verification_output_merge,
        Governance_output_merge,
        Risks_opportunities_output_merge,
        Carbon_pricing_output_merge,
    ]   >> Merge_EN


    [
        Emissions_translate,
        Emissions_breakdown_translate,
        Target_translate,
        Initiatives_translate,
        Verification_translate,
        Governance_translate,
        Risks_opportunities_translate,
        Carbon_pricing_translate,
    ]   >> Merge_CN

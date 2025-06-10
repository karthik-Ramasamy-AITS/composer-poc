from com.amway.integration.custom.v2.AmGlCommon import logError
import traceback, pymsteams
from airflow.models.variable import Variable

alert_config = Variable.get("amgl_alert_config", deserialize_json=True)
teams_webhook_url = alert_config['teams_webhook_url']

def failure_callback(context):
    log_url = context.get("task_instance").log_url
    exception = context.get('exception')
    formatted_exception = ''.join(
                traceback.format_exception(type(exception),
                                           value=exception,
                                           tb=exception.__traceback__)
    ).strip()
    #logError(f'failure_callback : {msg}')
    myTeamsMessage = pymsteams.connectorcard(teams_webhook_url)
    myTeamsMessage.color("#d90404")
    # create the section
    myMessageSection = pymsteams.cardsection()
    myMessageSection.activityTitle("Airflow/Composer DAG Alerts")

    # Add Facts
    myMessageSection.addFact("DAG", context.get('task_instance').dag_id)
    myMessageSection.addFact("Task", context.get('task_instance').task_id)
    myMessageSection.addFact("Execution Time", str(context.get('logical_date')))
    myMessageSection.addFact("Exception", formatted_exception)

    # Section Text
    myMessageSection.text(f"DAG scheduled execution failed")
    myTeamsMessage.addLinkButton("Click Here for More Details", log_url)
    myTeamsMessage.addSection(myMessageSection)
    myTeamsMessage.summary("DAG scheduled execution failed")
    # send the message
    myTeamsMessage.send()
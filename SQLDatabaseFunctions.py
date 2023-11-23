# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(SQLDatabaseFunctions) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import azure.functions as func
import logging
import json 
import os 
import datetime
from azure.functions.decorators.core import DataType


bp = func.Blueprint()

############################################################################################################################
#########################################   SELECT ALL   ###################################################################
############################################################################################################################


@bp.function_name(name="DatabaseSelectAllFunction")
@bp.route(route="DatabaseSelect", auth_level=func.AuthLevel.ANONYMOUS)
@bp.generic_input_binding(
                        arg_name="selectSQLAll", 
                        type="sql",
                        CommandText="SELECT * FROM dbo.OnlineCourses",
                        CommandType="Text",
                        ConnectionStringSetting="SQLConnectionString",
                        data_type=DataType.STRING)
def DatabaseSelectAllFunction(req: func.HttpRequest, selectSQLAll: func.SqlRowList) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    rows = list(map(lambda r: json.loads(r.to_json()), selectSQLAll))

    return func.HttpResponse(
        json.dumps(rows),
        status_code=200,
        mimetype="application/json"
    )
    
############################################################################################################################
#########################################   SELECT INSTRUCTOR   ############################################################
############################################################################################################################

@bp.function_name(name="DatabaseSelectInstructorFunction")
@bp.route(route="DatabaseSelect/{instructor}", auth_level=func.AuthLevel.ANONYMOUS)
@bp.generic_input_binding(
                        arg_name="selectSQLInstructor", 
                        type="sql",
                        CommandText="SELECT * FROM dbo.OnlineCourses WHERE Instructor = @Instructor",
                        CommandType="Text",
                        ConnectionStringSetting="SQLConnectionString",
                        Parameters="@Instructor={instructor}",
                        data_type=DataType.STRING)
def DatabaseSelectInstructorFunction(req: func.HttpRequest, selectSQLInstructor: func.SqlRowList) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    rows = list(map(lambda r: json.loads(r.to_json()), selectSQLInstructor))

    #rows = [json.loads(r.to_json()) for r in selectSQLInstructor]

    return func.HttpResponse(
        json.dumps(rows),
        status_code=200,
        mimetype="application/json"
    )

############################################################################################################################
#########################################   INSERT ROWS   ##################################################################
############################################################################################################################

#  The upsert command requires primary keys in the SQL Table to identify unique records in the table. 
# An upsert statement in a SQL Server database is a combination of the words "update" and "insert." It is a single SQL 
# statement that performs an update if a record already exists, and inserts a new record if the record does not exist. 


@bp.function_name(name="DatabaseInsertFunction")
@bp.route(route="DatabaseInsert", auth_level=func.AuthLevel.ANONYMOUS)
@bp.generic_output_binding(
                        arg_name="insertSQL", 
                        type="sql",
                        CommandText="[dbo].[StudentReviews]",
                        ConnectionStringSetting="SQLConnectionString",
                        data_type=DataType.STRING)
def DatabaseInsertFunction(req: func.HttpRequest, insertSQL: func.Out[func.SqlRow]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # The following example shows a SQL output binding in a function.json file and a 
    # Python function that adds records to a table, using data provided in an HTTP POST request as a JSON body.

    body = json.loads(req.get_body()) # load the body of an HTTP request as a JSON object. 
    review_time = datetime.datetime.now().isoformat() #  retrieves the current date and time and converts it into a string format using the ISO 8601 standard. 

    #  The  SqlRow  object is initialized with two key-value pairs: "ReviewText" which is obtained 
    # from the  body  dictionary's "ReviewText" key, and "ReviewTime" which is assigned the value of  review_time . 
    row = func.SqlRow.from_dict({
        "ReviewText": body.get("ReviewText"),
        "ReviewTime": review_time
    })
    insertSQL.set(row)

    logging.warning("Row Inserted")



############################################################################################################################
#########################################   DELETE ROWS WITH TIMER   #######################################################
############################################################################################################################

@bp.function_name(name="TimerDeleteSQLFunction")
@bp.generic_input_binding(
                        arg_name="deleteSQL", 
                        type="sql",
                        CommandText="DELETE FROM dbo.StudentReviews WHERE ReviewText = 'BAD_REQUEST'",
                        CommandType="Text",
                        ConnectionStringSetting="SQLConnectionString",
                        data_type=DataType.STRING)
@bp.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def TimerDeleteSQL(myTimer: func.TimerRequest, deleteSQL: func.SqlRowList) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()    

    logging.warning('Python timer trigger function ran at %s', utc_timestamp)
    logging.warning('Scheduled task completed')
 
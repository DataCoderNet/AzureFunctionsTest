# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(SchoolTransactions) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
from azure.functions.decorators.core import DataType
import json 

bp3 = func.Blueprint()

###################################################################################################################
##################################        FROM HTTP TO SERVICE BUS    #############################################
###################################################################################################################


@bp3.function_name(name="Get_Transaction_Status_Queue")
@bp3.route(route="Get_Transaction_Status_Queue", auth_level=func.AuthLevel.ANONYMOUS)
@bp3.service_bus_queue_output(arg_name="serviceBusOut",
                              connection="personalservicebus_SERVICEBUS",
                              queue_name="school_transactions2")
def Get_Transaction_Status_Queue(req: func.HttpRequest, serviceBusOut: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Getting the parameter values from http request
        user = req.params.get('user')
        course = req.params.get('course')
        status = req.params.get('status')
        email = req.params.get('email')

        if user and course and status and email:
            addMsgToQueue = """{
                "user": "%s",
                "course": "%s",
                "status": "%s",
                "email": "%s"
            }""" % (user, course, status, email)

            logging.warning(addMsgToQueue)
            serviceBusOut.set(addMsgToQueue)
            return func.HttpResponse("Message Received")
        else:
            logging.warning("Request failed as one or more parameters are missing in the http call")
            return func.HttpResponse("Request failed as one or more parameters are missing in the http call")
    except:
        logging.warning("Something went wrong")
        return func.HttpResponse("Something went wrong")
    

###################################################################################################################
#####################################   FROM SERVICE BUS TO SQL TABLE     #########################################
###################################################################################################################


@bp3.function_name(name="Process_School_Transaction_Messages")
@bp3.service_bus_queue_trigger( arg_name="azservicebus", 
                                queue_name="school_transactions2",
                                connection="personalservicebus_SERVICEBUS")
@bp3.generic_output_binding(
                        arg_name="insertSQLSuccess", 
                        type="sql",
                        CommandText="[dbo].[StudentsData]",
                        ConnectionStringSetting="SQLConnectionString",
                        data_type=DataType.STRING)
@bp3.generic_output_binding(
                        arg_name="insertSQLFailure", 
                        type="sql",
                        CommandText="[dbo].[FailedToConvert]",
                        ConnectionStringSetting="SQLConnectionString",
                        data_type=DataType.STRING)                                                              
def Process_School_Transaction_Messages(azservicebus: func.ServiceBusMessage, insertSQLSuccess: func.Out[func.SqlRow], insertSQLFailure: func.Out[func.SqlRow]):
    logging.info('Python ServiceBus Queue trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))

    message_json_string = azservicebus.get_body().decode('utf-8')
    json_data = json.loads(message_json_string) 

    #Getting values out of JSON object
    user = json_data["user"]
    course = json_data["course"]
    status = json_data["status"]
    email = json_data["email"]     

    #Save data to SQL Database
    row = func.SqlRow.from_dict({
        "user_name": user,
        "course_id": course,
        "transaction_status": status,
        "user_email" : email
    })

    if status == "SUCCESS" or status == "success":
        insertSQLSuccess.set(row)
    else:
        insertSQLFailure.set(row)

    logging.info("SQL Row Inserted")
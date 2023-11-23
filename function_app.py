import azure.functions as func
import logging
from SQLDatabaseFunctions import bp 
from FunctionChain import bp2
from SchoolTransactions import bp3

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

#Registered Blueprints
app.register_functions(bp) 
app.register_functions(bp2)
app.register_functions(bp3)

@app.function_name(name="http_trigger_function")
@app.route(route="HttpTrigger1")
def http_trigger_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )










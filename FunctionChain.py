# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(FunctionChain) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
from shared_code.common_functions import delete_blob

bp2 = func.Blueprint()

@bp2.function_name(name="Function1_Http_to_Blob")
@bp2.route(route="Function1_Http_to_Blob", auth_level=func.AuthLevel.FUNCTION)
@bp2.blob_output(arg_name="outputBlob",
                path="container1a/{rand-guid}",
                connection="personalstoragehector_STORAGE")
def Function1_Http_to_Blob(req: func.HttpRequest, outputBlob: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    message = req.params.get('message')

    sendMessageOut = 'Processed by Func1 \n' + message

    outputBlob.set(sendMessageOut)

    logging.warning('Processed by Function 1')

    return func.HttpResponse('Thank you for your message')

###################################################################################################
###################################################################################################
###################################################################################################


@bp2.function_name(name="Function2_Blob_to_Blob")
@bp2.blob_trigger(arg_name="inputBlob", 
                  path="container1a/{name}",
                  connection="personalstoragehector_STORAGE")
@bp2.blob_output(arg_name="outputBlob",
                path="container2a/{rand-guid}",
                connection="personalstoragehector_STORAGE")                             
def Function2_Blob_to_Blob(inputBlob: func.InputStream, outputBlob: func.Out[str] ):
    logging.warning(f"Python blob trigger function processed blob"
                f"Name: {inputBlob.name}"
                f"Blob Size: {inputBlob.length} bytes")

    clear_text = inputBlob.read().decode('utf-8')

    sendMessageOut = "Processed by Func2 \n" + clear_text

    outputBlob.set(sendMessageOut)

    logging.warning('Processed by Function 2')

    #Fetching the name of the blob
    tmp = (inputBlob.name).split("/")
    containerName = tmp[0]
    blobName = tmp[1]

    logging.warning(f'Calling Method do delete blob {blobName}')

    delete_blob(containerName, blobName)


###################################################################################################
###################################################################################################
###################################################################################################


@bp2.function_name(name="Function3_Blob_to_QueueStorage")
@bp2.blob_trigger(arg_name="myBlob", path="container2a/{name}",
                               connection="personalstoragehector_STORAGE")
@bp2.queue_output(arg_name="outputQueueItem", 
                  queue_name="myappqueuestorage2", 
                  connection="personalstoragehector_STORAGE")                                
def Function3_Blob_to_QueueStorage(myBlob: func.InputStream, outputQueueItem: func.Out[str]):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myBlob.name}"
                f"Blob Size: {myBlob.length} bytes")

    clear_text = myBlob.read().decode('utf-8')

    sendMessageOut = "Processed by Func3 \n"+clear_text

    outputQueueItem.set(sendMessageOut)

    logging.warning('Processed by Function 3')


    #Fetching the name of the blob
    tmp = (myBlob.name).split("/")
    containerName = tmp[0]
    blobName = tmp[1]

    logging.warning(f'Calling Method do delete blob {blobName}')

    delete_blob(containerName, blobName)


###################################################################################################
###################################################################################################
###################################################################################################



@bp2.function_name(name="Function4_QueueStorage_to_ServiceBus")    
@bp2.queue_trigger(arg_name="myappqueuestorage", queue_name="myappqueuestorage2",
                               connection="personalstoragehector_STORAGE") 
@bp2.service_bus_queue_output(arg_name="serviceBus",
                              connection="personalservicebus_SERVICEBUS",
                              queue_name="myappqueue2")
def Function4_QueueStorage_to_ServiceBus(myappqueuestorage: func.QueueMessage, serviceBus: func.Out[str]):
    logging.info('Python Queue trigger processed a message: %s',
                myappqueuestorage.get_body().decode('utf-8'))

    clear_text = myappqueuestorage.get_body().decode('utf-8')

    sendMessageOut = "Processed by Function 4 \n" + clear_text

    serviceBus.set(sendMessageOut)

    logging.warning("Processed by Function 4")
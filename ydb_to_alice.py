import os
import ydb
import base64
import json

def simple_select(pool, data):
    def callee(session):
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            data,
            commit_tx=True,
        )
        return result_sets[0]
    return pool.retry_operation_sync(callee)


def handler(event, context):
    """
    Entry-point for Serverless Function.
    :param event: request payload.
    :param context: information about current execution context.
    :return: response to be serialized as JSON.
    """
    

    if 'request' in event and  'original_utterance' in event['request'] and len(event['request']['original_utterance']) > 0:
        req = event['request']['original_utterance'] 
        if ("темп" in req) or ("влаж" in req) or ("Темп" in req) or ("Влаж" in req):
        
            endpoint = os.getenv('YDB_ENDPOINT')
            database = os.getenv('YDB_DATABASE')

            with ydb.Driver(endpoint=endpoint, database=database) as driver:
                driver.wait(timeout=5, fail_fast=True)

                with ydb.SessionPool(driver) as pool:
                    full_path = os.path.join(database, "")

                    data = """
                        SELECT temperature, humidity
                        FROM DHT22
                        LIMIT 1;
                    """

                    meas = simple_select(pool, data).rows[0]
                    res = """Температура в квартире {} градусов, а влажность {} процентов""".format(
                        meas.temperature,
                        meas.humidity
                    )
        else:
            res = "Не поняла вопроса. Ну-ка повтори"

                
    else:
        res = "Че надо?"

    return {
        'version': event['version'],
        'session': event['session'],
        'response': {
            # Respond with the original request or welcome the user if this is the beginning of the dialog and the request has not yet been made.
            'text': res,
            # Don't finish the session after this response.
            'end_session': 'true'
        },
    }

import os
import ydb
import json
import base64

def upsert_simple(pool, path, cmd):
    def callee(session):
        session.transaction().execute(
            cmd,
            commit_tx=True,
        )
    return pool.retry_operation_sync(callee)

def create_tables(pool, path):
    def callee(session):
        # Creating Series table
        session.execute_scheme(
            """
                PRAGMA TablePathPrefix("{}");
                CREATE table `DHT22` (
                    `device_id` Uint32,
                    `meas_id` Uint32,
                    `meas_time` String,
                    `humidity` Double,
                    `temperature` Double,
                PRIMARY KEY (`device_id`)
                )
                """.format(
                path
            )
        )
    return pool.retry_operation_sync(callee)

def handler(event, context):
    
    msg_payload = json.dumps(event["messages"][0])
    json_msg = json.loads(msg_payload)
    event_payload = base64.b64decode(json_msg["details"]["payload"])
    e = json.loads(event_payload)

    timestamp       = json.dumps(e['TimeStamp'])
    humidity        = e['Values'][0]['Value']
    temperature     = e['Values'][1]['Value']
    meas_id         = e['MeasId']
    device_id       = e['DeviceId']

    endpoint = os.getenv('YDB_ENDPOINT')
    database = os.getenv('YDB_DATABASE')

    with ydb.Driver(endpoint=endpoint, database=database) as driver:
        driver.wait(timeout=5, fail_fast=True)

        with ydb.SessionPool(driver) as pool:
            full_path = os.path.join(database, "")
            create_tables(pool, full_path)

            data = """
                        PRAGMA TablePathPrefix("{}");
                        UPSERT INTO DHT22 (device_id, meas_id, humidity, meas_time, temperature) 
                        VALUES ({}, {}, {}, {}, {});
                        """.format(
                                full_path, 
                                int(device_id),
                                int(meas_id), 
                                float(humidity), 
                                timestamp, 
                                float(temperature)
                        )
            upsert_simple(pool, full_path, data)


    return {
        "STATUS" : 200
    }

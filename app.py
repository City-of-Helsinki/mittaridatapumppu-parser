import datetime
import importlib
import logging
import os
import pathlib
from pprint import pformat
from zoneinfo import ZoneInfo
import asyncio
import httpx
from kafka.errors import UnknownTopicOrPartitionError

from fvhiot.utils import init_script
from fvhiot.utils.data import data_unpack, data_pack
from fvhiot.utils.aiokafka import (
    get_aiokafka_producer_by_envs,
    get_aiokafka_consumer_by_envs,
    on_send_success,
    on_send_error,
)

# TODO: for testing, add better defaults (or remove completely to make sure it is set in env)
ENDPOINT_CONFIG_URL = os.getenv("ENDPOINT_CONFIG_URL", "http://127.0.0.1:8000/api/v1/hosts/localhost/")
DEVICE_REGISTRY_URL = os.getenv("DEVICE_REGISTRY_URL", "http://127.0.0.1:8000/api/v1/")
DEVICE_REGISTRY_TOKEN = os.getenv("DEVICE_REGISTRY_TOKEN", "abcdef1234567890abcdef1234567890abcdef12")

device_registry_request_headers = {
    "Authorization": f"Token {DEVICE_REGISTRY_TOKEN}",
    "User-Agent": "mittaridatapumppu-parser/0.1.0",
    "Accept": "application/json",
}


def backup_messages(raw_data_topic: str, msg):
    """
    Store received raw messages somewhere, currently into a local file.

    :param raw_data_topic: Raw data topic's name
    :param msg: Kafka message
    """
    backupdir = pathlib.Path("messages") / pathlib.Path(raw_data_topic)
    backupdir.mkdir(parents=True, exist_ok=True)
    backupfile = backupdir / pathlib.Path("{}.msgpack".format(datetime.datetime.utcnow().strftime("%Y-%m-%d")))
    try:
        with open(backupfile, "ab") as outfile:
            outfile.write(msg.value)  # msg.value is msgpack.packb()'ed data
    except Exception:
        logging.exception("Failed to backup message.")
        logging.error(msg)


async def get_device_data_devreg(device_id: str) -> dict:
    """
    Get device metadata from device registry

    :param device_id:
    :return: Device data in a dict
    """
    metadata = {}
    if DEVICE_REGISTRY_URL is None or DEVICE_REGISTRY_TOKEN is None:
        logging.error(
            "DEVICE_REGISTRY_URL and DEVICE_REGISTRY_TOKEN must be defined, " "querying device metadata failed"
        )
        return metadata
    if device_id is None:
        logging.info("No device_id available, querying device metadata failed")
        return metadata
    # NOTE: creating redis client is very cheap operation, but perhaps it
    # should be benchmarked? Another solution would be to create client once
    # (like kafka consumer) and re-using it in subsequent calls
    url = f"{DEVICE_REGISTRY_URL}/devices/{device_id}/"
    logging.info(f"Querying metadata from {url}")
    # Get metadata from device registry using httpx
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=device_registry_request_headers)
            if response.status_code == 200:
                metadata = response.json()
                logging.debug(metadata)
            else:
                logging.warning(f"Device registry returned {response.status_code} {response.text}")
        except httpx.HTTPError as err:
            logging.exception(f"{err}")

    return metadata


async def get_kafka_topics_from_device_registry_endpoints(fail_on_error: bool) -> dict:
    """
    Update kafka topics information for endpoints from device registry.
    This is done on startup and when device registry is updated.
    """
    # Create request to ENDPOINTS_URL and get data using httpx
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(ENDPOINT_CONFIG_URL, headers=device_registry_request_headers)
            if response.status_code == 200:
                data = response.json()
                logging.info(f"Got {len(data['endpoints'])} endpoints from device registry {ENDPOINT_CONFIG_URL}")
                endpoint_topic_mappings = {}
                for endpoint in data["endpoints"]:
                    try:
                        endpoint_path = endpoint["endpoint_path"]
                        raw_topic = endpoint["kafka_raw_data_topic"]
                        parsed_topic = endpoint["kafka_parsed_data_topic"]
                        group_id = endpoint["kafka_group_id"]

                        endpoint_topic_mappings[endpoint_path] = {
                            "raw_topic": raw_topic,
                            "parsed_topic": parsed_topic,
                            "group_id": group_id,
                            "endpoint_path": endpoint_path,
                        }

                    except KeyError as e:
                        logging.error(f"ATTENTION: endpoint data missing kafka topic information. {e} in {endpoint}")

                print(f"REMOVE ME {endpoint_topic_mappings}")
                if len(endpoint_topic_mappings) >= 1:
                    return endpoint_topic_mappings

        except Exception as e:
            logging.error(f"Failed to get endpoints from device registry {ENDPOINT_CONFIG_URL}: {e}")
            if fail_on_error:
                raise e

    return None


async def get_device_data(device_id: str) -> dict:
    """
    Get device metadata from device registry

    :param device_id:
    :return: Device data in a dict
    """
    metadata = {}
    if os.getenv("DEVICE_REGISTRY_URL"):
        return await get_device_data_devreg(device_id)
    else:
        logging.error("DEVICE_REGISTRY_URL must be defined, querying device metadata failed")
        return metadata


# TODO: generic
def create_parsed_data_message(timestamp: datetime.datetime, payload: list, device: dict) -> dict:
    """
    Mega function to create parsed data messages.
    Data structure loosely follows JTS (Json time series) format.
    This doesn't validate result in any way, yet.
    TODO: add (pydantic) validation
    TODO: split into smaller functions
    TODO: move finally to fvhiot module
    """
    parsed_data = {"version": "1.0"}
    time_str = timestamp.isoformat()
    parsed_data["meta"] = {
        "timestamp_received": "{}".format(time_str),
        "timestamp_parsed": "{}".format(datetime.datetime.now(tz=ZoneInfo("UTC")).isoformat()),
    }
    parsed_data["device"] = device
    # header varibles
    columns = {}
    col_cnt = 0
    start_time = end_time = None
    parsed_data["data"] = []  # list for data
    if isinstance(payload, list):
        # create `keys` set for all unique keys
        keys = set()
        for item in payload:
            for d in item["data"].keys():
                keys.add(d)
        keys = sorted(list(keys))  # now we have all unique keys in sorted list
        col_map = {}  # create mapping for silly "0", "1", "2" named columns and real data keys
        for k in keys:
            col_name = str(col_cnt)  # "0", "1", "2" and so on
            columns[col_name] = {"name": k}  # e.g. columns["0] = {"name" : "temp"}
            col_map[k] = col_name  # e.g. col_map["temp"] = "0"
            col_cnt += 1
        for item in payload:  # create list of data items
            data_item = {
                "time": item["time"],
                "f": {},
            }  # take "time" as is, we trust that it is a valid ISO date str
            for k, v in sorted(item["data"].items()):  # put keys into "f" in sorted order (same as in header)
                col_name = col_map[k]
                data_item["f"][col_name] = {"v": v}
            parsed_data["data"].append(data_item)
            # Find start_time and end_time from data_items
            times = sorted([x["time"] for x in parsed_data["data"]])
            start_time = times[0]
            end_time = times[-1]
    else:
        raise ValueError(f"Unknown type of payload: {type(payload)}")
    parsed_data["header"] = {
        "columns": columns,
        "start_time": start_time,
        "end_time": end_time,
    }
    return parsed_data


async def process_kafka_raw_topic(raw_data: bytes):
    """
    Process raw data received from Kafka.
    :param raw_data: Raw data received from Kafka
    :return: unpacked_data, device_data, parser_module_name
    """

    try:
        unpacked_data = data_unpack(raw_data)
        logging.info(pformat(unpacked_data))

        device_id = unpacked_data.get("device_id")
        if device_id is None:
            logging.warning("Device id not found in raw data - unpacked_data['device_id'] ")
            # TODO: store data for future re-processing
            return unpacked_data, None, None
        device_data = await get_device_data(device_id)
        #        if device_data is None or "device_metadata" not in device_data:
        if device_data is None:
            logging.warning(f"Device data not found for device_id: {device_id}")
            # TODO: store data for future re-processing
            return unpacked_data, None, None

        parser_module_name = device_data.get("parser_module", "")
        if parser_module_name == "":
            logging.warning("Parser module name not found")
            # TODO: store data for future re-processing
            return unpacked_data, device_data, None

        #print(device_data)
        logging.info(f"printing parser module {parser_module_name}")
        return unpacked_data, device_data, parser_module_name

    except Exception as err:
        logging.exception(f"Failed to process raw data: {err}")
        return


async def initialize_kafka_consumer(raw_data_topic):
    """
    Initialize Kafka consumer for raw data topic
    :param raw_data_topic: Raw data topic's name
    :return: AIOKafkaConsumer
    """
    try:
        # TODO make group id a an argument to be passed to get_kafka_consumer_by_envs ?
        return await get_aiokafka_consumer_by_envs([raw_data_topic])
    except UnknownTopicOrPartitionError as err:
        logging.error(f"Kafka Error. topic: {raw_data_topic} does not exist. error: {err}")
        raise err

    except Exception as err:
        logging.exception(f"Kafka Error. topic: {raw_data_topic} error: {err}")
        raise err


async def produce_parsed_data_message(parsed_data_topic, producer, packed_data):
    """
    Produce parsed data message to Kafka
    :param parsed_data_topic: Parsed data topic's name
    :param producer: AIOKafka producer
    :param packed_data: Packed data to be sent to Kafka
    """
    try:
        future = await producer.send(parsed_data_topic, packed_data)

        # Attach callbacks to the future
        # add_done_callback takes a function as an argument. a lambda is an anonymous function
        future.add_done_callback(lambda fut: asyncio.ensure_future(on_send_success(fut.result())))
        future.add_done_callback(lambda fut: fut.add_errback(on_send_error))
    except Exception as e:
        logging.error(f"Failed to send parsed data to Kafka topic {parsed_data_topic}: {e}")
        raise e


async def parse_data(unpacked_data, device_data, parser_module_name):
    """
    Parse data using parser module
    :param unpacked_data: Unpacked data
    :param device_data: Device data
    :param parser_module_name: Parser module's name
    :return: Packed data
    """
    logging.info("Preparing to parse payload")
    try:
        parser_module = importlib.import_module(parser_module_name)
        packet_timestamp, datalines = parser_module.create_datalines_from_raw_unpacked_data(unpacked_data)
        parsed_data = create_parsed_data_message(packet_timestamp, datalines, device_data)
        logging.debug(pformat(parsed_data))
        packed_data = data_pack(parsed_data)
        return packed_data

    except ModuleNotFoundError as err:
        logging.critical(f"Importing parser module  {parser_module_name} failed: {err}")
        return None
        # TODO: store data for future re-processing
    except Exception as err:
        logging.exception(f"parsing failed at {parser_module_name} : {err}")
        # TODO: send data to spare topic for future reprocessing?
        return None


# TODO group id variable
async def consume_and_parse_data_stream(raw_data_topic, parsed_data_topic, producer):
    """
    Consume raw data from Kafka and parse it
    :param raw_data_topic: Raw data topic's name
    :param parsed_data_topic: Parsed data topic's name
    :param producer: AIOKafka producer
    """
    try:
        logging.info(f"Get Kafka consumer for  {raw_data_topic}")
        consumer = await initialize_kafka_consumer(raw_data_topic)

        logging.info(f"Parser is waiting for raw data messages from Kafka topic {raw_data_topic}")
        # Loop forever for incoming messages
        async for msg in consumer:
            logging.info("Preparing to parse payload")

            [unpacked_data, device_data, parser_module_name] = await process_kafka_raw_topic(msg.value)
            logging.debug(f"printing unpacked data {unpacked_data}")
            packed_data = None
            if parser_module_name:
                packed_data = await parse_data(unpacked_data, device_data, parser_module_name)
            if packed_data:
                await produce_parsed_data_message(parsed_data_topic, producer, packed_data)

    finally:
        logging.info(f"Stopped Listening to {raw_data_topic}")
        logging.info("Closing Kafka consumer and producer.")
        if consumer:
            await consumer.stop()
        if producer:
            await producer.stop()


async def main():
    init_script()

    logging.info("Get producer for pushing parsed data messages to Kafka.")
    producer = await get_aiokafka_producer_by_envs()
    if producer is None:
        logging.critical("Kafka connection failed, exiting.")
        exit(1)

    tasks = []
    endpoint_topic_mappings = await get_kafka_topics_from_device_registry_endpoints(True)
    logging.debug(endpoint_topic_mappings)
    endpoint_topic_mappings.pop("/api/v1/data")
    endpoints = endpoint_topic_mappings.keys()
    for endpoint in endpoints:
        logging.info(f"Setting up parser for path: {endpoint}")

        e2t_map = endpoint_topic_mappings[endpoint]
        raw_data_topic = e2t_map["raw_topic"]
        parsed_data_topic = e2t_map["parsed_topic"]
        tasks.append(consume_and_parse_data_stream(raw_data_topic, parsed_data_topic, producer))

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("Bye!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bye!")

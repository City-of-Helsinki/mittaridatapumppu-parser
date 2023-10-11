import datetime
import importlib
import logging
import os
import pathlib
from pprint import pformat
from zoneinfo import ZoneInfo

import httpx
from kafka.producer.future import RecordMetadata


from fvhiot.utils import init_script
from fvhiot.utils.data import data_unpack, data_pack
from fvhiot.utils.kafka import get_kafka_producer_by_envs, get_kafka_consumer_by_envs


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


# TODO: to kafka module
def on_send_success(record_metadata: RecordMetadata):
    logging.info(
        "Successfully sent to topic {}, partition {}, offset {}".format(
            record_metadata.topic, record_metadata.partition, record_metadata.offset
        )
    )


# TODO: to kafka module
def on_send_error(excp):
    logging.error("Error on Kafka producer", exc_info=excp)


def get_device_data_devreg(device_id: str) -> dict:
    """
    Get device metadata from device registry

    :param device_id:
    :return: Device data in a dict
    """
    metadata = {}
    devreg_url = os.getenv("DEVICE_REGISTRY_URL")
    devreg_token = os.getenv("DEVICE_REGISTRY_TOKEN")
    if devreg_url is None or devreg_token is None:
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
    url = f"{devreg_url}/devices/{device_id}/"
    logging.info(f"Querying metadata from {url}")
    # Get metadata from device registry using httpx
    headers = {
        "Authorization": f"Token {devreg_token}",
        "User-Agent": "mittaridatapumppu-parser/0.0.1",
    }

    try:
        response = httpx.get(url, headers=headers)
        if response.status_code == 200:
            metadata = response.json()
            logging.debug(metadata)
        else:
            logging.warning(f"Device registry returned {response.status_code} {response.text}")
    except httpx.HTTPError as err:
        logging.exception(f"{err}")

    return metadata


def get_device_data(device_id: str) -> dict:
    """
    Get device metadata from device registry

    :param device_id:
    :return: Device data in a dict
    """
    metadata = {}
    if os.getenv("DEVICE_REGISTRY_URL"):
        return get_device_data_devreg(device_id)
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


def process_kafka_raw_topic(raw_data: bytes):
    # TODO doc
    unpacked_data = data_unpack(raw_data)
    logging.info(pformat(unpacked_data))
    device_id = unpacked_data["device_id"]
    if device_id is None:
        logging.warning("Device id not found in raw data - unpacked_data['device_id'] ")
        # TODO: store data for future re-processing
        return unpacked_data, None, None
    device_data = get_device_data(device_id)
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

    print(device_data)
    print(f"printing parser module {parser_module_name}")
    return unpacked_data, device_data, parser_module_name


def main():
    init_script()
    raw_data_topic = "cesva.rawdata"  # os.getenv("KAFKA_RAW_DATA_TOPIC_NAME")
    parsed_data_topic = os.getenv("KAFKA_PARSED_DATA_TOPIC_NAME")
    logging.info(f"Get Kafka consumer for {raw_data_topic} and producer for {parsed_data_topic}")
    # Create Kafka consumer for incoming raw data messages
    consumer = get_kafka_consumer_by_envs(raw_data_topic)  # listen to multiple topics -> ?
    producer = get_kafka_producer_by_envs()
    if consumer is None or producer is None:
        logging.critical("Kafka connection failed, exiting.")
        exit(1)

    # Loop forever for incoming messages
    logging.info("Parser is waiting for raw data messages from Kafka.")
    for msg in consumer:
        logging.info("Preparing to parse payload")

        [unpacked_data, device_data, parser_module_name] = process_kafka_raw_topic(msg.value)
        print(f"printing unpacked data {unpacked_data}")
        if parser_module_name:
            try:
                parser_module = importlib.import_module(parser_module_name)
            except ModuleNotFoundError as err:
                logging.warning(f"Importing parser module failed: {err}")
                # TODO: store data for future re-processing
                continue
            try:
                packet_timestamp, datalines = parser_module.create_datalines_from_raw_unpacked_data(unpacked_data)
                parsed_data = create_parsed_data_message(packet_timestamp, datalines, device_data)
                logging.debug(pformat(parsed_data))
                packed_data = data_pack(parsed_data)
                producer.send(parsed_data_topic, packed_data).add_callback(on_send_success).add_errback(on_send_error)
            except Exception as err:
                logging.exception(f"Failed to get parser module: {err}")
                # TODO: send data to spare topic for future reprocessing?


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Bye!")

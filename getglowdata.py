#!/usr/bin/env python3
"""
Programme 'getglowdata'.

Downloads energy usage and cost data from the API, publishes to mqtt and writes to file.

The programme will download any historic data from the start date defined,
and will then continuously poll for new data according to the defined interval

Functions:
    setup_logging
    publish_readings
    process_data
    do_process_loop
    main
"""
import io
import sys
import logging
from typing import NoReturn, IO
import json

from time import sleep
from datetime import datetime

import pytz

import paho.mqtt.client as mqtt

from glow import GlowConfig, GlowClient, Checkpoint
from glow import ConfigError, GlowAPIError, CheckpointError, InvalidPeriod
from glow import round_down, max_end_dt

MQTT_CLIENT_CONNECTED = False


class MqttError(Exception):
    """MQTT exceptions."""


def setup_logging(log_name: str, log_level: str) -> None:
    """
    Configure the logging system.

    :param log_name: name of the log file to use
    :param log_level: log level to use
    :return: None
    """
    logging.basicConfig(
        filename=log_name,
        level=log_level,
        format='%(asctime)s: %(name)-12s %(levelname)-8s %(message)s'
    )


def publish_readings(resource, readings, period, mqtt_client, mqtt_topic, metrics_fp):
    """
    Publish the list of readings to mqtt and the output fil metrics_fp.

    :param resource: the relevant glow resource
    :param readings: the list of readings
    :param period: aggregation period for the readings
    :param mqtt_client: mqtt publish connection
    :param mqtt_topic: topic to publish on
    :param metrics_fp: output file for the readings

    """
    # pylint: disable=too-many-arguments
    if resource.base_unit == "pence":
        multiplier = 0.01
        units = "GBP"
    else:
        multiplier = 1
        units = resource.base_unit

    topic = mqtt_topic + '/' + resource.res_type()

    for reading in readings:

        if reading[1] is not None:
            reading[1] = round(reading[1] * multiplier, 5)

            if mqtt_client is not None:
                payload = json.dumps({'metric_fname': resource.res_type(),
                                      'supply_type': resource.supply_type(),
                                      'metric_type': 'consumption',
                                      'period': period,
                                      'timestamp': reading[0],
                                      'timestamp_str':
                                          datetime.fromtimestamp(reading[0], pytz.UTC).isoformat(),
                                      'metric_value': reading[1],
                                      'units': units})

                try:
                    message = mqtt_client.publish(topic, payload)
                    message.wait_for_publish()

                except ValueError as err:
                    # failed to queue message
                    raise MqttError('MQTT publish failed') from err

        else:
            msg = f'value at timestamp {str(reading[0])} ' \
                  f'[{datetime.fromtimestamp(reading[0], pytz.UTC).isoformat()}] ' \
                  f'for classifier {resource.classifier} with period {period} is Null - dropped'

            logging.info(msg)

        # log data for one of the classifiers for info
        msg = (
            f'{period}, {resource.res_type()}, {resource.supply_type()},'
            f'{reading[0]}, {datetime.fromtimestamp(reading[0], pytz.UTC).isoformat()}, '
            f'{str(reading[1])}, {units}'
        )

        logging.debug('published: %s', msg)
        metrics_fp.write(msg + '\n')

    metrics_fp.flush()


def process_data(glow: GlowClient, virtual_entities: list, conf: GlowConfig, dt_from: datetime,
                 dt_to: datetime, pub: mqtt.Client, metrics_fp: IO) -> None:
    """
    Retrieve the glow resources (and readings for each resource) in the list of virtual entities.

    :param glow: the Glow object
    :param virtual_entities: list of virtual entities that this connection is has permission to read
    :param conf: the glow configuration
    :param dt_from: start of time period for which data is being retrieved
    :param dt_to: end of time period for which data is being retrieved
    :param pub: mqtt publish connection
    :param metrics_fp: metrics output file
    """
    # pylint: disable=too-many-arguments

    for this_ve in virtual_entities:

        logging.info('Requesting data for %s to %s ...', dt_from, dt_to)

        # Glow query will fail if  dt_from = dt_to. This should not happen with a
        # wait period >= 1 Min, and rounding down dt_from and dt_to to the minute

        if dt_from == dt_to:
            logging.error("process data: dt_from == dt_to [%s [%s]", dt_from, dt_to)
            raise InvalidPeriod("Error in process_data: dt_from == dt_to")

        for res in glow.get_resources(this_ve):

            if not glow.check_valid_period(res, conf.period):
                continue

            readings = glow.get_readings(res, dt_from, dt_to, conf.period)

            if not conf.incomplete_periods:
                # The glow response will always include a partial reading for the last datapoint:
                #   - if dt_to is not on "period" boundary (this is expected)
                #   - if dt_to is on a 'period' boundary (partial reading starts at dt_to)
                #  Thus, to avoid partial readings, we should drop the last reading in all cases

                readings.pop()

            publish_readings(res, readings, conf.period, pub, conf.mqtt_topic, metrics_fp)


def do_process_loop(glow: GlowClient, mqtt_conn: mqtt.Client, checkpoint: Checkpoint,
                    metrics_fp: io.TextIOWrapper, conf: GlowConfig) -> NoReturn:
    """
    Generate start / end time for the next dataset, process, and repeat is a continuous loop.

    Once the end time is in the future (i.e. now up to-date), the function sleep for a wait time
    (defined in the config file) before looping again

    :param glow: Glow connection object - includes e.g. Glow virtual entities
    :param mqtt_conn: mqtt publish connection
    :param checkpoint: checkpoint maintains the timestamp for the last Glow data published
    :param metrics_fp: output file for glow data points
    :param conf: glow configuration data (comprising e.g. period..)
    """
    # get dt_from from checkpoint. This should be rounded to a period boundary
    dt_from = checkpoint.last_dt

    # Glow does not like fractional seconds = we'll just round down to complete minutes
    now = round_down(datetime.now().astimezone(pytz.UTC), 'PT1M')

    # There is a limit to the number of datapoints that can be
    # requested in each call of "get_readings" -
    # If dt_from is not recent, we'll need to get outstanding data in tranches.

    dt_to = min(now, max_end_dt(dt_from, conf.period))

    # logging.info("do_process_loop(start): now: %s, from: %s, to: %s", now, dt_from, dt_to)

    ve_list = glow.get_virtual_entities()

    while True:

        if not glow.token_check_valid():
            glow.authenticate()

        process_data(glow, ve_list, conf, dt_from, dt_to, mqtt_conn, metrics_fp)
        # process_data(glow_conn, mqtt_con, metrics_fp, conf, ve_list, dt_from, dt_to)
        # note: occasionally zeroes are returned if glow has not updated.
        # Need to see how we detect / fix
        # if we are not yet up-to-date with the data,
        # then don't wait to get the next tranche of the backlog...

        if now == dt_to:
            logging.info('sleeping for %d minutes...', conf.wait_minutes)
            sleep(conf.wait_minutes * 60)
            now = round_down(datetime.now().astimezone(pytz.UTC), 'PT1M')

        dt_from = round_down(dt_to, conf.period)
        dt_to = min(now, max_end_dt(dt_from, conf.period))

        checkpoint.update_checkpoint(conf.period, round_down(dt_to, conf.period))

        # logging.info("do_process_loop(loop): now: %s, from: %s, to: %s", now, dt_from, dt_to)


def on_connect(client, userdata, flags, response_code):
    """Update global var MQTT_CLIENT_CONNECTED on successful connection."""
    global MQTT_CLIENT_CONNECTED

    _resp_text = {0: 'Connection successful',
                  1: 'Connection refused - incorrect protocol version',
                  2: 'Connection refused - invalid client',
                  3: 'Connection refused - server unavailable',
                  4: 'Coonection refused - bad username or password',
                  5: 'Connection refused - not authorised'}

    logging.info('MQTT %s', _resp_text[response_code])
    if response_code == 0:
        MQTT_CLIENT_CONNECTED = True


def on_disconnect(client, userdata, response_code):
    """Update global var MQTT_CLIENT_CONNECTED on disconnection."""
    global MQTT_CLIENT_CONNECTED

    MQTT_CLIENT_CONNECTED = False

    if response_code == 0:
        logging.info('MQTT disconnected')
    else:
        logging.info('MQTT disconnected unexpectedly')


def connect_to_mqtt(host, username, password):
    """Connect to MQTT and start MQTT loop."""
    global MQTT_CLIENT_CONNECTED

    MQTT_CLIENT_CONNECTED = False
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.username_pw_set(username, password)
    mqtt_client.connect(host)
    mqtt_client.loop_start()

    # make sure we are connected...
    retries = 3
    while True:
        # MQTT_CLIENT_CONNECTED is set via the on_connect callback
        if MQTT_CLIENT_CONNECTED:
            logging.info('MQTT Connection acknowledged')
            break

        if retries > 0:
            logging.info('Waiting for MQTT connection acknowledgement')
            retries = retries - 1
            sleep(1)
        else:
            raise MqttError('MQTT connection failure')

    return mqtt_client


def main():
    """Get configuration and initialise, then call main process loop."""
    # GET CONFIGURATION & VALIDATE, CONFIGURE LOGGING

    #
    # get configuration, set up logging and validate config
    #
    try:
        conf = GlowConfig()

    except ConfigError as err:
        print(repr(err))
        sys.exit(1)

    setup_logging(conf.logfile, conf.loglevel)
    conf.write_to_log()

    # create checkpoint object and set start time as:
    #   1. time in checkpoint file, or
    #   2. current time (rounded down to start of current period) where period is an ISO_PERIOD
    # glow requires  weekly / monthly data requests to be aligned on weekly / monthly boundaries
    # - so we'll do the same for all time periods.

    default_start_time = round_down(datetime.now().astimezone(pytz.UTC), conf.period)

    try:
        checkpoint = Checkpoint(conf.checkpoint_file, conf.period, default_start_time)

    except CheckpointError as err:
        logging.fatal(repr(err))
        sys.exit(2)

    logging.info('Default start time is: %s', default_start_time.isoformat())
    logging.info('Actual start time is: %s', checkpoint.last_dt.isoformat())
    logging.info('Checkpoint file is: %s', checkpoint.path)
    logging.info('Checkpoint time is: %s with period %s',
                 checkpoint.last_dt.isoformat(), checkpoint.period)

    try:
        # Set up connection to Glow
        glow = GlowClient(conf.glow_user, conf.glow_pwd)
        logging.info("Connected to Glow API..")

    except GlowAPIError as err:
        logging.fatal(repr(err))
        sys.exit(3)

    # Set up connection to MQTT (if enabled) and start MQTT loop

    try:
        if conf.mqtt_enabled:
            mqtt_client = connect_to_mqtt(conf.mqtt_host, conf.mqtt_user, conf.mqtt_pwd)
            logging.info('Publishing to MQTT server %s on topic "%s/+" ',
                         conf.mqtt_host, conf.mqtt_topic)
        else:
            mqtt_client = None
            logging.info('Publishing to MQTT disabled')

    except MqttError as err:
        logging.fatal(repr(err))
        sys.exit(4)

    # Set up metrics file
    try:
        metrics_fp = open(conf.output_file, 'a', encoding="utf-8")
        logging.info('Publishing to file "%s"', conf.output_file)

    except OSError as err:
        logging.fatal('Error opening metrics file %s', conf.output_file)
        sys.exit(5)

    try:
        # call the main process loop to request / process glow data
        do_process_loop(glow, mqtt_client, checkpoint, metrics_fp, conf)

    except (GlowAPIError, MqttError, InvalidPeriod) as err:
        metrics_fp.close()
        checkpoint.close()
        logging.fatal(repr(err))
        sys.exit(6)


if __name__ == '__main__':
    main()

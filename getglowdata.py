#!/usr/bin/env python3
"""
add docstring for getflowdata
"""

import sys
import logging
from time import sleep
from datetime import datetime

import pytz
import json

import paho.mqtt.client as mqtt

from glow import GlowConfig, GlowClient, Checkpoint
from glow import round_down, max_end_dt

def setup_logging(log_name, log_level):
    """

    :param log_name:
    :type log_name:
    :param log_level:
    :type log_level:
    :return:
    :rtype:
    """
    logging.basicConfig(
        filename=log_name,
        level=log_level,
        format='%(asctime)s: %(name)-12s %(levelname)-8s %(message)s'
    )


def publish_readings(resource, readings, period, mqtt_client, mqtt_topic, metrics_fp):
    """

    :param resource:
    :type resource:
    :param readings:
    :type readings:
    :param period:
    :type period:
    :param mqtt_client:
    :type mqtt_client:
    :param mqtt_topic:
    :type mqtt_topic:
    :param metrics_fp:
    :type metrics_fp:
    :return:
    :rtype:
    """
    if resource.base_unit == "pence":
        multiplier = 0.01
        units = "GBP"
    else:
        multiplier = 1
        units = resource.base_unit


    topic = mqtt_topic + '/' + resource.res_type()

    for r in readings:

        if r[1] is not None:
            r[1] = round(r[1] * multiplier, 5)

            if mqtt_client is not None:
                payload = json.dumps({'metric_fname': resource.res_type(),
                                      'supply_type': resource.supply_type(),
                                      'metric_type': 'consumption',
                                      'period': period,
                                      'timestamp': r[0],
                                      'timestamp_str':
                                          datetime.fromtimestamp(r[0], pytz.UTC).isoformat(),
                                      'metric_value': r[1],
                                      'units': units})

                message = mqtt_client.publish(topic, payload)
                message.wait_for_publish()
        else:
            msg = f'value at timestamp {str(r[0])} ' \
                  f'[{datetime.fromtimestamp(r[0], pytz.UTC).isoformat()}] ' \
                  f'for classifier {resource.classifier} with period {period} is Null - dropped'

            logging.info(msg)

        # log data for one of the classifiers for info
        msg = (
            f'{period}, {resource.res_type()}, {resource.supply_type()},'
            f'{r[0]}, {datetime.fromtimestamp(r[0], pytz.UTC).isoformat()}, '
            f'{str(r[1])}, {units}'
        )

        logging.info('published: %s', msg)
        metrics_fp.write(msg + '\n')

    metrics_fp.flush()


def process_data(glow, virtual_entities, period, dt_from, dt_to, pub, mqtt_topic,
                 no_incomplete_periods, metrics_fp):
    """

    :param glow:
    :type glow:
    :param virtual_entities:
    :type virtual_entities:
    :param period:
    :type period:
    :param dt_from:
    :type dt_from:
    :param dt_to:
    :type dt_to:
    :param pub:
    :type pub:
    :param mqtt_topic:
    :type mqtt_topic:
    :param no_incomplete_periods:
    :type no_incomplete_periods:
    :param metrics_fp:
    :type metrics_fp:
    :return:
    :rtype:
    """

    for ve in virtual_entities:

        logging.info('Requesting data for %s to %s ...', dt_from, dt_to)

        # The glow response will always include a partial reading for the last datapoint:
        #   - if dt_to is not on "period" boundary (this is expected)
        #   - if dt_to is on a 'period' boundary (in which case the partial reading starts at dt_to)
        #
        #  Thus, to avoid partial readings, we should drop the last reading in all cases

        if not glow.token_check_valid():
            glow.authenticate()

        for res in glow.get_resources(ve):

            if period == "PT1M":
                if res.classifier not in ['electricity.consumption']:
                    # only electricity.consumption is valid for PT1M
                    continue

            # Glow query will fail if  dt_from = dt_to. This should not happen with a
            # wait period >= 1 Min, and rounding down dt_from and dt_to to the minute

            if dt_from == dt_to:
                logging.error("process data: dt_from == dt_to [%s [%s]", dt_from, dt_to)
                raise RuntimeError("Error in process_data: dt_from == dt_to")

            readings = glow.get_readings(res, dt_from, dt_to, period)

            if no_incomplete_periods:
                readings.pop()

            publish_readings(res, readings, period, pub, mqtt_topic, metrics_fp)


def do_process_loop(glow_conn, mqtt_conn, checkpoint, metrics_fp, conf):
    """

    :param glow_conn:
    :type glow_conn:
    :param mqtt_conn:
    :type mqtt_conn:
    :param checkpoint:
    :type checkpoint:
    :param metrics_fp:
    :type metrics_fp:
    :param conf:
    :type conf:
    :return:
    :rtype:
    """
    # Glow does not like fractional seconds = we'll just round down to complete minutes

    # get dt_from from checkpoint. This should be rounded to a period boundary
    dt_from = checkpoint.dt

    # Glow does not like fractional seconds = we'll just round down to complete minutes
    now = round_down(datetime.now().astimezone(pytz.UTC), 'PT1M')

    # There is a limit to the number of datapoints that can be
    # requested in each call of "get_readings" -
    # If dt_from is not recent, we'll need to get outstanding data in tranches.

    dt_to = min(now, max_end_dt(dt_from, conf.period))

    # logging.info("do_process_loop(start): now: %s, from: %s, to: %s", now, dt_from, dt_to)

    ve_list = glow_conn.get_virtual_entities()

    while True:
        process_data(glow_conn, ve_list, conf.period, dt_from, dt_to, mqtt_conn, conf.mqtt_topic,
                     conf.incomplete_periods, metrics_fp)
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


def main():
    """
    :return:
    :rtype:
    """
    # GET CONFIGURATION & VALIDATE, CONFIGURE LOGGING
    try:
        #
        # get configuration, set up logging and validate config
        #
        conf = GlowConfig()

        setup_logging(conf.logfile, conf.loglevel)
        conf.write_to_log()

        # create checkpoint object and set start time as:
        #   1. time in checkpoint file, or
        #   2. current time (rounded down to start of current period) where period is an ISO_PERIOD
        # glow requires  weekly / monthly data requests to be aligned on weekly / monthly boundaries
        # - so we'll do the same for all time periods.

        default_start_time = round_down(datetime.now().astimezone(pytz.UTC), conf.period)

        logging.info('Default start time is: %s', default_start_time.isoformat())

        checkpoint = Checkpoint(conf.checkpoint_file, conf.period, default_start_time)

        logging.info('Checkpoint file is: %s', checkpoint.path)
        logging.info('Checkpoint time is: %s with period %s',
                     checkpoint.dt.isoformat(), checkpoint.period)

        logging.info('Actual start time is: %s', checkpoint.dt.isoformat())

        # Set up connection to Glow
        glow = GlowClient(conf.glow_user, conf.glow_pwd)
        logging.info("Connected to Glow API..")

        # Set up connection to MQTT (if enabled) and start MQTT loop
        if conf.mqtt_enabled:
            mqtt_client = mqtt.Client()
            mqtt_client.username_pw_set(conf.mqtt_user, conf.mqtt_pwd)
            mqtt_client.connect(conf.mqtt_host)
            mqtt_client.loop_start()
            logging.info('Publishing to MQTT server %s on topic "%s/+" ',
                         conf.mqtt_host, conf.mqtt_topic)
        else:
            mqtt_client = None
            logging.info('Publishing to MQTT disabled')

        # set up metrics file

        try:
            metrics_fp = open(conf.output_file, 'a')
            logging.info('Publishing to file "%s"', conf.output_file)

        except Exception as err:
            logging.critical(err)
            print(err)
            sys.exit()

        # call the main process loop to request / process glow data
        do_process_loop(glow, mqtt_client, checkpoint, metrics_fp, conf)

    except SystemExit:
        logging.critical('system exit')
    except Exception as err:
        print(err)
        logging.critical(err)
        print(err)
        sys.exit()


if __name__ == '__main__':
    main()

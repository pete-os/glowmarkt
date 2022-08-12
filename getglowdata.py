#!/usr/bin/env python3


import json
import logging
import os
import sys
from datetime import datetime
from time import sleep

import paho.mqtt.client as mqtt
import pytz
from dotenv import dotenv_values

from GlowClient import GlowClient, round_down, Checkpoint, max_end_dt

ISO_PERIODS = ['PT1M', 'PT30M', 'PT1H', 'P1D', 'P1W', 'P1M']
MIN_WAIT_MINUTES = 15
MAX_WAIT_MINUTES = 1500
ENV_FILE = "glow.env"
DEF_LOGFILE = "glow.log"
DEF_LOGDIR = "."
DEF_LOGLEVEL = "WARNING"

class_to_fname = {
        'electricity.consumption': 'energy',
        'gas.consumption': 'energy',
        'electricity.consumption.cost': 'cost',
        'gas.consumption.cost': 'cost',
    }

class_to_supply = {
        'electricity.consumption': 'electricity',
        'gas.consumption': 'gas',
        'electricity.consumption.cost': 'electricity',
        'gas.consumption.cost': 'gas',
    }

def get_config(env_file):
    result = {
        **dotenv_values(env_file)
    }
    return result


def setup_logging(log_name, log_level):
    logging.basicConfig(
        filename=log_name,
        level=log_level,
        format='%(asctime)s: %(name)-12s %(levelname)-8s %(message)s'
    )

    return


def validate_config(conf_dict):
    if not conf_dict['GLOW_PERIOD']:
        logging.critical("GLOW_PERIOD NOT SET")

    if conf_dict['GLOW_PERIOD'] not in ISO_PERIODS:
        logging.critical("Invalid period %s", conf_dict['GLOW_PERIODS'])

    wait_minutes = int(conf_dict['GLOW_WAIT_MINUTES'])
    if wait_minutes < MIN_WAIT_MINUTES or wait_minutes > MAX_WAIT_MINUTES:
        logging.critical("Invalid wait minutes %s", wait_minutes)
        sys.exit()

    if not conf_dict['GLOW_CHECKPOINT_FILE']:
        logging.critical("GLOW_CHECKPOINT_FILE is not defined")

    if not conf_dict['GLOW_OUTPUT_FILE']:
        logging.critical("GLOW_OUTPUT_FILE not set")
        sys.exit()

    if not conf_dict['GLOW_MQTT_OUTPUT']:
        logging.critical("GLOW_MQTT_OUTPUT not set")
        sys.exit()

    if not conf_dict['GLOW_OUTPUT_FILE']:
        logging.critical("GLOW_OUTPUT_FILE not set")
        sys.exit()

    else:

        if str_to_bool(conf_dict['GLOW_MQTT_OUTPUT']):
            if not conf_dict['GLOW_MQTT_HOSTNAME']:
                logging.critical("GLOW_MQTT_HOSTNAME not set")
            elif not conf_dict['GLOW_MQTT_USERNAME']:
                logging.critical("GLOW_MQTT_USERNAME not set")
            elif not conf_dict['GLOW_MQTT_PASSWORD']:
                logging.critical('GLOW_MQTT_PASSWORD not set')
            elif not conf_dict['GLOW_MQTT_TOPIC']:
                logging.critical('GLOW_MQTT_TOPIC not set')
        else:
            logging.warning("MQTT Output disabled")

    if not conf_dict['GLOW_USERNAME']:
        logging.critical("GLOW_USERNAME not set")
        sys.exit()

    elif not conf_dict['GLOW_PASSWORD']:
        logging.critical("GLOW_PASSWORD not set")
        sys.exit()

    logging.info('')
    logging.info('')
    logging.info('STARTED - running %s with config file %s', os.path.basename(sys.argv[0]), ENV_FILE)
    logging.info('log level = %s', conf_dict.get('GLOW_LOGLEVEL', DEF_LOGLEVEL))

    logging.info("Aggregation Period is %s, no_incomplete_periods = %s",
                 conf_dict['GLOW_PERIOD'], conf_dict['GLOW_NO_INCOMPLETE_PERIODS'])
    logging.info("Wait minutes is %s", conf_dict['GLOW_WAIT_MINUTES'])


def str_to_bool(s):
    if s.lower() == 'true':
        return bool(True)
    else:
        return bool(False)


def publish_readings(resource, readings, period, mqtt_client, mqtt_topic, metrics_fp):

    if resource.base_unit == "pence":
        multiplier = 0.01
        units = "GBP"
    else:
        multiplier = 1
        units = resource.base_unit

    topic = mqtt_topic + '/' + class_to_fname[resource.classifier]

    for r in readings:

        if r[1] is not None:
            r[1] = round(r[1] * multiplier, 5)

            if mqtt_client is not None:
                payload = json.dumps({'metric_fname': class_to_fname[resource.classifier],
                                      'supply_type': class_to_supply[resource.classifier],
                                      'metric_type': 'consumption',
                                      'period': period,
                                      'timestamp': r[0],
                                      'timestamp_str': datetime.fromtimestamp(r[0], pytz.UTC).isoformat(),
                                      'metric_value': r[1],
                                      'units': units})

                message = mqtt_client.publish(topic, payload)
                message.wait_for_publish()
        else:
            logging.warning('value at timestamp %s [%s] for classifier %s with period %s is Null - dropped',
                            str(r[0]), datetime.fromtimestamp(r[0], pytz.UTC).isoformat(),
                            resource.classifier, period)

        # log data for one of the classifiers for info
        msg = (
            f'{period}, {class_to_fname[resource.classifier]}, '
            f'{class_to_supply[resource.classifier]}, {r[0]}, '
            f'{datetime.fromtimestamp(r[0], pytz.UTC).isoformat()}, '
            f'{str(r[1])}, {units}'
        )

        metrics_fp.write(msg + '\n')

    metrics_fp.flush()

    return


def process_data(glow, virtual_entities, period, dt_from, dt_to, pub, mqtt_topic,
                 no_incomplete_periods, metrics_fp):

    for ve in virtual_entities:

        logging.info('Requesting data for %s to %s ...', dt_from, dt_to)

        # The glow response will always include a partial reading for the last datapoint:
        #     - if dt_to is not on "period" boundary (this is expected)
        #     - if dt_to is on a 'period' boundary (in which case the partial reading starts at dt_to)
        #
        #  Thus, if we want to avoid partial readings, we should drop the last reading in all cases

        if not glow.token_check_valid():
            glow.authenticate()

        for res in glow.get_resources(ve):

            if period == "PT1M":
                if res.classifier not in ['electricity.consumption']:
                    # only electricity.consumption is valid for PT1M
                    continue

            # Glow query will fail if  dt_from = dt_to. This should not happen with a wait period >= 1 Min
            # and rounding down dt_from and dt_to to the minute

            if dt_from == dt_to:
                logging.error("process data: dt_from == dt_to [%s [%s]", dt_from, dt_to)
                raise RuntimeError("Error in process_data: dt_from == dt_to")

            readings = glow.get_readings(res, dt_from, dt_to, period)

            if no_incomplete_periods:
                readings.pop()


            publish_readings(res, readings, period, pub, mqtt_topic, metrics_fp)



def do_process_loop(glow_conn, mqtt_conn, mqtt_topic,
                    period, no_incomplete_periods, wait_minutes, checkpoint, metrics_fp):
    # Glow does not like fractional seconds = we'll just round down to complete minutes

    # get dt_from from checkpoint. This should be rounded to a period boundary
    dt_from = checkpoint.dt

    # Glow does not like fractional seconds = we'll just round down to complete minutes
    now = round_down(datetime.now().astimezone(pytz.UTC), 'PT1M')

    # There is a limit to the number of datapoints that can be requested in each call of "get_readings" -
    # If dt_from is not recent, we'll need to get outstanding data in tranches.

    dt_to = min(now, max_end_dt(dt_from, period))

    # logging.info("do_process_loop(start): now: %s, from: %s, to: %s", now, dt_from, dt_to)

    ve_list = glow_conn.get_virtual_entities()

    while True:
        process_data(glow_conn, ve_list, period, dt_from, dt_to, mqtt_conn, mqtt_topic,
                     no_incomplete_periods, metrics_fp)

        # note: occasionally zeroes are returned if glow has not updated. Need to see how we detect / fix
        # if we are not yet up-to-date with the data, then don't wait to get the next tranche of the backlog..

        if now == dt_to:
            logging.info(f'sleeping for {wait_minutes} minutes...')
            sleep(wait_minutes * 60)
            now = round_down(datetime.now().astimezone(pytz.UTC), 'PT1M')

        dt_from = round_down(dt_to, period)
        dt_to = min(now, max_end_dt(dt_from, period))

        checkpoint.update_checkpoint(period, round_down(dt_to, period))

        # logging.info("do_process_loop(loop): now: %s, from: %s, to: %s", now, dt_from, dt_to)


def main():
    # GET CONFIGURATION & VALIDATE, CONFIGURE LOGGING
    try:
        #
        # get configuration, set up logging and validate config
        #
        conf = get_config(ENV_FILE)

        setup_logging(os.path.join(conf.get('GLOW_LOGDIR', DEF_LOGDIR),
                                   conf.get('GLOW_LOGFILE', DEF_LOGFILE)),
                      conf.get('GLOW_LOGLEVEL', DEF_LOGLEVEL))

        validate_config(conf)

        # create checkpoint object and set start time as:
        #   1. time in checkpoint file, or
        #   2. current time (rounded down to start of current period) where period is an ISO_PERIOD
        # glow requires  weekly / monthly data requests to be aligned on weekly / monthly boundaries - so we'll
        # do the same for all time periods.

        default_start_time = round_down(datetime.now().astimezone(pytz.UTC), conf.get('GLOW_PERIOD'))
        logging.info('Default start time is: %s', default_start_time.isoformat())

        cp = Checkpoint(os.path.join(conf.get('GLOW_LOGDIR', DEF_LOGDIR), conf.get('GLOW_CHECKPOINT_FILE')),
                        conf.get('GLOW_PERIOD'), default_start_time)

        logging.info('Checkpoint file is: %s', cp.path)
        logging.info('Checkpoint time is: %s with period %s', cp.dt.isoformat(), cp.period)
        logging.info('Actual start time is: %s', cp.dt.isoformat())

        # Set up connection to Glow
        glow = GlowClient(conf['GLOW_USERNAME'], conf['GLOW_PASSWORD'])
        logging.info("Connected to Glow API..")

        # Set up connection to MQTT (if enabled) and start MQTT loop
        if str_to_bool(conf['GLOW_MQTT_OUTPUT']):
            mqtt_client = mqtt.Client()
            mqtt_client.username_pw_set(conf['GLOW_MQTT_USERNAME'], conf['GLOW_MQTT_PASSWORD'])
            mqtt_client.connect(conf['GLOW_MQTT_HOSTNAME'])
            mqtt_client.loop_start()
            logging.info('Publishing to MQTT server %s on topic "%s/+" ', conf['GLOW_MQTT_HOSTNAME'], conf['GLOW_MQTT_TOPIC'])
        else:
            mqtt_client = None
            logging.info('Publishing to MQTT disabled')

        # set up metrics file

        metrics_filename = os.path.join(
            conf.get('GLOW_LOGDIR', DEF_LOGDIR), conf.get('GLOW_OUTPUT_FILE'))

        logging.info('Publishing to file "%s"', metrics_filename)

        try:
            metrics_fp = open(metrics_filename, 'a')

        except Exception as err:
            logging.critical(err)
            print(err)
            sys.exit()

        # call the main process loop to request / process glow data
        do_process_loop(glow, mqtt_client, conf['GLOW_MQTT_TOPIC'], conf['GLOW_PERIOD'],
                        str_to_bool(conf['GLOW_NO_INCOMPLETE_PERIODS']),
                        int(conf['GLOW_WAIT_MINUTES']),
                        cp, metrics_fp)


    except Exception as err:
        logging.critical(err)
        print(err)
        sys.exit()


if __name__ == '__main__':
    main()

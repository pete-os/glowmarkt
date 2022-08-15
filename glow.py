"""
Module glow
"""
import sys
import os
import logging
import json

from datetime import datetime

import pytz
from dateutil.relativedelta import relativedelta, MO
from dotenv import dotenv_values
import requests


ISO_PERIODS = ['PT1M', 'PT30M', 'PT1H', 'P1D', 'P1W', 'P1M']
MIN_WAIT_MINUTES = 15
MAX_WAIT_MINUTES = 1500
ENV_FILE = "glow.env"

DEF_LOGFILE = "glow.log"
DEF_CHECKPOINT_FILE = "glow.checkpoint"
DEF_LOGDIR = "."
DEF_LOGLEVEL = "WARNING"

def str_to_bool(s):
    """

    :param s:
    :type s:
    :return:
    :rtype:
    """
    if s.lower() == 'true':
        return bool(True)

    return bool(False)


def start_of_day(dt: datetime):
    """
    do this
    :param dt:
    :type dt:
    :return:
    :rtype:
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def round_down(dt: datetime, period):
    """
    do this
    :param dt:
    :type dt:
    :param period:
    :type period:
    :return:
    :rtype:
    """
    if period == "PT1M":
        result = dt.replace(second=0, microsecond=0)

    elif period == "PT30M":
        result = dt.replace(minute=int(dt.minute / 30) * 30, second=0, microsecond=0)

    elif period == "PT1H":
        result = dt.replace(minute=0, second=0, microsecond=0)

    elif period == "P1D":
        result = dt.replace(hour=0, minute=0, second=0, microsecond=0)

    elif period == "P1W":
        result = dt + relativedelta(weekday=MO(-1), hour=0, minute=0, second=0, microsecond=0)

    elif period == "P1M":
        result = dt + relativedelta(day=1, hour=0, minute=0, second=0, microsecond=0)

    elif period == "P1Y":
        result = dt + relativedelta(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    else:
        raise RuntimeError("last_timeslice_end - invalid period")

    return result


def max_end_dt(dt, period):
    """
    do this
    :param dt:
    :type dt:
    :param period:
    :type period:
    :return:
    :rtype:
    """
    if period == "PT1M":
        result = dt + relativedelta(days=2)

    elif period == "PT30M":
        result = dt + relativedelta(days=10)

    elif period == "PT1H":
        result = dt + relativedelta(days=31)

    elif period == "P1D":
        result = dt + relativedelta(days=31)

    elif period == "P1W":
        result = dt + relativedelta(weeks=6)

    elif period == "P1M":
        result = dt + relativedelta(months=12)

    elif period == "P1Y":
        result = dt + relativedelta(months=12)
    else:
        raise RuntimeError("cal_from_to - invalid period")

    return result


class GlowError(Exception):
    """Base-class for all exceptions raised by this module"""


class ConfigError(GlowError):
    """Base-class for all exceptions raised by this module"""


class ConfigKeyError(GlowError):
    """Base-class for all exceptions raised by this module"""


class ConfigInvalidPeriodError(GlowError):
    """Base-class for all exceptions raised by this module"""


class ConfigInvalidWaitError(GlowError):
    """Base-class for all exceptions raised by this module"""


class GlowConfig:
    """
    do this
    """

    def __init__(self, env_file=ENV_FILE):
        """

        :param env_file:
        :type env_file:
        """

        _conf = {**dotenv_values(env_file)}

        try:
            self.env_file = env_file
            self.period = _conf['GLOW_PERIOD']
            self.glow_user = _conf['GLOW_USERNAME']
            self.glow_pwd = _conf['GLOW_PASSWORD']
            self.wait_minutes = int(_conf['GLOW_WAIT_MINUTES'])
            self.mqtt_enabled = str_to_bool(_conf['GLOW_MQTT_OUTPUT'])
            self.incomplete_periods = str_to_bool(_conf['GLOW_INCOMPLETE_PERIODS'])
            self.loglevel = _conf.get('GLOW_LOGLEVEL', DEF_LOGLEVEL)
            self.logdir = _conf.get('GLOW_LOGDIR', DEF_LOGDIR)

            if not os.path.exists(self.logdir):
                print(f'Conf file err: GLOW_LOGDIR {self.logdir} does not exist')

            self.output_file = os.path.join(self.logdir,
                                            _conf['GLOW_OUTPUT_FILE'])

            self.logfile = os.path.join(self.logdir,
                                        _conf.get('GLOW_LOGFILE', DEF_LOGFILE))

            self.checkpoint_file = os.path.join(
                self.logdir,_conf.get('GLOW_CHECKPOINT_FILE', DEF_CHECKPOINT_FILE))

            if self.mqtt_enabled:
                self.mqtt_host = _conf['GLOW_MQTT_HOSTNAME']
                self.mqtt_user = _conf['GLOW_MQTT_USERNAME']
                self.mqtt_pwd = _conf['GLOW_MQTT_PASSWORD']
                self.mqtt_topic = _conf['GLOW_MQTT_TOPIC']

            if self.period not in ISO_PERIODS:
                print(f'Conf file err: Invalid period {self.period} in {env_file}')
                raise ConfigInvalidPeriodError

            if self.wait_minutes < MIN_WAIT_MINUTES:
                print(f'Conf file error: WAIT_MINUTES must not be less than {MIN_WAIT_MINUTES}')
                raise ConfigInvalidWaitError

            if self.wait_minutes > MAX_WAIT_MINUTES:
                print(f'Conf file error: WAIT_MINUTES must not be greater than {MAX_WAIT_MINUTES}')
                raise ConfigInvalidWaitError

        except KeyError as e:
            print(f'Conf file err: {e.args[0]} is not defined in {env_file}')
            raise ConfigKeyError

        except Exception as err:
            print(f'Conf file error')
            raise ConfigError

        else:
            print('done')

    def write_to_log(self):
        """

        :return:
        :rtype:
        """

        logging.info('')
        logging.info('')
        logging.info('STARTED - running %s with config file %s',
                     os.path.basename(sys.argv[0]), self.env_file)

        logging.info('log level = %s', self.loglevel)

        logging.info("Aggregation Period is %s, no_incomplete_periods = %s",
                     self.period, self.incomplete_periods)

        logging.info("Wait minutes is %s", self.wait_minutes)


class Checkpoint:
    def __init__(self, path, period, def_start_dt):
        """

        :param path:
        :type path:
        :param period:
        :type period:
        :param def_start_dt:
        :type def_start_dt:
        """

        try:
            self.path = path
            self.period = None
            self.dt = None

            if os.path.exists(self.path):

                if not os.path.isfile(self.path):
                    raise RuntimeError("Checkpoint file exists but is not a file")

                # Valid checkpoint file exists, get checkpoint and period from file

                self.fp = open(self.path, 'r+')
                self.period, self.dt = self.get_from_file()

                if self.period != period:
                    print('checkpoint_init: period in checkpoint file is not the same as parameter',
                          self.period, period)

            else:
                print(f'setting checkpoint to default - period: {period} value: {def_start_dt}')
                self.fp = open(self.path, 'w+')

                self.update_checkpoint(period, def_start_dt)

        except Exception as err:

            self.fp.close()
            print(err)

    def get_from_file(self):
        """
        do this
        :return:
        :rtype:
        """

        try:
            self.fp.seek(0)

            tokens = self.fp.readline().strip().split(',')

            if tokens[0] not in ISO_PERIODS:
                raise RuntimeError("read_checkpoint: checkpoint does not start with 'period'")

            period = tokens[0]
            dt = datetime.fromisoformat(tokens[1])

        except ValueError:
            print('read_checkpoint: invalid isoformat string')
            self.fp.close()
            sys.exit()

            # if more data in file, raise error.
        if self.fp.readline().strip():
            self.fp.close()
            raise RuntimeError("checkpoint file has too much data")

        return period, dt

    def update_checkpoint(self, period, dt):
        """

        :param period:
        :type period:
        :param dt:
        :type dt:
        :return:
        :rtype:
        """

        try:

            self.fp.seek(0)
            self.fp.truncate()
            self.fp.write(period + ',' + dt.isoformat() + os.linesep)
            self.fp.flush()
            self.dt = dt
            self.period = period

        except Exception as err:
            self.fp.close()
            print(err)


class VirtualEntity:
    """
    do this
    """
    def __init__(self, payload):
        self.application_id = payload.get("applicationId")
        self.postalcode = payload.get("postalCode", '--- ---')
        self.name = payload.get("name")
        self.owner_id = payload.get("ownerId")
        self.type_id = payload.get("veTypeId")
        self.id = payload.get("veId")
        self.active = payload.get("active")
        # we won't bother getting resource summaries here..

    def __repr__(self):
        """"""
        return f'VirtualEntity("{self.type_id}", "{self.id}","{self.active}", ' \
               f'"{self.postalcode}","{self.name}", "{self.owner_id}"'


class Resource:
    """

    """
    def __init__(self, payload):
        self.id = payload["resourceId"]
        self.type_id = payload["resourceTypeId"]
        self.name = payload["name"]
        self.classifier = payload["classifier"]
        self.description = payload["description"]
        self.base_unit = payload["baseUnit"]

        self.temp = 1

    def supply_type(self):
        """"""
        return self.classifier.partition('.')[0]

    def res_type(self):
        """"""
        return self.classifier.partition('.')[2]

    def __repr__(self):
        """"""
        return f'Resource("{self.id}". "{self.type_id}", "{self.name}", ' \
               f'"{self.classifier}", "{self.base_unit}", ' \
               f'"{self.description}"'


class GlowClient:
    """
    do this
    """

    def __repr__(self):
        """
        do this
        :return:
        :rtype:
        """
        return 'token'

    def __init__(self, username, password):
        """
        do this
        :param username:
        :type username:
        :param password:
        :type password:
        """
        self.username = username
        self.password = password
        self.application = "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
        self.url = "https://api.glowmarkt.com/api/v0-1/"
        self.token = None
        self.token_expires = None
        self.session = requests.Session()
        if not self.authenticate():
            raise RuntimeError("Expected an authentication token")

    def authenticate(self):
        """
        do this
        :return:
        :rtype:
        """

        headers = {"Content-Type": "application/json", "applicationId": self.application}
        data = {"username": self.username, "password": self.password}

        url = self.url + "auth"

        resp = self.session.post(url, headers=headers, data=json.dumps(data))

        # responses:
        #   200: OK
        #   400: Bad Request
        #   401: Unauthorised
        #   500: Internal Server Error

        if resp.status_code != 200:
            raise RuntimeError("Authentication failed")

        resp = resp.json()

        if not resp["valid"]:
            raise RuntimeError("Expected an authentication token")

        if "token" not in resp:
            raise RuntimeError("Expected an authentication token")

        if "exp" not in resp:
            raise RuntimeError("Expected token expiry time")

        self.token = resp["token"]
        self.token_expires = resp["exp"]

        print("authenticated - token expires ",
              datetime.fromtimestamp(self.token_expires, pytz.UTC).isoformat())

        return True

    def get_virtual_entities(self):
        """

        :return:
        :rtype:
        """

        headers = {"Content-Type": "application/json",
                   "applicationId": self.application,
                   "token": self.token}

        url = self.url + "virtualentity"

        resp = self.session.get(url, headers=headers, timeout=10)

        # responses:
        #   200: OK
        #   500: Internal Server Error

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        ves = []

        for elt in resp:
            ve = VirtualEntity(elt)
            ves.append(ve)

        return ves

    def get_resources(self, virtual_entity):
        """

        :param virtual_entity:
        :type virtual_entity:
        :return:
        :rtype:
        """

        headers = {"Content-Type": "application/json",
                   "applicationId": self.application,
                   "token": self.token}

        url = self.url + "virtualentity/" + virtual_entity.id + "/resources"

        resp = self.session.get(url, headers=headers, timeout=10)

        # responses:
        #   200: OK
        #   404: not found
        #   500: Internal Server Error

        if resp.status_code != 200:
            raise RuntimeError("Request failed")
        resp = resp.json()

        resources = []

        for elt in resp["resources"]:
            res = Resource(elt)
            resources.append(res)

        return resources

    def get_readings(self, resource, t_from, t_to, period, func="sum"):
        """

        :param resource:
        :type resource:
        :param t_from:
        :type t_from:
        :param t_to:
        :type t_to:
        :param period:
        :type period:
        :param func:
        :type func:
        :return:
        :rtype:
        """

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token}

        if period not in ISO_PERIODS:
            raise RuntimeError("getreadings: 'period' is invalid")

        params = {
            "from": t_from.replace(tzinfo=None).isoformat(),
            "to": t_to.replace(tzinfo=None).isoformat(),
            "period": period,
            "offset": 0,
            "function": func,
            "nulls": 1,
        }
        url = self.url + "resource/" + resource.id + "/readings"

        resp = self.session.get(url, headers=headers, params=params, timeout=10)

        # responses:
        #   200: OK
        #   401: Unauthorised
        #   500: Internal Server Error

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        return resp["data"]

    def token_check_valid(self):
        """

        :return:
        :rtype:
        """
        now = datetime.now().astimezone(pytz.UTC)
        expiry_time = datetime.fromtimestamp(self.token_expires, pytz.UTC)
        if expiry_time > (now + relativedelta(days=1)):
            print('time remaining on token is: ', str(expiry_time - now))
            return True

        print('get new token - one hour or less remaining on current token')
        return False

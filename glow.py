"""
Module glow.

Classes:
    GlowConfig
    GlowClient
    Checkpoint
    GlowError
    ConfigError
    ConfigKeyError
    ConfigInvalidPeriodError
    ConfigInvalidWaitError

Functions:
    start_of_day(dt: datetime):
    round_down(dt: datetime, period):
    max_end_dt(dt, period):

Exception Classes:
    GlowError(Exception):
    ConfigError(GlowError):
    ConfigKeyError(GlowError):
    ConfigInvalidPeriodError(GlowError):
    ConfigInvalidWaitError(GlowError):
"""
import sys
import os
import logging
import json
from datetime import datetime
from typing import Tuple

import pytz
from dateutil.relativedelta import relativedelta, MO
from dotenv import dotenv_values
import requests

ISO_PERIODS = {'PT1M', 'PT30M', 'PT1H', 'P1D', 'P1W', 'P1M', 'P1Y'}
MIN_WAIT_MINUTES = 15
MAX_WAIT_MINUTES = 1500
ENV_FILE = "glow.env"

DEF_LOGFILE = "glow.log"
DEF_CHECKPOINT_FILE = "glow.checkpoint"
DEF_LOGDIR = "."
DEF_LOGLEVEL = "WARNING"
DEF_OUTPUT_FILE = "glow.dat"


class GlowError(Exception):
    """Base-class for all exceptions raised by this module."""


class ConfigError(GlowError):
    """Configuration error."""


class InvalidPeriod(GlowError):
    """Invalid Glow Aggregation Period."""


class CheckpointError(GlowError):
    """Error in Checkpoint file."""


class GlowAPIError(GlowError):
    """HTTP error response in Glow API call."""


def str_to_bool(string: str) -> bool:
    """Return a bool representation of a string - case-insensitive."""
    if string.lower() == 'true':
        return bool(True)

    return bool(False)


def start_of_day(this_dt: datetime) -> datetime:
    """Return a datetime object representing the start of day for the datetime supplied."""
    return this_dt.replace(hour=0, minute=0, second=0, microsecond=0)


def round_down(this_dt: datetime, period: str) -> datetime:
    """Return the datetime for the start of 'period' at datetime 'dt'.

    The glow API document states that weekly / monthly periods must be aligned with the start of
    the week and start of the month. Here, we use the same logic for all periods in order to
    avoid errors relating to partial periods.

    Example: if 'period' is 'P1M' (monthly), 00:00 for the first of the month will be returned.

    :param this_dt: datetime to be rounded down
    :param period: string containing an ISO_PERIOD
    :return: rounded-down datetime
    """
    if period not in ISO_PERIODS:
        raise InvalidPeriod("last_timeslice_end - invalid period")

    if period == "PT1M":
        result = this_dt.replace(second=0, microsecond=0)

    elif period == "PT30M":
        result = this_dt.replace(minute=int(this_dt.minute / 30) * 30, second=0, microsecond=0)

    elif period == "PT1H":
        result = this_dt.replace(minute=0, second=0, microsecond=0)

    elif period == "P1D":
        result = this_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    elif period == "P1W":
        result = this_dt + relativedelta(weekday=MO(-1), hour=0, minute=0, second=0, microsecond=0)

    elif period == "P1M":
        result = this_dt + relativedelta(day=1, hour=0, minute=0, second=0, microsecond=0)

    elif period == "P1Y":
        result = this_dt + relativedelta(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    else:
        raise InvalidPeriod("last_timeslice_end - period not implemented")

    return result


def max_end_dt(this_dt: datetime, period: str) -> datetime:
    """Return max possible datetime for a glow dataset with the given with start datetime & period.

    The glow 'get resource readings API limits the number of datapoints that can be retrieved by
    limiting the time period between the start datetime and  end datetime. This time period is
    dependent on the aggregation period 'period'.

    :param this_dt: datetime for the start of the time period - this should already be rounded down
    :param period: the aggregation period to use (one of ISO_PERIOD)
    :return: maximum end-datetime
    """
    if period == "PT1M":
        result = this_dt + relativedelta(days=2)

    elif period == "PT30M":
        result = this_dt + relativedelta(days=10)

    elif period == "PT1H":
        result = this_dt + relativedelta(days=31)

    elif period == "P1D":
        result = this_dt + relativedelta(days=31)

    elif period == "P1W":
        result = this_dt + relativedelta(weeks=6)

    elif period == "P1M":
        result = this_dt + relativedelta(months=12)

    elif period == "P1Y":
        result = this_dt + relativedelta(months=12)
    else:
        raise InvalidPeriod("cal_from_to - invalid period")

    return result


class GlowConfig:
    """Manages the set of configuration parameters that are defined in the glow config file."""

    # pylint: disable=too-few-public-methods
    # pylint: disable=too-many-instance-attributes

    def __init__(self, env_file=ENV_FILE):
        """
        Create a GlowConfig object using values stored in the config file 'env_file' & validate.

        :param env_file: the configuration file in env format
        """
        _conf = {**dotenv_values(env_file)}

        try:
            self.env_file = env_file
            self.period = _conf['GLOW_PERIOD']
            self.glow_user = _conf['GLOW_USERNAME']
            self.glow_pwd = _conf['GLOW_PASSWORD']
            self.wait_minutes = int(_conf['GLOW_WAIT_MINUTES'])
            self.incomplete_periods = str_to_bool(_conf['GLOW_INCOMPLETE_PERIODS'])
            self.loglevel = _conf.get('GLOW_LOGLEVEL', DEF_LOGLEVEL)
            self.logdir = _conf.get('GLOW_LOGDIR', DEF_LOGDIR)

            self.mqtt_enabled = str_to_bool(_conf['GLOW_MQTT_OUTPUT'])
            if self.mqtt_enabled:
                self.mqtt_host = _conf['GLOW_MQTT_HOSTNAME']
                self.mqtt_user = _conf['GLOW_MQTT_USERNAME']
                self.mqtt_pwd = _conf['GLOW_MQTT_PASSWORD']
                self.mqtt_topic = _conf['GLOW_MQTT_TOPIC']

        except KeyError as this_exception:
            raise ConfigError(f'Conf file err: {this_exception.args[0]} is not defined'
                              f' in {env_file}') from this_exception

        else:
            if not os.path.exists(self.logdir):
                raise ConfigError(f'Conf file err: GLOW_LOGDIR {self.logdir} does not exist')

            self.output_file = os.path.join(
                self.logdir, _conf.get('GLOW_OUTPUT_FILE', DEF_OUTPUT_FILE))

            self.logfile = os.path.join(
                self.logdir, _conf.get('GLOW_LOGFILE', DEF_LOGFILE))

            self.checkpoint_file = os.path.join(
                self.logdir, _conf.get('GLOW_CHECKPOINT_FILE', DEF_CHECKPOINT_FILE))

            if self.period not in ISO_PERIODS:
                raise ConfigError(f'Conf file err: Invalid period'
                                  f' {self.period} in {env_file}')

            if self.wait_minutes < MIN_WAIT_MINUTES:
                raise ConfigError(f'Conf file error: WAIT_MINUTES '
                                  f'must not be less than {MIN_WAIT_MINUTES}')

            if self.wait_minutes > MAX_WAIT_MINUTES:
                raise ConfigError(f'Conf file error: WAIT_MINUTES '
                                  f'must not be greater than {MAX_WAIT_MINUTES}')

    def write_to_log(self) -> None:
        """Write primary config items to the log."""
        logging.info('')
        logging.info('')
        logging.info('STARTED: running %s with config file %s',
                     os.path.basename(sys.argv[0]), self.env_file)

        logging.info('log level = %s', self.loglevel)

        logging.info('Aggregation Period is %s, no_incomplete_periods = %s',
                     self.period, self.incomplete_periods)

        logging.info("Wait minutes is %s", self.wait_minutes)


class Checkpoint:
    """Maintains the checkpoint (high water mark) in the checkpoint file defined in the config.

    A checkpoint is represented in the checkpoint file as: <ISO period>.<checkpoint datetime>
    """

    CheckpointTuple = Tuple[str, datetime]

    def __init__(self, path, period, default_checkpoint_dt):
        """
        Initialise the checkpoint object.

        If the checkpoint file 'path' exists, then checkpoint value and period are read from file.
        A checkpoint file is created if it does not exist, using 'period and 'def_start_dt'.

        :param path: checkpoint file path (directory must exist)
        :param period: glow aggregation period for the checkpoint
        :param default_checkpoint_dt: default datetime for a new checkpoint
        """
        self.path = path
        self.period = None
        self.last_dt = None

        # Get checkpoint from file if checkpoint file exists
        # If the checkpoint file doesn't exist, mcreate it and write a checkpoint

        if os.path.exists(self.path):
            # Get checkpoint from file if checkpoint file exists

            # make sure we don't clobber another filesystem object
            if not os.path.isfile(self.path):
                raise CheckpointError("Checkpoint file exists but is not a file")

            # Valid checkpoint file exists, get checkpoint and period from file
            try:
                self.file = open(self.path, 'r+', encoding="utf-8")

            except IOError as err:
                raise CheckpointError("Error opening checkpoint file") from err

            self.period, self.last_dt = self.get_from_file()

            if self.period != period:
                self.file.close()
                raise CheckpointError(f'Checkpoint file has period'
                                      f' {self.period} but config has {period}')

        else:
            # The checkpoint file doesn't exist

            logging.info('setting checkpoint to period %s value %s', period, default_checkpoint_dt)

            try:
                self.file = open(self.path, 'w+', encoding="utf-8")

            except IOError as err:
                raise CheckpointError("Error opening checkpoint file") from err

            self.update_checkpoint(period, default_checkpoint_dt)

    def get_from_file(self) -> CheckpointTuple:
        """Retrieve checkpoint from the checkpoint file, returning a CheckpointTuple."""
        try:
            self.file.seek(0)

            tokens = self.file.readline().strip().split(',')

            if tokens[0] not in ISO_PERIODS:
                raise CheckpointError("read_checkpoint: checkpoint does not start with 'period'")

            period = tokens[0]
            checkpoint_dt = datetime.fromisoformat(tokens[1])

        except ValueError as err:
            self.file.close()
            raise CheckpointError('read_checkpoint: invalid ISO format string') from err

        else:

            # if more data in file, raise error.
            if self.file.readline().strip():
                raise CheckpointError("checkpoint file has too much data")

            return period, checkpoint_dt

    def update_checkpoint(self, period, new_dt) -> None:
        """
        Update the checkpoint object and checkpoint file with the supplied period and datetime.

        :param period: glow aggregation period for the checkpoint
        :param new_dt: datetime to use for the checkpoint
        """
        try:

            self.file.seek(0)
            self.file.truncate()
            self.file.write(period + ',' + new_dt.isoformat() + os.linesep)
            self.file.flush()
            self.last_dt = new_dt
            self.period = period

        except OSError as err:
            self.file.close()
            raise CheckpointError('Error updating checkpoint') from err

    def close(self) -> None:
        """Close checkpoint file."""
        try:
            self.file.close()
            logging.warning('Checkpoint file %s closed...', self.path)
        except OSError as err:
            raise CheckpointError('Error closing checkpoint file') from err


class VirtualEntity:
    """Represents a Glow 'virtual entity'."""

    # pylint: disable=too-few-public-methods

    def __init__(self, payload):
        """Initialise object from the response to a glow 'get virtual entities ' API call."""
        self.application_id = payload.get("applicationId")
        self.postalcode = payload.get("postalCode", '--- ---')
        self.name = payload.get("name")
        self.owner_id = payload.get("ownerId")
        self.type_id = payload.get("veTypeId")
        self.ve_id = payload.get("veId")
        self.active = payload.get("active")
        # we won't bother getting resource summaries here..

    def __repr__(self):
        """Print a representation of a VirtualEntity object."""
        return f'VirtualEntity("{self.type_id}", "{self.ve_id}","{self.active}", ' \
               f'"{self.postalcode}","{self.name}", "{self.owner_id}"'


class Resource:
    """
    Represents a Glow 'resource'.

    Methods:
        supply_type
        res_type
        _repr_
    """

    def __init__(self, payload):
        """
        Initialise resource object from the response to a glow 'get resources' API call.

        Note that the structure of the resource classifier is <supply_type>.<resource_type>:
            'electricity.consumption"
            'electricity.consumption.cost'
            'gas.consumption'
            'gas.consumption.cost'
        """
        self.res_id = payload["resourceId"]
        self.type_id = payload["resourceTypeId"]
        self.name = payload["name"]
        self.classifier = payload["classifier"]
        self.description = payload["description"]
        self.base_unit = payload["baseUnit"]

        self.temp = 1

    def supply_type(self):
        """Return the supply type from resource classifier."""
        print(f'supply_type: {self.classifier.partition(".")[0]}')
        return self.classifier.partition('.')[0]

    def res_type(self):
        """Return the resource type from resource classifier."""
        print(f'res_type:  {self.classifier} --> {self.classifier.rpartition(".")[2]}')
        return self.classifier.rpartition('.')[2]

    def __repr__(self):
        """Print a representation of a Resource object."""
        return f'Resource("{self.res_id}". "{self.type_id}", "{self.name}", ' \
               f'"{self.classifier}", "{self.base_unit}", ' \
               f'"{self.description}"'


class GlowClient:
    """
    Wrapper for the Glow API.

    Includes methods to authenticate the connection and retrieve
    virtual identities, resources and readings. Also includes some utility methods

    API Methods:
        authenticate
        get_virtual_identities
        get_resources
        get_readings

    Other Methods
        check_valid_period
        token_check_valid
    """

    def __repr__(self):
        """Print a representation of a Resource object."""
        return 'token'

    def __init__(self, username: str, password: str) -> None:
        """Create a GlowClient object, and authenticates with Glow retrieving a token.

        :param username: Glow username
        :param password: Glow password
        """
        self.username = username
        self.password = password
        self.application = "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
        self.url = "https://api.glowmarkt.com/api/v0-1/"
        self.token = None
        self.token_expires = None
        self.session = requests.Session()
        self.authenticate()

    def authenticate(self) -> None:
        """Authenticate with Glow, retrieving a token and its expiry time."""
        _responses = {200: 'OK', 400: 'Bad Request',
                      401: 'Unauthorised', 500: 'Internal Server Error'}

        headers = {"Content-Type": "application/json", "applicationId": self.application}
        data = {"username": self.username, "password": self.password}
        url = self.url + "auth"
        resp = self.session.post(url, headers=headers, data=json.dumps(data))

        if resp.status_code != 200:
            raise GlowAPIError(
                f'Glow auth. failed: status {resp.status_code} {_responses[resp.status_code]}')

        resp = resp.json()

        if not resp["valid"]:
            raise GlowAPIError("Expected an authentication token")

        if "token" not in resp:
            raise GlowAPIError("Expected an authentication token")

        if "exp" not in resp:
            raise GlowAPIError("Expected token expiry time")

        self.token = resp["token"]
        self.token_expires = resp["exp"]

        logging.info('authenticated - token expires %s',
                     datetime.fromtimestamp(self.token_expires, pytz.UTC).isoformat())

    def get_virtual_entities(self):
        """Get list of virtual entities from Glow.

        :return: list of virtual entities
        """
        _responses = {200: 'OK', 500: 'Internal Server Error'}
        headers = {"Content-Type": "application/json",
                   "applicationId": self.application,
                   "token": self.token}

        url = self.url + "virtualentity"
        resp = self.session.get(url, headers=headers, timeout=10)

        if resp.status_code != 200:
            raise GlowAPIError(f'Could not retrieve virtual entities: '
                               f'status {resp.status_code} {_responses[resp.status_code]}')

        resp = resp.json()

        ve_list = []

        for elt in resp:
            this_ve = VirtualEntity(elt)
            ve_list.append(this_ve)

        return ve_list

    def get_resources(self, virtual_entity):
        """Get list of resources from Glow for the specified virtual entity.

        :param virtual_entity: the entity for which resources are required
        :return: list of Glow resources
        """
        _responses = {200: 'OK', 404: 'Not found', 500: 'Internal Server Error'}

        headers = {"Content-Type": "application/json",
                   "applicationId": self.application,
                   "token": self.token}

        url = self.url + "virtualentity/" + virtual_entity.ve_id + "/resources"

        resp = self.session.get(url, headers=headers, timeout=10)

        if resp.status_code != 200:
            raise GlowAPIError(f'Could not retrieve resources: '
                               f'status {resp.status_code} {_responses[resp.status_code]}')

        resp = resp.json()

        resources = []

        for elt in resp["resources"]:
            res = Resource(elt)
            resources.append(res)

        return resources

    def get_readings(self, resource, t_from, t_to, period, func="sum"):
        """Get the glow readings from 't_from' to t_to' for the current resource.

        :param resource: the resource for which readings are being requested
        :param t_from: the start datetime for the data (timezone aware with tz = UTC)
        :param t_to: the end datetime for the data (timezone aware with tz = UTC
        :param period: the aggregation period to be used for the data
        :param func: the summation function to use for the data (only "sum" is valid)
        :return:glow response in json format
        :rtype:
        """
        # pylint: disable=too-many-arguments

        _responses = {200: 'OK', 401: 'Unauthorised', 500: 'Internal Server Error'}

        headers = {
            "Content-Type": "application/json",
            "applicationId": self.application,
            "token": self.token}

        if period not in ISO_PERIODS:
            raise ConfigError("getreadings: 'period' is invalid")

        params = {
            "from": t_from.replace(tzinfo=None).isoformat(),
            "to": t_to.replace(tzinfo=None).isoformat(),
            "period": period,
            "offset": 0,
            "function": func,
            "nulls": 1,
        }
        url = self.url + "resource/" + resource.res_id + "/readings"

        resp = self.session.get(url, headers=headers, params=params, timeout=10)

        if resp.status_code != 200:
            raise GlowAPIError(f'Could not retrieve readings: {resp.status_code} '
                               f'{_responses[resp.status_code]}')

        resp = resp.json()

        return resp["data"]

    def token_check_valid(self) -> bool:
        """Return True if the current glow token is at least 1 day before expiry."""
        now = datetime.now().astimezone(pytz.UTC)
        expiry_time = datetime.fromtimestamp(self.token_expires, pytz.UTC)
        if expiry_time > (now + relativedelta(days=1)):
            return True     # token OK

        return False        # one day or less remaning on current token

    @staticmethod
    def check_valid_period(res, period):
        """Return True if the aggregation period 'period' is valid for this resource.

        :param res: a glow resource
        :param period: an aggregation period
        """
        # only electricity.consumption is valid for PT1M
        if period == "PT1M":
            if res.classifier not in ['electricity.consumption']:
                return False

        return True

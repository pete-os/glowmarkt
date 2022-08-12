import json
import os
import sys
from datetime import datetime

import pytz
import requests
from dateutil.relativedelta import relativedelta, MO

ISO_PERIODS = ['PT1M', 'PT30M', 'PT1H', 'P1D', 'P1W', 'P1M']


def start_of_day(dt: datetime):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def round_down(dt: datetime, period):
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


class Checkpoint:
    def __init__(self, path, period, def_start_dt):

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
        return f'VirtualEntity("{self.type_id}", "{self.id}","{self.active}", ' \
               f'"{self.postalcode}","{self.name}", "{self.owner_id}"'


class Resource:
    def __init__(self, payload):
        self.id = payload["resourceId"]
        self.type_id = payload["resourceTypeId"]
        self.name = payload["name"]
        self.classifier = payload["classifier"]
        self.description = payload["description"]
        self.base_unit = payload["baseUnit"]

        self.temp = 1

    def __repr__(self):
        return f'Resource("{self.id}". "{self.type_id}", "{self.name}", "{self.classifier}", "{self.base_unit}", ' \
               f'"{self.description}"'


class GlowClient:

    def __repr__(self):
        return 'token'

    def __init__(self, username, password):
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

        headers = {"Content-Type": "application/json", "applicationId": self.application}
        data = {"username": self.username, "password": self.password}

        url = self.url + "auth"

        resp = self.session.post(url, headers=headers, data=json.dumps(data))

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

        print("authenticated - token expires ", datetime.fromtimestamp(self.token_expires, pytz.UTC).isoformat())

        return True

    def get_virtual_entities(self):

        headers = {"Content-Type": "application/json", "applicationId": self.application, "token": self.token}

        url = self.url + "virtualentity"

        resp = self.session.get(url, headers=headers, timeout=10)

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        ves = []

        for elt in resp:
            ve = VirtualEntity(elt)
            ves.append(ve)

        return ves

    def get_resources(self, virtual_entity):

        headers = {"Content-Type": "application/json", "applicationId": self.application, "token": self.token}

        url = self.url + "virtualentity/" + virtual_entity.id + "/resources"

        resp = self.session.get(url, headers=headers, timeout=10)

        if resp.status_code != 200:
            raise RuntimeError("Request failed")
        resp = resp.json()

        resources = []

        for elt in resp["resources"]:
            res = Resource(elt)
            resources.append(res)

        return resources

    def get_readings(self, resource, t_from, t_to, period, func="sum"):

        headers = {"Content-Type": "application/json", "applicationId": self.application, "token": self.token}

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

        if resp.status_code != 200:
            raise RuntimeError("Request failed")

        resp = resp.json()

        return resp["data"]

    def token_check_valid(self):
        now = datetime.now().astimezone(pytz.UTC)
        expiry_time = datetime.fromtimestamp(self.token_expires, pytz.UTC)
        if expiry_time > (now + relativedelta(days=1)):
            print('time remaining on token is: ', str(expiry_time - now))
            return True
        else:
            print('get new token - one hour or less remaining on current token')
            return False

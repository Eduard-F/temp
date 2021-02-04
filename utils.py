# utils.py

"""Utility methods for the project"""

import datetime
import requests
import iso8601
import os
import time
import datetime
from dateutil.parser import parse


def format_time(raw_time_text):
    dt = iso8601.parse_date(raw_time_text) + datetime.timedelta(0, 7200)
    return dt.strftime('%a %d %b %Y %H:%M')


def to_dashboard_name(input_string):
    input_string = input_string.replace('mobiloan_instance', 'Mobiloan Instance')
    return input_string.replace('custom_user', 'user').title()


def snake_to_title(input_string):
    output_string = ' '.join([word.title() for word in input_string.split('_')])
    return output_string


def is_workday():
    today = datetime.datetime.today()
    try:
        # Get public holidays and check
        response = requests.get(
            url='http://www.kayaposoft.com/enrico/json/v1.0/?action=getPublicHolidaysForYear&year=%s&country=zaf' % today.year)
        holidays = response.json()
        for holiday in holidays:
            if holiday['date']['year'] == today.year \
                    and holiday['date']['month'] == today.month \
                    and holiday['date']['day'] == today.day:
                return False

        # Check for a sunday
        if today.weekday() == 6:
            return False

        return True
    except Exception as e:
        # If any exceptions, return True so other programs can run anyways
        return True


def set_permissions(path):
    """                                                                                             .---._____,
    Sets default permissions to files (apache2 and celery messes it up sometimes)                .-='-E-'==-.
    Copy pasta the code from WinSCP program                                                     (O_o_o_o_o_O)
    note - sometimes the exception gets raised, maybe because file is in use at that time ?
    Args:
        path: file path for document/folder
    """
    try:
        os.chmod(path, 0o774)
    except Exception:
        raise Exception('Unable to set permissions')


def obj_to_dict(obj):
    """                                                      .---._____,
    Converts objects to dictionaries                      .-='-E-'==-.
    It can handle nested objects/dictionaries as well    (O_o_o_o_o_O)

    Args:
        obj: can be a object or dictionary with nested objects of whatever kind
    """
    result = {}
    if isinstance(obj, dict):
        for key, val in obj.items():
            if key.startswith("_"):
                continue
            element = []
            if isinstance(val, list):
                for item in val:
                    element.append(obj_to_dict(item))
            else:
                element = obj_to_dict(val)
            result[key] = element
    else:
        if not hasattr(obj, "__dict__"):
            return obj
        for key, val in list(obj.__dict__.items()):
            if key.startswith("_"):
                continue
            element = []
            if isinstance(val, list):
                for item in val:
                    element.append(obj_to_dict(item))
            else:
                element = obj_to_dict(val)
            result[key] = element
    return result


def json_serial(obj):
    """                                                                     .---._____,
    JSON serializer for objects not serializable by default json code    .-='-E-'==-.
    Currently just datetime objects                                     (O_o_o_o_o_O)
     Args:
         obj: Django model
    """
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:           .---._____,
    items added                                                  .-='-E-'==-.
    items removed                                               (O_o_o_o_o_O)
    keys same in both but changed values
    keys same in both and unchanged values
    """

    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)

    def added(self):
        return self.set_current - self.intersect

    def removed(self):
        return self.set_past - self.intersect

    def changed(self):
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])

    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])


def split_into_list(var, separator=","):
    """                                                ___
    Converts and splits variables to a list      _____/_O_\_____
                                               (==(/________\)==)
                                                \==\/      \/==/
    Args:
        var: variable to be converted to a list
        separator: separator used to split the values

    """
    if var:
        if isinstance(var, str):
            return var.split(separator)
        elif isinstance(var, list):
            temp = []
            for i in var:
                temp += (i.split(separator))
            return temp
        elif isinstance(var, tuple):
            temp = []
            for i in var:
                temp += (i.split(separator))
            return temp
        else:
            raise Exception("variable must be a string, unicode, list or tuple")
    else:
        return []


def get_files_older(file_dir, date_str):
    file_array = []
    last_time = 0.0
    try:
        starting_date = parse(date_str)
        now = (time.mktime(starting_date.timetuple()) + starting_date.microsecond / 1E6)

        for f in os.listdir(file_dir):
            path = os.path.join(file_dir, f)
            if os.path.isfile(path):
                stat = os.stat(path)
                if truncate(stat.st_ctime, 2) > truncate(now, 2):
                    file_array.append(path)
                    if stat.st_ctime > last_time or not last_time:
                        last_time = stat.st_ctime
        if last_time == 0.0:
            last_date = ''
        else:
            last_date = datetime.datetime.fromtimestamp(last_time)
    except Exception as e:
        return {"last_date": '', "files": []}
    return {"last_date": last_date, "files": file_array}


def delete_old_files(file_dir, date_time):
    """
    Delete everything older than date_str
    Args:
        file_dir:
        date_str:
    """
    try:
        starting_date = date_time
        now = time.mktime(starting_date.timetuple()) + starting_date.microsecond / 1E6
        temp = os.listdir(file_dir)

        for f in os.listdir(file_dir):
            path = os.path.join(file_dir, f)
            if os.path.isfile(path):
                stat = os.stat(path)
                if stat.st_ctime < now:
                    print("delete: ", path)
                    # os.remove(path) # uncomment when you are sure :)
                else:
                    print('do not delete')

        for root, dirs, files in os.walk(file_dir, topdown=False):
            for file_ in files:
                full_path = os.path.join(root, file_)
                stat = os.stat(full_path)

                if stat.st_mtime <= now:
                    os.remove(full_path)

            if not os.listdir(root):
                os.rmdir(root)
    except Exception as e:
        print((str(e)))


def delete_file(file_path):
    """
    Delete a specific file
    Args:
        file_path:
    """
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
            return {'success': True}
        else:
            return {'success': False, 'error': "File doesn't exist"}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def truncate(f, n):
    """Truncates/pads a float f to n decimal places without rounding"""
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return '.'.join([i, (d+'0'*n)[:n]])

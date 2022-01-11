from airflow.models import Variable
from jinja2 import Template
import yaml
from airflow.hooks.base import BaseHook
from json import loads
from datetime import datetime

DEFAULT_NOT_SPECIFIED = 'DEFAULT_NOT_SPECIFIED'


def get_current_date():
    today_date = datetime.today().strftime('%Y-%m-%d')
    return today_date


class Parser:
    def render(self, filePath, extra_vars={}):
        try:
            with open(filePath, "r") as f:
                template = Template(f.read()).render({
                    'var': self.var,
                    'conn': self.conn,
                    'extra_vars': extra_vars,
                    'json_loads': loads,
                    'get_current_date': get_current_date

                })
                print(template)
                return yaml.safe_load(template)
        except Exception as err:
            raise "Error loading YAML File" from err

    def var(self, var_name, default=DEFAULT_NOT_SPECIFIED):
        # Using this hack for allowing specifyin None as default
        # and still break if no default is specified and the var
        # is not defined at airflow
        if default == DEFAULT_NOT_SPECIFIED:
            return Variable.get(var_name)
        else:
            return Variable.get(var_name, default)

    def conn(self, conn_name):
        return BaseHook.get_connection(conn_name)



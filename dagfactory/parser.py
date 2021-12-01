from airflow.models import Variable
from jinja2 import Template
import yaml
from airflow.hooks.base import BaseHook
from json import loads
import jinja2


class Parser:
    def render(self, filePath, extra_vars={}):
        try:
            with open(filePath, "r") as f:
                template = Template(f.read()).render({
                    'var': self.var,
                    'conn': self.conn,
                    'extra_vars': extra_vars,
                    'json_loads': loads
                })
                print(template)
                return yaml.safe_load(template)
        except Exception as err:
            raise "Error loading YAML File" from err

    def var(self, var_name):
        return Variable.get(var_name)

    def conn(self, conn_name):
        return BaseHook.get_connection(conn_name)



from airflow.models import Variable
from jinja2 import Template
import yaml, json

class Parser:
    def render(self, filePath):
        try:
            with open(filePath, "r") as f:
                template = Template(f.read()).render({
                    'env': self.env
                })
                return yaml.load(
                    template,
                    Loader=yaml.FullLoader
                )
        except Exception as err:
            raise Exception("Error loading YAML File") from err

    def env(self, varName):
        return Variable.get(varName)
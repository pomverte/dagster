from dagster import Definitions
from pydantic import BaseModel

from dagster_components import ComponentLoadContext, component
from dagster_components.lib.custom_component import CustomComponent


class ExampleCustomComponentSchema(BaseModel): ...


@component(name="example_custom_component")
class ExampleCustomComponent(CustomComponent):
    params_schema = ExampleCustomComponentSchema

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        return Definitions()

    @classmethod
    def load(cls, context: ComponentLoadContext) -> ...:
        loaded_params = context.load_params(cls.params_schema)
        assert loaded_params  # silence linter complaints
        return cls()

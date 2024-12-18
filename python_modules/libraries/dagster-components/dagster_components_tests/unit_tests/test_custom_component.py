import inspect
import textwrap

from dagster_components.lib.custom_component import (
    custom_component_template,
    get_custom_component_template,
)


def test_custom_component_template() -> None:
    raw_source = inspect.getsource(custom_component_template)
    without_decl = raw_source.split("\n", 1)[1]
    dedented = textwrap.dedent(without_decl)

    source = textwrap.dedent(raw_source).split("\n", 1)[1]
    source_from_fn = get_custom_component_template()
    import code

    code.interact(local=locals())

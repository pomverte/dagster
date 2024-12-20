import functools
import json
import os
from typing import AbstractSet, Any, Callable, Mapping, Optional, Sequence, Type, TypeVar, Union

import dagster._check as check
from dagster._core.definitions.declarative_automation.automation_condition import (
    AutomationCondition,
)
from dagster._record import record
from jinja2.nativetypes import NativeTemplate
from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo

T = TypeVar("T")

REF_BASE = "#/$defs/"
REF_TEMPLATE = f"{REF_BASE}{{model}}"

CONTEXT_KEY = "required_rendering_scope"


def automation_condition_scope() -> Mapping[str, Any]:
    return {
        "eager": AutomationCondition.eager,
        "on_cron": AutomationCondition.on_cron,
    }


def RenderingScope(field: Optional[FieldInfo] = None, *, required_scope: AbstractSet[str]) -> Any:
    """Defines a Pydantic Field that requires a specific scope to be available before rendering.

    Examples:
    ```python
    class Schema(BaseModel):
        a: str = RenderingScope(required_scope={"foo", "bar"})
        b: Optional[int] = RenderingScope(Field(default=None), required_scope={"baz"})
    ```
    """
    return FieldInfo.merge_field_infos(
        field or Field(), Field(json_schema_extra={CONTEXT_KEY: json.dumps(list(required_scope))})
    )


def get_required_rendering_context(subschema: Mapping[str, Any]) -> Optional[AbstractSet[str]]:
    raw = check.opt_inst(subschema.get(CONTEXT_KEY), str)
    return set(json.loads(raw)) if raw else None


def _env(key: str) -> Optional[str]:
    return os.environ.get(key)


ShouldRenderFn = Callable[[Sequence[Union[str, int]]], bool]


@record
class TemplatedValueResolver:
    context: Mapping[str, Any]

    @staticmethod
    def default() -> "TemplatedValueResolver":
        return TemplatedValueResolver(
            context={"env": _env, "automation_condition": automation_condition_scope()}
        )

    def with_context(self, **additional_context) -> "TemplatedValueResolver":
        return TemplatedValueResolver(context={**self.context, **additional_context})

    def _resolve_value(self, val: Any) -> Any:
        return NativeTemplate(val).render(**self.context) if isinstance(val, str) else val

    def _resolve(
        self,
        val: Any,
        valpath: Optional[Sequence[Union[str, int]]],
        should_render: Callable[[Sequence[Union[str, int]]], bool],
    ) -> Any:
        if valpath is not None and not should_render(valpath):
            return val
        elif isinstance(val, dict):
            return {
                k: self._resolve(v, [*valpath, k] if valpath is not None else None, should_render)
                for k, v in val.items()
            }
        elif isinstance(val, list):
            return [
                self._resolve(v, [*valpath, i] if valpath is not None else None, should_render)
                for i, v in enumerate(val)
            ]
        else:
            return self._resolve_value(val)

    def resolve(self, val: Any) -> Any:
        """Given a raw value, preprocesses it by rendering any templated values."""
        return self._resolve(val, None, lambda _: True)

    def resolve_params(self, val: T, target_type: Type) -> T:
        """Given a raw value, preprocesses it by rendering any templated values that are not marked as deferred in the target_type's json schema."""
        json_schema = (
            target_type.model_json_schema() if issubclass(target_type, BaseModel) else None
        )
        if json_schema is None:
            should_render = lambda _: True
        else:
            should_render = functools.partial(
                has_rendering_scope, json_schema=json_schema, subschema=json_schema
            )
        return self._resolve(val, [], should_render=should_render)


def has_rendering_scope(
    valpath: Sequence[Union[str, int]], json_schema: Mapping[str, Any], subschema: Mapping[str, Any]
) -> bool:
    # List[ComplexType] (e.g.) will contain a reference to the complex type schema in the
    # top-level $defs, so we dereference it here.
    if "$ref" in subschema:
        subschema = json_schema["$defs"].get(subschema["$ref"][len(REF_BASE) :])

    if get_required_rendering_context(subschema) is not None:
        return False
    elif len(valpath) == 0:
        return True

    # Optional[ComplexType] (e.g.) will contain multiple schemas in the "anyOf" field
    if "anyOf" in subschema:
        return all(has_rendering_scope(valpath, json_schema, inner) for inner in subschema["anyOf"])

    el = valpath[0]
    if isinstance(el, str):
        # valpath: ['field']
        # field: X
        inner = subschema.get("properties", {}).get(el)
    elif isinstance(el, int):
        # valpath: ['field', 0]
        # field: List[X]
        inner = subschema.get("items")
    else:
        check.failed(f"Unexpected valpath element: {el}")

    # the path wasn't valid, or unspecified
    if not inner:
        return subschema.get("additionalProperties", True)

    _, *rest = valpath
    return has_rendering_scope(rest, json_schema, inner)

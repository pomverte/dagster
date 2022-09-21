from abc import abstractmethod
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Dict,
    FrozenSet,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Type,
    Union,
    cast,
)

import dagster._check as check
from dagster._core.definitions.utils import validate_tags
from dagster._serdes.serdes import DefaultEnumSerializer, whitelist_for_serdes
from dagster._utils import merge_dicts

from .handle import ResolvedFromDynamicStepHandle, StepHandle, UnresolvedStepHandle
from .inputs import StepInput, UnresolvedCollectStepInput, UnresolvedMappedStepInput
from .objects import StepKeyOutputNamePair
from .outputs import StepOutput

if TYPE_CHECKING:
    from dagster._core.definitions.dependency import Node, NodeHandle
    from dagster._core.definitions.hook_definition import HookDefinition


class StepKindSerializer(DefaultEnumSerializer):
    @classmethod
    def value_from_storage_str(cls, storage_str: str, klass: Type) -> Enum:
        # old name for unresolved mapped
        if storage_str == "UNRESOLVED":
            value = "UNRESOLVED_MAPPED"
        else:
            value = storage_str
        return super().value_from_storage_str(value, klass)


@whitelist_for_serdes(serializer=StepKindSerializer)
class StepKind(Enum):
    COMPUTE = "COMPUTE"
    UNRESOLVED_MAPPED = "UNRESOLVED_MAPPED"
    UNRESOLVED_COLLECT = "UNRESOLVED_COLLECT"


def is_executable_step(step: Union["ExecutionStep", "UnresolvedMappedExecutionStep"]) -> bool:
    # This function is set up defensively to ensure new step types handled properly
    if isinstance(step, ExecutionStep):
        return True
    elif isinstance(step, UnresolvedMappedExecutionStep):
        return False
    else:
        check.failed(f"Unexpected execution step type {step}")


class IExecutionStep:
    @property
    @abstractmethod
    def handle(self) -> Union[StepHandle, UnresolvedStepHandle, ResolvedFromDynamicStepHandle]:
        pass

    @property
    @abstractmethod
    def key(self) -> str:
        pass

    @property
    @abstractmethod
    def solid_handle(self) -> "NodeHandle":
        pass

    @property
    @abstractmethod
    def kind(self) -> StepKind:
        pass

    @property
    @abstractmethod
    def tags(self) -> Optional[Dict[str, str]]:
        pass

    @property
    @abstractmethod
    def step_inputs(
        self,
    ) -> Sequence[Union[StepInput, UnresolvedCollectStepInput, UnresolvedMappedStepInput]]:
        pass

    @property
    @abstractmethod
    def step_outputs(self) -> Sequence[StepOutput]:
        pass

    @abstractmethod
    def step_input_named(
        self, name: str
    ) -> Union[StepInput, UnresolvedCollectStepInput, UnresolvedMappedStepInput]:
        pass

    @abstractmethod
    def step_output_named(self, name: str) -> StepOutput:
        pass


class ExecutionStep(
    NamedTuple(
        "_ExecutionStep",
        [
            ("handle", Union[StepHandle, ResolvedFromDynamicStepHandle]),
            ("pipeline_name", str),
            ("step_input_dict", Dict[str, StepInput]),
            ("step_output_dict", Dict[str, StepOutput]),
            ("tags", Dict[str, str]),
            ("logging_tags", Dict[str, str]),
            ("key", str),
        ],
    ),
    IExecutionStep,
):
    """
    A fully resolved step in the execution graph.
    """

    def __new__(
        cls,
        handle: Union[StepHandle, ResolvedFromDynamicStepHandle],
        pipeline_name: str,
        step_inputs: List[StepInput],
        step_outputs: List[StepOutput],
        tags: Optional[Dict[str, str]],
        logging_tags: Optional[Dict[str, str]] = None,
        key: Optional[str] = None,
    ):
        return super(ExecutionStep, cls).__new__(
            cls,
            handle=check.inst_param(handle, "handle", (StepHandle, ResolvedFromDynamicStepHandle)),
            pipeline_name=check.str_param(pipeline_name, "pipeline_name"),
            step_input_dict={
                si.name: si
                for si in check.list_param(step_inputs, "step_inputs", of_type=StepInput)
            },
            step_output_dict={
                so.name: so
                for so in check.list_param(step_outputs, "step_outputs", of_type=StepOutput)
            },
            tags=validate_tags(check.opt_dict_param(tags, "tags", key_type=str)),
            logging_tags=merge_dicts(
                {
                    "step_key": handle.to_key(),
                    "pipeline_name": pipeline_name,
                    "solid_name": handle.solid_handle.name,
                },
                check.opt_dict_param(logging_tags, "logging_tags"),
            ),
            # mypy can't tell that if default is set, this is guaranteed to be a str
            key=cast(str, check.opt_str_param(key, "key", default=handle.to_key())),
        )

    @property
    def solid_handle(self) -> "NodeHandle":
        return self.handle.solid_handle

    @property
    def solid_name(self) -> str:
        return self.solid_handle.name

    @property
    def kind(self) -> StepKind:
        return StepKind.COMPUTE

    @property
    def step_outputs(self) -> List[StepOutput]:
        return list(self.step_output_dict.values())

    @property
    def step_inputs(self) -> List[StepInput]:
        return list(self.step_input_dict.values())

    def has_step_output(self, name: str) -> bool:
        check.str_param(name, "name")
        return name in self.step_output_dict

    def step_output_named(self, name: str) -> StepOutput:
        check.str_param(name, "name")
        return self.step_output_dict[name]

    def has_step_input(self, name: str) -> bool:
        check.str_param(name, "name")
        return name in self.step_input_dict

    def step_input_named(self, name: str) -> StepInput:
        check.str_param(name, "name")
        return self.step_input_dict[name]

    def get_execution_dependency_keys(self) -> Set[str]:
        deps = set()
        for inp in self.step_inputs:
            deps.update(inp.dependency_keys)
        return deps

    def get_mapping_key(self):
        if isinstance(self.handle, ResolvedFromDynamicStepHandle):
            return self.handle.mapping_key

        return None




class UnresolvedMappedExecutionStep(
    NamedTuple(
        "_UnresolvedMappedExecutionStep",
        [
            ("handle", UnresolvedStepHandle),
            ("pipeline_name", str),
            ("step_input_dict", Dict[str, Union[StepInput, UnresolvedMappedStepInput]]),
            ("step_output_dict", Dict[str, StepOutput]),
            ("tags", Dict[str, str]),
        ],
    ),
    IExecutionStep,
):
    """
    A placeholder step that will become N ExecutionSteps once the upstream dynamic output resolves in to N mapping keys.
    """

    def __new__(
        cls,
        handle: UnresolvedStepHandle,
        pipeline_name: str,
        step_inputs: List[Union[StepInput, UnresolvedMappedStepInput]],
        step_outputs: List[StepOutput],
        tags: Optional[Dict[str, str]],
    ):
        return super(UnresolvedMappedExecutionStep, cls).__new__(
            cls,
            handle=check.inst_param(handle, "handle", UnresolvedStepHandle),
            pipeline_name=check.str_param(pipeline_name, "pipeline_name"),
            step_input_dict={
                si.name: si
                for si in check.list_param(
                    step_inputs, "step_inputs", of_type=(StepInput, UnresolvedMappedStepInput)
                )
            },
            step_output_dict={
                so.name: so
                for so in check.list_param(step_outputs, "step_outputs", of_type=StepOutput)
            },
            tags=check.opt_dict_param(tags, "tags", key_type=str),
        )

    @property
    def solid_handle(self) -> "NodeHandle":
        return self.handle.solid_handle

    @property
    def key(self) -> str:
        return self.handle.to_key()

    @property
    def kind(self) -> StepKind:
        return StepKind.UNRESOLVED_MAPPED

    @property
    def step_outputs(self) -> List[StepOutput]:
        return list(self.step_output_dict.values())

    @property
    def step_inputs(self) -> List[Union[StepInput, UnresolvedMappedStepInput]]:
        return list(self.step_input_dict.values())

    def step_input_named(self, name: str) -> Union[StepInput, UnresolvedMappedStepInput]:
        check.str_param(name, "name")
        return self.step_input_dict[name]

    def step_output_named(self, name: str) -> StepOutput:
        check.str_param(name, "name")
        return self.step_output_dict[name]

    def get_all_dependency_keys(self) -> Set[str]:
        deps = set()
        for inp in self.step_inputs:
            if isinstance(inp, StepInput):
                deps.update(
                    [handle.step_key for handle in inp.get_step_output_handle_dependencies()]
                )
            elif isinstance(inp, UnresolvedMappedStepInput):
                deps.update(
                    [
                        handle.step_key
                        for handle in inp.get_step_output_handle_deps_with_placeholders()
                    ]
                )
            else:
                check.failed(f"Unexpected step input type {inp}")

        return deps

    @property
    def resolved_step_key_output_name_mapping(self) -> Sequence[StepKeyOutputNamePair]:
        pairs = []
        for inp in self.step_inputs:
            if isinstance(inp, UnresolvedMappedStepInput):
                pairs.append(StepKeyOutputNamePair(step_key=inp.resolved_by_step_key, output_name=inp.resolved_by_output_name))

        return pairs

    @property
    def resolved_by_step_key(self) -> str:
        # this function will be removed in moving to supporting being downstream of multiple dynamic outputs
        keys = self.resolved_by_step_keys
        # check.invariant(len(keys) == 1, "Unresolved step expects one and only one dynamic step key")
        return list(keys)

    @property
    def resolved_by_output_name(self) -> str:
        # this function will be removed in moving to supporting being downstream of multiple dynamic outputs
        keys = []
        for inp in self.step_inputs:
            if isinstance(inp, UnresolvedMappedStepInput):
                keys.append(inp.resolved_by_output_name)

        # check.invariant(
        #     len(keys) == 1, "Unresolved step expects one and only one dynamic output name"
        # )

        return keys

    @property
    def resolved_by_step_keys(self) -> Sequence[str]:
        keys = []
        for inp in self.step_inputs:
            if isinstance(inp, UnresolvedMappedStepInput):
                keys.append(inp.resolved_by_step_key)

        return keys

    def resolve(self, mappings: Dict[str, Dict[str, List[str]]]) -> List[ExecutionStep]:
        check.invariant(
            all(key in mappings for key in self.resolved_by_step_keys),
            "resolving with mappings that do not contain all required step keys",
        )
        execution_steps = []
        step_key_output_pairs = self.resolved_step_key_output_name_mapping

        # zip the dynamic outputs into groups (this can certainly be optimized)

        # ensure all upstream ops returned the same number of outputs
        mappings_lists = [mappings[p.step_key][p.output_name] for p in step_key_output_pairs]
        if not all(len(mappings_lists[0])== len(i) for i in mappings_lists):
            # replace with real exception
            raise Exception("All upstream ops must return an equal number of outputs to use zip")

        output_groups: List[Dict[str, Dict[str, str]]] = []
        for i in range(len(mappings_lists[0])):
            group = {}
            for p in step_key_output_pairs:
                step_key_group = group.get(p.step_key, {})
                step_key_group[p.output_name] = mappings[p.step_key][p.output_name][i]
                group[p.step_key] = step_key_group
            output_groups.append(group)


        for group in output_groups:
            group_mapped_key = ",".join([group[p.step_key][p.output_name] for p in step_key_output_pairs])
            resolved_inputs = [_resolved_input(inp, group[inp.resolved_by_step_key][inp.resolved_by_output_name]) for inp in self.step_inputs]
            execution_steps.append(
                    ExecutionStep(
                        handle=ResolvedFromDynamicStepHandle(self.handle.solid_handle, group_mapped_key),
                        pipeline_name=self.pipeline_name,
                        step_inputs=resolved_inputs,
                        step_outputs=self.step_outputs,
                        tags=self.tags,
                    )
                )

        return execution_steps


def _resolved_input(
    step_input: Union[StepInput, UnresolvedMappedStepInput],
    map_key: str,
):
    if isinstance(step_input, StepInput):
        return step_input

    return step_input.resolve(map_key)


class UnresolvedCollectExecutionStep(
    NamedTuple(
        "_UnresolvedCollectExecutionStep",
        [
            ("handle", StepHandle),
            ("pipeline_name", str),
            ("step_input_dict", Dict[str, Union[StepInput, UnresolvedCollectStepInput]]),
            ("step_output_dict", Dict[str, StepOutput]),
            ("tags", Dict[str, str]),
        ],
    ),
    IExecutionStep,
):
    """
    A placeholder step that will become 1 ExecutionStep that collects over a dynamic output or downstream from one once it resolves.
    """

    def __new__(
        cls,
        handle: StepHandle,
        pipeline_name: str,
        step_inputs: List[Union[StepInput, UnresolvedCollectStepInput]],
        step_outputs: List[StepOutput],
        tags: Optional[Dict[str, str]],
    ):
        return super(UnresolvedCollectExecutionStep, cls).__new__(
            cls,
            handle=check.inst_param(handle, "handle", StepHandle),
            pipeline_name=check.str_param(pipeline_name, "pipeline_name"),
            step_input_dict={
                si.name: si
                for si in check.list_param(
                    step_inputs, "step_inputs", of_type=(StepInput, UnresolvedCollectStepInput)
                )
            },
            step_output_dict={
                so.name: so
                for so in check.list_param(step_outputs, "step_outputs", of_type=StepOutput)
            },
            tags=check.opt_dict_param(tags, "tags", key_type=str),
        )

    @property
    def solid_handle(self) -> "NodeHandle":
        return self.handle.solid_handle

    @property
    def key(self) -> str:
        return self.handle.to_key()

    @property
    def kind(self) -> StepKind:
        return StepKind.UNRESOLVED_COLLECT

    @property
    def step_inputs(self) -> List[Union[StepInput, UnresolvedCollectStepInput]]:
        return list(self.step_input_dict.values())

    @property
    def step_outputs(self) -> List[StepOutput]:
        return list(self.step_output_dict.values())

    def step_input_named(self, name: str) -> Union[StepInput, UnresolvedCollectStepInput]:
        check.str_param(name, "name")
        return self.step_input_dict[name]

    def step_output_named(self, name: str) -> StepOutput:
        check.str_param(name, "name")
        return self.step_output_dict[name]

    def get_all_dependency_keys(self) -> Set[str]:
        deps = set()
        for inp in self.step_inputs:
            if isinstance(inp, StepInput):
                deps.update(
                    [handle.step_key for handle in inp.get_step_output_handle_dependencies()]
                )
            elif isinstance(inp, UnresolvedCollectStepInput):
                deps.update(
                    [
                        handle.step_key
                        for handle in inp.get_step_output_handle_deps_with_placeholders()
                    ]
                )
            else:
                check.failed(f"Unexpected step input type {inp}")

        return deps

    @property
    def resolved_by_step_keys(self) -> FrozenSet[str]:
        keys = []
        for inp in self.step_inputs:
            if isinstance(inp, UnresolvedCollectStepInput):
                if isinstance(inp.resolved_by_step_key, list):
                    keys.extend(inp.resolved_by_step_key)
                else:
                    keys.append(inp.resolved_by_step_key)

        return keys

    def resolve(self, mappings: Dict[str, Dict[str, List[str]]]) -> ExecutionStep:
        check.invariant(
            all(key in mappings for key in self.resolved_by_step_keys),
            "resolving with mappings that do not contain all required step keys",
        )

        resolved_inputs = []
        for inp in self.step_inputs:
            if isinstance(inp, StepInput):
                resolved_inputs.append(inp)
            else:
                # this is super hacky. what we really need is a way to pass the combined mapping keys from
                # the map step down to this resolve. Instead i'm just recreating them from the mappings
                # dict, which is prone to error

                zipped_mappings = []
                step_key_output_pairs = inp.resolved_step_key_output_name_mapping
                mappings_lists = [mappings[p.step_key][p.output_name] for p in step_key_output_pairs]

                for i in range(len(mappings_lists[0])):
                    zipped_mappings.append([])
                    for p in step_key_output_pairs:
                        zipped_mapping_i = zipped_mappings[i]
                        zipped_mapping_i.append(mappings[p.step_key][p.output_name][i])
                        zipped_mappings[i] = zipped_mapping_i

                zipped_mapping_keys = [",".join(zip_group) for zip_group in zipped_mappings]

                resolved_inputs.append(
                        inp.resolve(zipped_mapping_keys)
                    )

        return ExecutionStep(
            handle=self.handle,
            pipeline_name=self.pipeline_name,
            step_inputs=resolved_inputs,
            step_outputs=self.step_outputs,
            tags=self.tags,
        )

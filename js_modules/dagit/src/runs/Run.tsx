import * as React from "react";
import gql from "graphql-tag";
import styled from "styled-components/macro";
import { useMutation } from "react-apollo";
import ApolloClient from "apollo-client";

import LogsScrollingTable from "./LogsScrollingTable";
import { LogFilter, LogsProvider, GetDefaultLogFilter } from "./LogsProvider";
import { RunFragment, RunFragment_executionPlan } from "./types/RunFragment";
import { SplitPanelContainer, SplitPanelToggles } from "../SplitPanelContainer";
import { RunMetadataProvider, IStepState } from "../RunMetadataProvider";
import LogsToolbar from "./LogsToolbar";
import {
  handleExecutionResult,
  START_PIPELINE_EXECUTION_MUTATION,
  LAUNCH_PIPELINE_EXECUTION_MUTATION,
  getReexecutionVariables
} from "./RunUtils";
import { StartPipelineExecutionVariables } from "./types/StartPipelineExecution";
import { RunStatusToPageAttributes } from "./RunStatusToPageAttributes";
import InfoModal from "../InfoModal";
import PythonErrorInfo from "../PythonErrorInfo";
import { RunContext } from "./RunContext";

import {
  RunPipelineRunEventFragment_ExecutionStepFailureEvent,
  RunPipelineRunEventFragment
} from "./types/RunPipelineRunEventFragment";
import { GaantChart, GaantChartMode } from "../gaant/GaantChart";
import { RunActionButtons } from "./RunActionButtons";

interface IRunProps {
  client: ApolloClient<any>;
  run?: RunFragment;
}

interface IRunState {
  logsFilter: LogFilter;
  highlightedError?: { message: string; stack: string[] };
}

export class Run extends React.Component<IRunProps, IRunState> {
  static fragments = {
    RunFragment: gql`
      fragment RunFragment on PipelineRun {
        ...RunStatusPipelineRunFragment

        environmentConfigYaml
        runId
        canCancel
        status
        mode
        tags {
          key
          value
        }
        pipeline {
          __typename
          ... on PipelineReference {
            name
          }
          ... on Pipeline {
            solids {
              name
            }
          }
        }
        executionPlan {
          steps {
            key
            inputs {
              dependsOn {
                key
                outputs {
                  name
                  type {
                    name
                  }
                }
              }
            }
          }
          artifactsPersisted
          ...GaantChartExecutionPlanFragment
        }
        stepKeysToExecute
      }

      ${RunStatusToPageAttributes.fragments.RunStatusPipelineRunFragment}
      ${GaantChart.fragments.GaantChartExecutionPlanFragment}
    `,
    RunPipelineRunEventFragment: gql`
      fragment RunPipelineRunEventFragment on PipelineRunEvent {
        ... on MessageEvent {
          message
          timestamp
          level
          step {
            key
          }
        }

        ...LogsScrollingTableMessageFragment
        ...RunMetadataProviderMessageFragment
      }

      ${RunMetadataProvider.fragments.RunMetadataProviderMessageFragment}
      ${LogsScrollingTable.fragments.LogsScrollingTableMessageFragment}
      ${PythonErrorInfo.fragments.PythonErrorFragment}
    `
  };

  state: IRunState = {
    logsFilter: GetDefaultLogFilter(),
    highlightedError: undefined
  };

  onShowStateDetails = (
    stepKey: string,
    logs: RunPipelineRunEventFragment[]
  ) => {
    const errorNode = logs.find(
      node =>
        node.__typename === "ExecutionStepFailureEvent" &&
        node.step != null &&
        node.step.key === stepKey
    ) as RunPipelineRunEventFragment_ExecutionStepFailureEvent;

    if (errorNode) {
      this.setState({ highlightedError: errorNode.error });
    }
  };
  render() {
    const { client, run } = this.props;
    const { logsFilter, highlightedError } = this.state;

    return (
      <RunContext.Provider value={run}>
        {run && <RunStatusToPageAttributes run={run} />}
        {highlightedError && (
          <InfoModal
            onRequestClose={() =>
              this.setState({ highlightedError: undefined })
            }
          >
            <PythonErrorInfo error={highlightedError} />
          </InfoModal>
        )}

        <LogsProvider
          client={client}
          runId={run ? run.runId : ""}
          filter={logsFilter}
        >
          {({ filteredNodes, allNodes, loaded }) => (
            <RunWithData
              run={run}
              filteredNodes={filteredNodes}
              allNodes={allNodes}
              logsLoading={!loaded}
              logsFilter={logsFilter}
              onSetLogsFilter={logsFilter => this.setState({ logsFilter })}
              onShowStateDetails={this.onShowStateDetails}
              getExecutionVariables={getReexecutionVariables}
            />
          )}
        </LogsProvider>
      </RunContext.Provider>
    );
  }
}

interface RunWithDataProps {
  run?: RunFragment;
  allNodes: (RunPipelineRunEventFragment & { clientsideKey: string })[];
  filteredNodes: (RunPipelineRunEventFragment & { clientsideKey: string })[];
  logsFilter: LogFilter;
  logsLoading: boolean;
  onSetLogsFilter: (v: LogFilter) => void;
  onShowStateDetails: (
    stepKey: string,
    logs: RunPipelineRunEventFragment[]
  ) => void;
  getExecutionVariables: (input: {
    run: RunFragment;
    stepKey?: string;
    resumeRetry?: boolean;
  }) => StartPipelineExecutionVariables | undefined;
}

const RunWithData = ({
  run,
  allNodes,
  filteredNodes,
  logsFilter,
  logsLoading,
  onSetLogsFilter,
  getExecutionVariables
}: RunWithDataProps) => {
  const [startPipelineExecution] = useMutation(
    START_PIPELINE_EXECUTION_MUTATION
  );
  const [launchPipelineExecution] = useMutation(
    LAUNCH_PIPELINE_EXECUTION_MUTATION
  );
  const splitPanelContainer = React.createRef<SplitPanelContainer>();
  const selectedStep =
    logsFilter.values.find(v => v.token === "step")?.value || null;

  const executionPlan: RunFragment_executionPlan = run?.executionPlan || {
    __typename: "ExecutionPlan",
    steps: [],
    artifactsPersisted: false
  };
  const onExecute = async (stepKey?: string, resumeRetry?: boolean) => {
    if (!run || run.pipeline.__typename === "UnknownPipeline") return;
    const variables = getExecutionVariables({
      run,
      stepKey,
      resumeRetry
    });
    const result = await startPipelineExecution({ variables });
    handleExecutionResult(run.pipeline.name, result, {
      openInNewWindow: false
    });
  };
  const onLaunch = async (stepKey?: string, resumeRetry?: boolean) => {
    if (!run || run.pipeline.__typename === "UnknownPipeline") return;
    const variables = getExecutionVariables({
      run,
      stepKey,
      resumeRetry
    });
    const result = await launchPipelineExecution({ variables });
    handleExecutionResult(run.pipeline.name, result, {
      openInNewWindow: false
    });
  };

  return (
    <RunMetadataProvider logs={allNodes}>
      {metadata => (
        <SplitPanelContainer
          ref={splitPanelContainer}
          axis={"vertical"}
          identifier="run-gaant"
          firstInitialPercent={35}
          firstMinSize={40}
          first={
            <GaantChart
              options={{
                mode: GaantChartMode.WATERFALL_TIMED
              }}
              toolbarLeftActions={
                <SplitPanelToggles
                  axis={"vertical"}
                  container={splitPanelContainer}
                />
              }
              toolbarActions={
                <RunActionButtons
                  run={run}
                  artifactsPersisted={executionPlan.artifactsPersisted}
                  onExecute={onExecute}
                  onLaunch={onLaunch}
                  selectedStep={selectedStep}
                  selectedStepState={
                    (selectedStep && metadata.steps[selectedStep]?.state) ||
                    IStepState.PREPARING
                  }
                />
              }
              plan={executionPlan}
              metadata={metadata}
              selectedStep={selectedStep}
              onApplyStepFilter={stepKey =>
                onSetLogsFilter({
                  ...logsFilter,
                  values: [{ token: "step", value: stepKey }]
                })
              }
            />
          }
          second={
            <LogsContainer>
              <LogsToolbar
                onSetFilter={onSetLogsFilter}
                filter={logsFilter}
                steps={Object.keys(metadata.steps)}
                filterStep={selectedStep}
                filterStepState={
                  (selectedStep && metadata.steps[selectedStep]?.state) ||
                  IStepState.PREPARING
                }
              />
              <LogsScrollingTable
                nodes={filteredNodes}
                loading={logsLoading}
                filterKey={JSON.stringify(logsFilter)}
              />
            </LogsContainer>
          }
        />
      )}
    </RunMetadataProvider>
  );
};

const LogsContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #f1f6f9;
`;

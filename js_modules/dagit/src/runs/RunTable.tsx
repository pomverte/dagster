import * as React from "react";
import * as qs from "query-string";
import * as yaml from "yaml";
import gql from "graphql-tag";
import {
  Button,
  Colors,
  Icon,
  Menu,
  MenuItem,
  Popover,
  MenuDivider,
  Tooltip,
  NonIdealState,
  Tag
} from "@blueprintjs/core";
import {
  Details,
  Legend,
  LegendColumn,
  RowColumn,
  RowContainer
} from "../ListComponents";
import {
  RunStatus,
  titleForRun,
  REEXECUTE_MUTATION,
  handleStartExecutionResult,
  unixTimestampToString
} from "./RunUtils";
import { formatElapsedTime } from "../Util";
import { HighlightedCodeBlock } from "../HighlightedCodeBlock";
import { Link } from "react-router-dom";
import { RunTableRunFragment } from "./types/RunTableRunFragment";
import { showCustomAlert } from "../CustomAlertProvider";
import { useMutation } from "react-apollo";

interface RunTableProps {
  runs: RunTableRunFragment[];
}

export class RunTable extends React.Component<RunTableProps> {
  static fragments = {
    RunTableRunFragment: gql`
      fragment RunTableRunFragment on PipelineRun {
        runId
        status
        stepKeysToExecute
        mode
        environmentConfigYaml
        pipeline {
          ... on PipelineReference {
            name
          }
          ... on Pipeline {
            solids {
              name
            }
          }
        }
        stats {
          stepsSucceeded
          stepsFailed
          startTime
          endTime
          expectations
          materializations
        }
        tags {
          key
          value
        }
        executionPlan {
          steps {
            key
          }
        }
      }
    `
  };

  render() {
    if (this.props.runs.length === 0) {
      return (
        <div style={{ marginTop: 100 }}>
          <NonIdealState
            icon="history"
            title="Pipeline Runs"
            description="No runs to display. Use the Execute tab to start a pipeline."
          />
        </div>
      );
    }
    return (
      <div>
        <Legend>
          <LegendColumn style={{ maxWidth: 30 }}></LegendColumn>
          <LegendColumn style={{ flex: 2.4 }}>Run</LegendColumn>
          <LegendColumn>Pipeline</LegendColumn>
          <LegendColumn style={{ flex: 1 }}>Execution Params</LegendColumn>
          <LegendColumn style={{ flex: 1.8 }}>Timing</LegendColumn>
          <LegendColumn style={{ maxWidth: 50 }}></LegendColumn>
        </Legend>
        {this.props.runs.map(run => (
          <RunRow run={run} key={run.runId} />
        ))}
      </div>
    );
  }
}

const RunRow: React.FunctionComponent<{ run: RunTableRunFragment }> = ({
  run
}) => {
  return (
    <RowContainer key={run.runId} style={{ paddingRight: 3 }}>
      <RowColumn style={{ maxWidth: 30, paddingLeft: 0, textAlign: "center" }}>
        <RunStatus status={run.status} />
      </RowColumn>
      <RowColumn style={{ flex: 2.4 }}>
        <Link
          style={{ display: "block" }}
          to={`/p/${run.pipeline.name}/runs/${run.runId}`}
        >
          {titleForRun(run)}
        </Link>
        <Details>
          <Link
            to={`/p/${run.pipeline.name}/runs/${run.runId}?q=type:step_success`}
          >{`${run.stats.stepsSucceeded} steps succeeded, `}</Link>
          <Link
            to={`/p/${run.pipeline.name}/runs/${run.runId}?q=type:step_failure`}
          >
            {`${run.stats.stepsFailed} steps failed, `}{" "}
          </Link>
          <Link
            to={`/p/${run.pipeline.name}/runs/${run.runId}?q=type:materialization`}
          >{`${run.stats.materializations} materializations`}</Link>
          ,{" "}
          <Link
            to={`/p/${run.pipeline.name}/runs/${run.runId}?q=type:expectation`}
          >{`${run.stats.expectations} expectations passed`}</Link>
        </Details>
      </RowColumn>
      <RowColumn>
        {run.pipeline.__typename === "Pipeline" ? (
          <Link
            style={{ display: "block" }}
            to={`/p/${run.pipeline.name}/explore/`}
          >
            <Icon icon="diagram-tree" /> {run.pipeline.name}
          </Link>
        ) : (
          <>
            <Icon icon="diagram-tree" color={Colors.GRAY3} />
            &nbsp;
            <Tooltip content="This pipeline is not present in the currently loaded repository, so dagit can't browse the pipeline solids, but you can still view the logs.">
              {run.pipeline.name}
            </Tooltip>
          </>
        )}
      </RowColumn>
      <RowColumn>
        <div>
          <div>{`Mode: ${run.mode}`}</div>

          {run.stepKeysToExecute && (
            <div>
              {run.stepKeysToExecute.length === 1
                ? `Step: ${run.stepKeysToExecute.join("")}`
                : `${run.stepKeysToExecute.length} Steps`}
            </div>
          )}
          <div>
            {run.tags.map((t, idx) => (
              <Tag key={idx}>{`${t.key}=${t.value}`}</Tag>
            ))}
          </div>
        </div>
      </RowColumn>
      <RowColumn style={{ flex: 1.8, borderRight: 0 }}>
        {run.stats.startTime ? (
          <div style={{ marginBottom: 4 }}>
            <Icon icon="calendar" />{" "}
            {unixTimestampToString(run.stats.startTime)}
            <Icon
              icon="arrow-right"
              style={{ marginLeft: 10, marginRight: 10 }}
            />
            {unixTimestampToString(run.stats.endTime)}
          </div>
        ) : run.status === "FAILURE" ? (
          <div style={{ marginBottom: 4 }}> Failed to start</div>
        ) : (
          <div style={{ marginBottom: 4 }}>
            <Icon icon="calendar" /> Starting...
          </div>
        )}
        <RunTime startUnix={run.stats.startTime} endUnix={run.stats.endTime} />
      </RowColumn>
      <RowColumn style={{ maxWidth: 50 }}>
        <RunActionsMenu run={run} />
      </RowColumn>
    </RowContainer>
  );
};

const RunActionsMenu: React.FunctionComponent<{
  run: RunTableRunFragment;
}> = ({ run }) => {
  const [reexecute] = useMutation(REEXECUTE_MUTATION);

  return (
    <Popover
      content={
        <Menu>
          <MenuItem
            text="View Configuration..."
            icon="share"
            onClick={() =>
              showCustomAlert({
                title: "Config",
                body: (
                  <HighlightedCodeBlock
                    value={run.environmentConfigYaml}
                    languages={["yaml"]}
                  />
                )
              })
            }
          />
          <MenuDivider />
          <MenuItem
            text="Open in Execute Tab..."
            icon="edit"
            target="_blank"
            href={`/p/${run.pipeline.name}/execute/setup?${qs.stringify({
              mode: run.mode,
              config: run.environmentConfigYaml,
              solidSubset:
                run.pipeline.__typename === "Pipeline"
                  ? run.pipeline.solids.map(s => s.name)
                  : []
            })}`}
          />
          <MenuItem
            text="Re-execute"
            icon="repeat"
            onClick={async () => {
              const result = await reexecute({
                variables: {
                  executionParams: {
                    mode: run.mode,
                    environmentConfigData: yaml.parse(
                      run.environmentConfigYaml
                    ),
                    selector: {
                      name: run.pipeline.name,
                      solidSubset:
                        run.pipeline.__typename === "Pipeline"
                          ? run.pipeline.solids.map(s => s.name)
                          : []
                    }
                  }
                }
              });
              handleStartExecutionResult(run.pipeline.name, result, {
                openInNewWindow: false
              });
            }}
          />
        </Menu>
      }
      position={"bottom"}
    >
      <Button minimal={true} icon="more" />
    </Popover>
  );
};

class RunTime extends React.Component<{
  startUnix: number | null;
  endUnix: number | null;
}> {
  _interval?: NodeJS.Timer;
  _timeout?: NodeJS.Timer;

  componentDidMount() {
    if (this.props.endUnix) return;

    // align to the next second and then update every second so the elapsed
    // time "ticks" up. Our render method uses Date.now(), so all we need to
    // do is force another React render. We could clone the time into React
    // state but that is a bit messier.
    setTimeout(() => {
      this.forceUpdate();
      this._interval = setInterval(() => this.forceUpdate(), 1000);
    }, Date.now() % 1000);
  }

  componentWillUnmount() {
    if (this._timeout) clearInterval(this._timeout);
    if (this._interval) clearInterval(this._interval);
  }

  render() {
    const start = this.props.startUnix ? this.props.startUnix * 1000 : 0;
    const end = this.props.endUnix ? this.props.endUnix * 1000 : Date.now();

    return (
      <div>
        <Icon icon="time" /> {start ? formatElapsedTime(end - start) : ""}
      </div>
    );
  }
}

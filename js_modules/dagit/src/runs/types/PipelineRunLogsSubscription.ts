// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { LogLevel, ObjectStoreOperationType } from "./../../types/globalTypes";

// ====================================================
// GraphQL subscription operation: PipelineRunLogsSubscription
// ====================================================

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepSkippedEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepSkippedEvent {
  __typename: "ExecutionStepSkippedEvent" | "ExecutionStepStartEvent" | "ExecutionStepSuccessEvent" | "LogMessageEvent" | "PipelineFailureEvent" | "PipelineProcessExitedEvent" | "PipelineStartEvent" | "PipelineSuccessEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepSkippedEvent_step | null;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepFailureEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepFailureEvent_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepFailureEvent_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepFailureEvent_error_cause | null;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepFailureEvent {
  __typename: "ExecutionStepFailureEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepFailureEvent_step | null;
  error: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepFailureEvent_error;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineProcessStartedEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineProcessStartedEvent {
  __typename: "PipelineProcessStartedEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineProcessStartedEvent_step | null;
  processId: number;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineProcessStartEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineProcessStartEvent {
  __typename: "PipelineProcessStartEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineProcessStartEvent_step | null;
  pipelineName: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export type PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries = PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventPathMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventJsonMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventUrlMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventTextMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries_EventMarkdownMetadataEntry;

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization {
  __typename: "Materialization";
  label: string;
  description: string | null;
  metadataEntries: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization_metadataEntries[];
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent {
  __typename: "StepMaterializationEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_step | null;
  materialization: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent_materialization;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineInitFailureEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineInitFailureEvent_error {
  __typename: "PythonError";
  stack: string[];
  message: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineInitFailureEvent {
  __typename: "PipelineInitFailureEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineInitFailureEvent_step | null;
  error: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineInitFailureEvent_error;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export type PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries = PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventPathMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventJsonMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventUrlMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventTextMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries_EventMarkdownMetadataEntry;

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck {
  __typename: "TypeCheck";
  label: string;
  description: string | null;
  success: boolean;
  metadataEntries: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck_metadataEntries[];
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent {
  __typename: "ExecutionStepInputEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_step | null;
  inputName: string;
  typeCheck: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent_typeCheck;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export type PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries = PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventPathMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventJsonMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventUrlMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventTextMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries_EventMarkdownMetadataEntry;

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck {
  __typename: "TypeCheck";
  label: string;
  description: string | null;
  success: boolean;
  metadataEntries: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck_metadataEntries[];
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent {
  __typename: "ExecutionStepOutputEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_step | null;
  outputName: string;
  typeCheck: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent_typeCheck;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export type PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries = PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventPathMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventJsonMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventUrlMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventTextMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries_EventMarkdownMetadataEntry;

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult {
  __typename: "ExpectationResult";
  success: boolean;
  label: string;
  description: string | null;
  metadataEntries: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult_metadataEntries[];
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent {
  __typename: "StepExpectationResultEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_step | null;
  expectationResult: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent_expectationResult;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export type PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries = PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventPathMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventJsonMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventUrlMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventTextMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries_EventMarkdownMetadataEntry;

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult {
  __typename: "ObjectStoreOperationResult";
  op: ObjectStoreOperationType;
  metadataEntries: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult_metadataEntries[];
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent {
  __typename: "ObjectStoreOperationEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_step | null;
  operationResult: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent_operationResult;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_step {
  __typename: "ExecutionStep";
  key: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export type PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries = PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventPathMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventJsonMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventUrlMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventTextMetadataEntry | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries_EventMarkdownMetadataEntry;

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent {
  __typename: "EngineEvent";
  runId: string;
  message: string;
  timestamp: string;
  level: LogLevel;
  step: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_step | null;
  metadataEntries: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent_metadataEntries[];
}

export type PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages = PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepSkippedEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepFailureEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineProcessStartedEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineProcessStartEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepMaterializationEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_PipelineInitFailureEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepInputEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ExecutionStepOutputEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_StepExpectationResultEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_ObjectStoreOperationEvent | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages_EngineEvent;

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess {
  __typename: "PipelineRunLogsSubscriptionSuccess";
  messages: PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess_messages[];
}

export interface PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionFailure {
  __typename: "PipelineRunLogsSubscriptionFailure";
  missingRunId: string | null;
  message: string;
}

export type PipelineRunLogsSubscription_pipelineRunLogs = PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionSuccess | PipelineRunLogsSubscription_pipelineRunLogs_PipelineRunLogsSubscriptionFailure;

export interface PipelineRunLogsSubscription {
  pipelineRunLogs: PipelineRunLogsSubscription_pipelineRunLogs;
}

export interface PipelineRunLogsSubscriptionVariables {
  runId: string;
  after?: any | null;
}

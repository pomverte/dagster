import {
  ANTLRErrorListener,
  CharStreams,
  CommonTokenStream,
  RecognitionException,
  Recognizer,
} from 'antlr4ts';
import {FeatureFlag} from 'shared/app/FeatureFlags.oss';

import {AntlrAssetSelectionVisitor} from './AntlrAssetSelectionVisitor';
import {AssetGraphQueryItem} from '../asset-graph/useAssetGraphData';
import {AssetSelectionLexer} from './generated/AssetSelectionLexer';
import {AssetSelectionParser} from './generated/AssetSelectionParser';
import {featureEnabled} from '../app/Flags';
import {filterByQuery} from '../app/GraphQueryImpl';

class AntlrInputErrorListener implements ANTLRErrorListener<any> {
  syntaxError(
    recognizer: Recognizer<any, any>,
    offendingSymbol: any,
    line: number,
    charPositionInLine: number,
    msg: string,
    e: RecognitionException | undefined,
  ): void {
    if (offendingSymbol) {
      throw new Error(`Syntax error caused by "${offendingSymbol.text}": ${msg}`);
    }
    throw new Error(`Syntax error at char ${charPositionInLine}: ${msg}`);
  }
}

type AssetSelectionQueryResult = {
  all: AssetGraphQueryItem[];
  focus: AssetGraphQueryItem[];
};

export const parseAssetSelectionQuery = (
  all_assets: AssetGraphQueryItem[],
  query: string,
): AssetSelectionQueryResult => {
  const lexer = new AssetSelectionLexer(CharStreams.fromString(query));
  lexer.removeErrorListeners();
  lexer.addErrorListener(new AntlrInputErrorListener());

  const tokenStream = new CommonTokenStream(lexer);

  const parser = new AssetSelectionParser(tokenStream);
  parser.removeErrorListeners();
  parser.addErrorListener(new AntlrInputErrorListener());

  const tree = parser.start();

  const visitor = new AntlrAssetSelectionVisitor(all_assets);
  const all_selection = visitor.visit(tree);
  const focus_selection = visitor.focus_assets;

  return {
    all: Array.from(all_selection),
    focus: Array.from(focus_selection),
  };
};

export const filterAssetSelectionByQuery = (
  all_assets: AssetGraphQueryItem[],
  query: string,
): AssetSelectionQueryResult =>
  featureEnabled(FeatureFlag.flagAssetSelectionSyntax)
    ? parseAssetSelectionQuery(all_assets, query)
    : filterByQuery(all_assets, query);

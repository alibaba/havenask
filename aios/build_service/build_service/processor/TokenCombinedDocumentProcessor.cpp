#include <autil/StringTokenizer.h>
#include <indexlib/config/index_partition_schema.h>
#include "build_service/processor/TokenCombinedDocumentProcessor.h"
#include "build_service/analyzer/Token.h"

using namespace std;
using namespace autil;

using namespace build_service::analyzer;
using namespace build_service::document;
IE_NAMESPACE_USE(config);

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, TokenCombinedDocumentProcessor);

const string TokenCombinedDocumentProcessor::PREFIX_IDENTIFIER = "prefix";
const string TokenCombinedDocumentProcessor::SUFFIX_IDENTIFIER = "suffix";
const string TokenCombinedDocumentProcessor::INFIX_IDENTIFIER = "infix";

const string TokenCombinedDocumentProcessor::PROCESSOR_NAME = "TokenCombinedDocumentProcessor";

TokenCombinedDocumentProcessor::TokenCombinedDocumentProcessor() {
}

TokenCombinedDocumentProcessor::TokenCombinedDocumentProcessor(
        const TokenCombinedDocumentProcessor &other)
{
}

TokenCombinedDocumentProcessor::~TokenCombinedDocumentProcessor() {
}

bool TokenCombinedDocumentProcessor::process(const ExtendDocumentPtr &document) {
    const TokenizeDocumentPtr &tokenizeDocumentPtr = document->getTokenizeDocument();
    const FieldSchemaPtr &fieldSchema = _schemaPtr->GetFieldSchema();

    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        if (!(*it)->IsNormal()) {
            continue;
        }

        fieldid_t fieldId = (*it)->GetFieldId();
        FieldType fieldType = (*it)->GetFieldType();
        if (ft_text != fieldType) {
            continue;
        }

        const TokenizeFieldPtr &tokenFieldPtr = tokenizeDocumentPtr->getField(fieldId);
        if (!tokenFieldPtr) {
            continue;
        }

        TokenizeField::Iterator sectionIt = tokenFieldPtr->createIterator();
        for (; !sectionIt.isEnd(); sectionIt.next()) {
            TokenizeSection* section = *sectionIt;
            processSection(section);
        }
    }

    return true;
}

void TokenCombinedDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void TokenCombinedDocumentProcessor::combinePreToken(TokenizeSection::Iterator& preIt,
        TokenizeSection::Iterator& curIt, TokenizeSection* section) const
{
    Token* preToken = preIt.getToken();
    if (preToken != NULL) {
        Token* curToken = curIt.getToken();
        curToken->setIsStopWord(true);

        string tokenTxt = preToken->getText();
        tokenTxt += curToken->getText();

        string normalizedTokenText = preToken->getNormalizedText();
        normalizedTokenText += curToken->getNormalizedText();

        Token combinedToken(tokenTxt, 0, normalizedTokenText, false);
        section->insertExtendToken(combinedToken, preIt);
    }
}

void TokenCombinedDocumentProcessor::combineNextToken(TokenizeSection::Iterator& curIt,
        TokenizeSection::Iterator& nextIt, TokenizeSection* section) const
{
    Token* nextToken = nextIt.getToken();
    if (nextToken != NULL) {
        Token* curToken = curIt.getToken();
        curToken->setIsStopWord(true);
        BS_LOG(DEBUG, "token text:%s, isStopWord:%d, next token text:%s",
            curToken->getText().c_str(), curToken->isStopWord(), nextToken->getText().c_str());
        string tokenTxt = curToken->getText();
        tokenTxt += nextToken->getText();

        string normalizedTokenText = curToken->getNormalizedText();
        normalizedTokenText += nextToken->getNormalizedText();

        Token combinedToken(tokenTxt, 0, normalizedTokenText, false);
        section->insertExtendToken(combinedToken, nextIt);
    }
}

void TokenCombinedDocumentProcessor::processSection(TokenizeSection* section) const {
    TokenizeSection::Iterator tokenIt = section->createIterator();
    TokenizeSection::Iterator preTokenIt;

    for (; *tokenIt != NULL; tokenIt.next()) {

        Token* token = *tokenIt;
        bool isPrefix = _prefixTokenSet.find(token->getText()) != _prefixTokenSet.end();
        bool isSuffix = _suffixTokenSet.find(token->getText()) != _suffixTokenSet.end();
        bool isInfix = _infixTokenSet.find(token->getText()) != _infixTokenSet.end();

        if (token->isStopWord()) {
            TokenizeSection::Iterator nullIt;
            preTokenIt = nullIt;
            continue;
        } else if (isPrefix) {
            TokenizeSection::Iterator curIt = tokenIt;
            tokenIt.next();
            combineNextToken(curIt, tokenIt, section);
        } else if (isInfix) {
            TokenizeSection::Iterator curIt = tokenIt;
            combinePreToken(preTokenIt, curIt, section);

            tokenIt.next();
            combineNextToken(curIt, tokenIt, section);
        } else if (isSuffix) {
            combinePreToken(preTokenIt, tokenIt, section);
        }

        preTokenIt = tokenIt;
    }
}

void TokenCombinedDocumentProcessor::destroy() {
    delete this;
}

DocumentProcessor* TokenCombinedDocumentProcessor::clone() {
    return new TokenCombinedDocumentProcessor(*this);
}

bool TokenCombinedDocumentProcessor::init(const KeyValueMap &kvMap,
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
        config::ResourceReader *resourceReader)
{
    _schemaPtr = schemaPtr;

    fillStopTokenSet(kvMap, PREFIX_IDENTIFIER, _prefixTokenSet);
    fillStopTokenSet(kvMap, SUFFIX_IDENTIFIER, _suffixTokenSet);
    fillStopTokenSet(kvMap, INFIX_IDENTIFIER, _infixTokenSet);

    return true;
}

void TokenCombinedDocumentProcessor::fillStopTokenSet(const KeyValueMap &kvMap,
        const string &stopTokenIdentifier, std::set<string> &stopTokenSet)
{
    KeyValueMap::const_iterator it = kvMap.find(stopTokenIdentifier);
    if (it != kvMap.end()) {
        StringTokenizer st;
        st.tokenize(it->second, " ");
        size_t tokenNum = st.getNumTokens();
        for (size_t i = 0; i < tokenNum; ++i) {
            stopTokenSet.insert(st[i]);
        }
    }
}

void TokenCombinedDocumentProcessor::setPrefixStopwordSet(const set<string> &prefixTokenSet)
{
    _prefixTokenSet = prefixTokenSet;
}


}
}

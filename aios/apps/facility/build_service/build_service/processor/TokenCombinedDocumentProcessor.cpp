/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/processor/TokenCombinedDocumentProcessor.h"

#include <ext/alloc_traits.h>
#include <stddef.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringTokenizer.h"
#include "build_service/analyzer/Token.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/document/TokenizeField.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/ClassifiedDocument.h"
#include "indexlib/document/normal/tokenize/TokenizeDocument.h"
#include "indexlib/document/normal/tokenize/TokenizeField.h"
#include "indexlib/document/normal/tokenize/TokenizeSection.h"
#include "indexlib/indexlib.h"

using namespace build_service::analyzer;
using namespace build_service::document;
using namespace indexlib::config;

namespace build_service { namespace processor {
namespace {
using std::string;
}
BS_LOG_SETUP(processor, TokenCombinedDocumentProcessor);

const string TokenCombinedDocumentProcessor::PREFIX_IDENTIFIER = "prefix";
const string TokenCombinedDocumentProcessor::SUFFIX_IDENTIFIER = "suffix";
const string TokenCombinedDocumentProcessor::INFIX_IDENTIFIER = "infix";
const string TokenCombinedDocumentProcessor::PROCESSOR_NAME = "TokenCombinedDocumentProcessor";

TokenCombinedDocumentProcessor::TokenCombinedDocumentProcessor() {}

TokenCombinedDocumentProcessor::TokenCombinedDocumentProcessor(const TokenCombinedDocumentProcessor& other) {}

TokenCombinedDocumentProcessor::~TokenCombinedDocumentProcessor() {}

bool TokenCombinedDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    const TokenizeDocumentPtr& tokenizeDocument = document->getTokenizeDocument();
    const TokenizeDocumentPtr& lastTokenizeDocument = document->getLastTokenizeDocument();
    const auto& fieldConfigs = _schema->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        processField(fieldConfig, tokenizeDocument);
        processField(fieldConfig, lastTokenizeDocument);
    }

    return true;
}

void TokenCombinedDocumentProcessor::processField(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                                                  const TokenizeDocumentPtr& tokenizeDocument)
{
    if (!tokenizeDocument) {
        return;
    }

    fieldid_t fieldId = fieldConfig->GetFieldId();
    FieldType fieldType = fieldConfig->GetFieldType();
    if (ft_text != fieldType) {
        return;
    }
    const TokenizeFieldPtr& tokenFieldPtr = tokenizeDocument->getField(fieldId);
    if (!tokenFieldPtr) {
        return;
    }
    TokenizeField::Iterator sectionIt = tokenFieldPtr->createIterator();
    for (; !sectionIt.isEnd(); sectionIt.next()) {
        TokenizeSection* section = *sectionIt;
        processSection(section);
    }
}

void TokenCombinedDocumentProcessor::batchProcess(const std::vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void TokenCombinedDocumentProcessor::combinePreToken(TokenizeSection::Iterator& preIt, TokenizeSection::Iterator& curIt,
                                                     TokenizeSection* section) const
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
        BS_LOG(DEBUG, "token text:%s, isStopWord:%d, next token text:%s", curToken->getText().c_str(),
               curToken->isStopWord(), nextToken->getText().c_str());
        string tokenTxt = curToken->getText();
        tokenTxt += nextToken->getText();

        string normalizedTokenText = curToken->getNormalizedText();
        normalizedTokenText += nextToken->getNormalizedText();

        Token combinedToken(tokenTxt, 0, normalizedTokenText, false);
        section->insertExtendToken(combinedToken, nextIt);
    }
}

void TokenCombinedDocumentProcessor::processSection(TokenizeSection* section) const
{
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

void TokenCombinedDocumentProcessor::destroy() { delete this; }

DocumentProcessor* TokenCombinedDocumentProcessor::clone() { return new TokenCombinedDocumentProcessor(*this); }

bool TokenCombinedDocumentProcessor::init(const DocProcessorInitParam& param)
{
    _schema = param.schema;

    fillStopTokenSet(param.parameters, PREFIX_IDENTIFIER, _prefixTokenSet);
    fillStopTokenSet(param.parameters, SUFFIX_IDENTIFIER, _suffixTokenSet);
    fillStopTokenSet(param.parameters, INFIX_IDENTIFIER, _infixTokenSet);

    return true;
}

void TokenCombinedDocumentProcessor::fillStopTokenSet(const KeyValueMap& kvMap, const string& stopTokenIdentifier,
                                                      std::set<string>& stopTokenSet)
{
    KeyValueMap::const_iterator it = kvMap.find(stopTokenIdentifier);
    if (it != kvMap.end()) {
        autil::StringTokenizer st;
        st.tokenize(it->second, " ");
        size_t tokenNum = st.getNumTokens();
        for (size_t i = 0; i < tokenNum; ++i) {
            stopTokenSet.insert(st[i]);
        }
    }
}

void TokenCombinedDocumentProcessor::setPrefixStopwordSet(const std::set<string>& prefixTokenSet)
{
    _prefixTokenSet = prefixTokenSet;
}

}} // namespace build_service::processor

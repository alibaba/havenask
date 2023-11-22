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
#include "build_service/processor/TokenizeDocumentProcessor.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <iosfwd>
#include <stddef.h>

#include "alog/Logger.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/analyzer/Token.h"
#include "build_service/document/ClassifiedDocument.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/extend_document/tokenize_document.h"
#include "indexlib/document/normal/ClassifiedDocument.h"
#include "indexlib/document/normal/tokenize/AnalyzerToken.h"
#include "indexlib/util/ErrorLogCollector.h"

using namespace indexlib::document;
using namespace indexlib::config;
using namespace build_service::document;
using namespace build_service::analyzer;

namespace build_service { namespace processor {
namespace {
using std::string;
using std::stringstream;
} // namespace
BS_LOG_SETUP(processor, TokenizeDocumentProcessor);

const string TokenizeDocumentProcessor::PROCESSOR_NAME = "TokenizeDocumentProcessor";
TokenizeDocumentProcessor::TokenizeDocumentProcessor() {}

TokenizeDocumentProcessor::TokenizeDocumentProcessor(const TokenizeDocumentProcessor& other)
    : DocumentProcessor(other)
    , _impl(other._impl)
{
}

TokenizeDocumentProcessor::~TokenizeDocumentProcessor() {}

bool TokenizeDocumentProcessor::init(const DocProcessorInitParam& param)
{
    assert(param.schema);
    _impl = std::make_shared<indexlibv2::document::TokenizeDocumentConvertor>();
    auto status = _impl->Init(param.schema, param.analyzerFactory);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "tokenize document processor init failed");
        return false;
    }
    return true;
}

void TokenizeDocumentProcessor::batchProcess(const std::vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

// Populate tokenizeDocument and lastTokenizeDocument by tokenizing fields from raw document.
bool TokenizeDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    assert(document);
    const TokenizeDocumentPtr& tokenizeDocument = document->getTokenizeDocument();
    if (!tokenizeDocument) {
        ERROR_COLLECTOR_LOG(WARN, "get tokenize document failed");
        return true;
    }
    // lastTokenizeDocument has data from previous doc. The tokens in this doc will be diff'ed against tokenizeDocument
    // to convert doc from ADD to UPDATE.
    const TokenizeDocumentPtr& lastTokenizeDocument = document->getLastTokenizeDocument();

    auto rawDoc = document->getExtendDoc()->GetRawDocument();
    auto status =
        _impl->Convert(rawDoc.get(), document->getFieldAnalyzerNameMap(), tokenizeDocument, lastTokenizeDocument);
    return status.IsOK();
}

void TokenizeDocumentProcessor::destroy() { delete this; }

DocumentProcessor* TokenizeDocumentProcessor::clone() { return new TokenizeDocumentProcessor(*this); }

}} // namespace build_service::processor

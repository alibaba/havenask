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
#include "build_service/processor/SingleDocProcessorChain.h"

#include "build_service/document/ProcessedDocument.h"
#include "build_service/processor/DocumentProcessor.h"

using namespace std;
using namespace build_service::document;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, SingleDocProcessorChain);

SingleDocProcessorChain::SingleDocProcessorChain(const plugin::PlugInManagerPtr& pluginManagerPtr,
                                                 const indexlib::config::IndexPartitionSchemaPtr& schema,
                                                 const std::shared_ptr<analyzer::AnalyzerFactory>& analyzerFactory)
    : DocumentProcessorChain(schema)
    , _pluginManagerPtr(pluginManagerPtr)
    , _analyzerFactory(analyzerFactory)
{
}

SingleDocProcessorChain::SingleDocProcessorChain(SingleDocProcessorChain& other)
    : DocumentProcessorChain(other)
    , _pluginManagerPtr(other._pluginManagerPtr)
    , _analyzerFactory(other._analyzerFactory)
{
    _documentProcessors.resize(other._documentProcessors.size());
    for (size_t i = 0; i < _documentProcessors.size(); ++i) {
        _documentProcessors[i] = other._documentProcessors[i]->allocate();
    }
}

SingleDocProcessorChain::~SingleDocProcessorChain()
{
    for (DocumentProcessor* processor : _documentProcessors) {
        processor->deallocate();
    }
}

bool SingleDocProcessorChain::processExtendDocument(const document::ExtendDocumentPtr& extendDocument)
{
    for (DocumentProcessor* processor : _documentProcessors) {
        DocOperateType docOperateType = extendDocument->getRawDocument()->getDocOperateType();
        if (!processor->needProcess(docOperateType)) {
            continue;
        }
        if (!processor->process(extendDocument)) {
            return false;
        }
    }
    return true;
}

DocumentProcessorChain* SingleDocProcessorChain::clone() { return new SingleDocProcessorChain(*this); }

void SingleDocProcessorChain::addDocumentProcessor(DocumentProcessor* processor)
{
    _documentProcessors.push_back(processor);
}

void SingleDocProcessorChain::batchProcessExtendDocs(const vector<ExtendDocumentPtr>& extDocVec)
{
    vector<ExtendDocumentPtr> needProcessDocs;
    needProcessDocs.reserve(extDocVec.size());
    for (DocumentProcessor* processor : _documentProcessors) {
        needProcessDocs.clear();
        for (const auto& extDoc : extDocVec) {
            if (extDoc->testWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH)) {
                continue;
            }

            DocOperateType docOperateType = extDoc->getRawDocument()->getDocOperateType();
            if (processor->needProcess(docOperateType)) {
                needProcessDocs.push_back(extDoc);
            }
        }
        if (!needProcessDocs.empty()) {
            processor->batchProcess(needProcessDocs);
        }
    }
}

}} // namespace build_service::processor

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
#ifndef ISEARCH_BS_PROCESSORWORKITEM_H
#define ISEARCH_BS_PROCESSORWORKITEM_H

#include "autil/WorkItem.h"
#include "build_service/common_define.h"
#include "build_service/config/ProcessorChainSelectorConfig.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/processor/ProcessorChainSelector.h"
#include "build_service/util/Log.h"

namespace indexlib::util {
class AccumulativeCounter;
using AccumulativeCounterPtr = std::shared_ptr<AccumulativeCounter>;
} // namespace indexlib::util

namespace build_service { namespace processor {

class ProcessorMetricReporter;

class ProcessorWorkItem : public autil::WorkItem
{
public:
    ProcessorWorkItem(const DocumentProcessorChainVecPtr& chains, const ProcessorChainSelectorPtr& chainSelector,
                      bool enableRewriteDeleteSubDoc, ProcessorMetricReporter* reporter);
    ~ProcessorWorkItem();

private:
    ProcessorWorkItem(const ProcessorWorkItem&) = delete;
    ProcessorWorkItem& operator=(const ProcessorWorkItem&) = delete;

public:
    void initProcessData(const document::RawDocumentPtr& rawDocPtr);
    void initBatchProcessData(const document::RawDocumentVecPtr& rawDocsPtr);

    // doc will be processed by all chains, error in one chain will not affect other chains
    // doc with *SKIP* and *UNKNOWN* will be ignored, not put in ProcessedDocumentVecPtr
    const document::ProcessedDocumentVecPtr& getProcessedDocs() const { return _processedDocumentVecPtr; }

public:
    void process() override;
    void setProcessErrorCounter(const indexlib::util::AccumulativeCounterPtr& processErrorCounter)
    {
        _processErrorCounter = processErrorCounter;
    }
    void setProcessDocCountCounter(const indexlib::util::AccumulativeCounterPtr& processDocCountCounter)
    {
        _processDocCountCounter = processDocCountCounter;
    }

    uint32_t getRawDocCount() const;

private:
    void singleProcess();
    void batchProcess();

    size_t getRawDocumentsForEachChain(const document::RawDocumentVecPtr& batchRawDocsPtr,
                                       std::vector<document::RawDocumentVecPtr>& rawDocsForChains,
                                       std::vector<const ProcessorChainSelector::ChainIdVector*>& chainIdQueue);

    void assembleProcessedDoc(size_t chainId, const document::RawDocumentPtr& rawDoc,
                              document::ProcessedDocumentPtr& pDoc);

    void batchProcessWithCustomizeChainSelector();
    void batchProcessWithAllChainSelector();

private:
    // input
    document::RawDocumentPtr _rawDocPtr;
    document::RawDocumentVecPtr _batchRawDocsPtr;

    // output
    document::ProcessedDocumentVecPtr _processedDocumentVecPtr;

    // const
    DocumentProcessorChainVecPtr _chains;
    ProcessorChainSelectorPtr _chainSelector;
    bool _enableRewriteDeleteSubDoc;
    ProcessorMetricReporter* _reporter;

    indexlib::util::AccumulativeCounterPtr _processErrorCounter;
    indexlib::util::AccumulativeCounterPtr _processDocCountCounter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorWorkItem);

}} // namespace build_service::processor

#endif // ISEARCH_BS_PROCESSORWORKITEM_H

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
#ifndef ISEARCH_BS_PROCESSEDDOCHANDLER_H
#define ISEARCH_BS_PROCESSEDDOCHANDLER_H

#include <unordered_set>

#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "build_service/common_define.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/processor/Processor.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/util/ConditionQueue.h"
#include "build_service/util/Log.h"
#include "build_service/util/StreamQueue.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/SourceOpConverter.h"
#include "build_service/workflow/SrcDataNode.h"
#include "build_service/workflow/SrcWorkItem.h"

namespace build_service { namespace workflow {

class ProcessedDocHandler
{
public:
    typedef reader::RawDocumentReader RawDocumentCreator;

public:
    ProcessedDocHandler(SrcDataNode* srcNode, processor::Processor* processor);
    virtual ~ProcessedDocHandler();

private:
    ProcessedDocHandler(const ProcessedDocHandler&);
    ProcessedDocHandler& operator=(const ProcessedDocHandler&);

public:
    virtual bool start(const config::ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap,
                       const proto::PartitionId& partitionId, indexlib::util::MetricProviderPtr metricsProvider,
                       const indexlib::util::CounterMapPtr& counterMap,
                       const indexlib::config::IndexPartitionSchemaPtr schema, int64_t initLocatorOffset);
    void consume(document::ProcessedDocumentPtr& doc);
    FlowError getProcessedDoc(document::ProcessedDocumentPtr& doc, int64_t& originalDocTs);
    virtual bool getLocator(common::Locator& locator) const;
    void stop(bool instant = false);

private:
    void solveWaitBuildDoc();
    void solveInputQueueDoc();
    void processBuiltDoc(const document::ProcessedDocumentPtr& doc, const SourceOpConverter::SolvedDocType& type);
    bool runWithTry(std::function<void(void)> func, uint64_t retryTime);
    void registerMetric(indexlib::util::MetricProviderPtr metricProvider);

protected:
    struct DocInfo {
        DocInfo() = default;
        DocInfo(const document::ProcessedDocumentPtr& doc_, const SourceOpConverter::SolvedDocType& type_)
            : type(type_)
            , doc(doc_)
            , docTimestamp(0)
            , pkHash(0)
        {
        }
        DocInfo(const document::ProcessedDocumentPtr& doc_, const SourceOpConverter::SolvedDocType& type_,
                int64_t docTimestamp_)
            : type(type_)
            , doc(doc_)
            , docTimestamp(docTimestamp_)
            , pkHash(0)
        {
        }
        DocInfo(const document::ProcessedDocumentPtr& doc_, uint64_t pkHash_)
            : type(SourceOpConverter::SDT_UNKNOWN)
            , doc(doc_)
            , docTimestamp(0)
            , pkHash(pkHash_)
        {
        }
        DocInfo(const DocInfo& rhs)
            : type(rhs.type)
            , doc(rhs.doc)
            , docTimestamp(rhs.docTimestamp)
            , pkHash(rhs.pkHash)
            , locator(rhs.locator)
        {
        }
        bool operator<(const DocInfo& rhs) const { return pkHash < rhs.pkHash; }
        SourceOpConverter::SolvedDocType type;
        document::ProcessedDocumentPtr doc;
        int64_t docTimestamp;
        uint64_t pkHash;
        common::Locator locator;
    };
    struct LocatorComparator {
        bool operator()(const common::Locator& lhs, const common::Locator& rhs) const
        {
            return lhs.GetOffset() < rhs.GetOffset();
        }
    };
    using DocumentStreamQueue = SrcWorkItem::DocumentStreamQueue;
    typedef util::ConditionQueue<DocInfo> ProcessedDocConditionQueue;
    typedef util::StreamQueue<DocInfo> ProcessedDocInfoStreamQueue;
    BS_TYPEDEF_PTR(DocumentStreamQueue);
    BS_TYPEDEF_PTR(ProcessedDocConditionQueue);
    BS_TYPEDEF_PTR(ProcessedDocInfoStreamQueue);

private:
    SrcDataNode* _srcNode;
    processor::Processor* _processor;
    SourceOpConverter _opConverter;

    // for parallel search
    autil::ThreadPoolPtr _pool;
    ProcessedDocConditionQueuePtr _inputQueue;
    DocumentStreamQueuePtr _buildQueue;
    ProcessedDocInfoStreamQueuePtr _outputQueue;
    autil::ThreadPtr _input2processThread;
    autil::ThreadPtr _build2outputThread;
    autil::ThreadMutex _locatorMutex;
    autil::ThreadCond _pkMutex;
    std::map<common::Locator, uint32_t, LocatorComparator> _uncommitedLocators;
    std::unordered_set<uint64_t> _ongoingPks;
    common::Locator _lastLocator;
    common::Locator _candidateLastLocator;

    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::util::MetricPtr _inputQueueSizeMetric;
    indexlib::util::MetricPtr _buildQueueSizeMetric;
    indexlib::util::MetricPtr _outputQueueSizeMetric;
    indexlib::util::MetricPtr _processQpsMetric;
    indexlib::util::MetricPtr _skipQpsMetric;
    indexlib::util::MetricPtr _buildLocatorMetric;
    indexlib::util::MetricPtr _exceptionQpsMetric;
    SrcWorkItem::SrcWorkItemMetricGroup _metricGroup;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessedDocHandler);

}} // namespace build_service::workflow

#endif // ISEARCH_BS_PROCESSEDDOCHANDLER_H

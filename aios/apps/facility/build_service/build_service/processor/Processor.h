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
#pragma once

#include <atomic>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/config/ProcessorConfigReader.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/document/RawDocument.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/processor/ProcessorChainSelector.h"
#include "build_service/processor/ProcessorMetricReporter.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/ProcessorWorkItemExecutor.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/Log.h"
#include "indexlib/document/builtin_parser_init_param.h"
#include "indexlib/document/document_parser.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace processor {

class Processor : public proto::ErrorCollector
{
public:
    Processor(const std::string& strategyParam = "");
    virtual ~Processor();

private:
    Processor(const Processor&);
    Processor& operator=(const Processor&);

public:
    // virtual for mock
    virtual bool start(const config::ProcessorConfigReaderPtr& resourceReaderPtr, const proto::PartitionId& partitionId,
                       indexlib::util::MetricProviderPtr metricProvider,
                       const indexlib::util::CounterMapPtr& counterMap, const KeyValueMap& kvMap = KeyValueMap(),
                       bool forceSingleThreaded = false, bool isTablet = false);
    virtual void stop(bool instant = false, bool seal = false);
    void drop();
    bool isSealed() const { return _sealed.load(); }
    bool isDropped() const { return _dropped.load(std::memory_order_relaxed); }

public:
    virtual void processDoc(const document::RawDocumentPtr& rawDoc);
    virtual document::ProcessedDocumentVecPtr getProcessedDoc();
    virtual document::ProcessedDocumentVecPtr process(const document::RawDocumentVecPtr& rawDoc);

public:
    virtual uint32_t getWaitProcessCount() const;
    virtual uint32_t getProcessedCount() const;

protected:
    indexlib::config::IndexPartitionSchemaPtr
    getIndexPartitionSchema(const config::ResourceReaderPtr& resourceReaderPtr,
                            const config::DocProcessorChainConfig& docProcessorChainConfig) const;

    config::DocProcessorChainConfigVec
    rewriteRegionDocumentProcessorConfig(const config::ProcessorConfigReaderPtr& resourceReader,
                                         const config::DocProcessorChainConfigVec& inChains) const;

    bool initProcessorChainSelector(const config::ProcessorConfigReaderPtr& resourceReader,
                                    const proto::PartitionId& partitionId,
                                    const config::DocProcessorChainConfigVecPtr& chainConfig);

    virtual bool loadChain(const config::ProcessorConfigReaderPtr& resourceReaderPtr,
                           const proto::PartitionId& partitionId, const indexlib::util::CounterMapPtr& counterMap,
                           const KeyValueMap& kvMap);

    virtual bool loadChainV2(const config::ProcessorConfigReaderPtr& resourceReaderPtr,
                             const proto::PartitionId& partitionId, const indexlib::util::CounterMapPtr& counterMap,
                             const KeyValueMap& kvMap);

    static std::vector<std::string> getClusterNames(const config::DocProcessorChainConfig& chainConfig,
                                                    const std::vector<std::string>& clusterNames);

protected:
    DocumentProcessorChainVecPtr getProcessorChains() const;
    void setProcessorChains(const DocumentProcessorChainVecPtr& newChains);

private:
    bool loadChain(const config::ProcessorConfigReaderPtr& resourceReaderPtr, const proto::PartitionId& partitionId,
                   const indexlib::util::CounterMapPtr& counterMap, const KeyValueMap& kvMap, bool isTablet);

protected:
    DocumentProcessorChainVecPtr _chains;
    ProcessorWorkItemExecutorPtr _executor;
    mutable autil::ThreadMutex _chainMutex;
    autil::ThreadMutex _stopMtx;
    indexlib::util::MetricProviderPtr _metricProvider;
    config::ProcessorConfigReaderPtr _processorConfigReader;
    indexlib::util::MetricPtr _waitProcessCountMetric;
    ProcessorMetricReporter _reporter;
    indexlib::util::AccumulativeCounterPtr _processErrorCounter;
    indexlib::util::AccumulativeCounterPtr _processDocCountCounter;
    ProcessorChainSelectorPtr _chainSelector;
    std::string _strategyParam;

    bool _stopped;

    // dropped and sealed are used for the communication between DocProcessorConsumer and DocProcessorProducer in two
    // workflows.
    //  flow1 sealed -> flow2 sealed; (FE_SEALED)
    //  flow2 dropped -> flow1 dropped; (FE_DROPFLOW)
    std::atomic<bool> _dropped;
    std::atomic<bool> _sealed;
    bool _enableRewriteDeleteSubDoc;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Processor);

}} // namespace build_service::processor

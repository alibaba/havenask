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

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/builder/Builder.h"
#include "build_service/builder/BuilderV2.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common/SourceEnd2EndLatencyReporter.h"
#include "build_service/common/SwiftParam.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/processor/Processor.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/workflow/ProcessedDocHandler.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/SrcDataNode.h"
#include "build_service/workflow/SwiftDocumentBatchProducer.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace indexlibv2::framework {
class ITablet;
}

namespace build_service { namespace common {
BS_TYPEDEF_PTR(CounterSynchronizer);
}} // namespace build_service::common

namespace build_service { namespace workflow {

class FlowFactory : public proto::ErrorCollector
{
public:
    enum ProducerType {
        TP_RAW = 0,
        TP_SRC = 1,
        TP_PROCESSOR = 2,
        TP_BUILDER = 3,
        TP_MESSAGE_QUEUE = 4,
        TP_BUILDERV2 = 5,
        TP_DOC_BATCH = 6,
    };
    typedef ProducerType ConsumerType;
    struct RoleInitParam {
        RoleInitParam() : metricProvider(NULL) {}
        config::ResourceReaderPtr resourceReader;
        KeyValueMap kvMap;
        proto::PartitionId partitionId;
        indexlib::util::MetricProviderPtr metricProvider;
        indexlib::util::CounterMapPtr counterMap;
        common::CounterSynchronizerPtr counterSynchronizer;
        indexlib::config::IndexPartitionSchemaPtr schema;
        std::shared_ptr<indexlibv2::config::ITabletSchema> schemav2;
        bool isTablet = false;
    };

public:
    FlowFactory(const common::ResourceKeeperMap& resourceMap = {},
                const util::SwiftClientCreatorPtr& swiftClientCreator = nullptr,
                const indexlib::partition::IndexPartitionPtr& indexPart = indexlib::partition::IndexPartitionPtr(),
                const indexlib::util::TaskSchedulerPtr& taskScheduler = indexlib::util::TaskSchedulerPtr());
    FlowFactory(const common::ResourceKeeperMap& resourceMap, const util::SwiftClientCreatorPtr& swiftClientCreator,
                std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                const indexlib::util::TaskSchedulerPtr& taskScheduler);
    virtual ~FlowFactory();

private:
    FlowFactory(const FlowFactory&);
    FlowFactory& operator=(const FlowFactory&);

public:
    virtual RawDocProducer* createRawDocProducer(const RoleInitParam& initParam, const ProducerType& type);
    virtual RawDocConsumer* createRawDocConsumer(const RoleInitParam& initParam, const ConsumerType& type);

    virtual ProcessedDocProducer* createProcessedDocProducer(const RoleInitParam& initParamconst,
                                                             const ProducerType& type);
    virtual ProcessedDocConsumer* createProcessedDocConsumer(const RoleInitParam& initParam, const ConsumerType& type);

    virtual DocumentBatchProducer* createBatchDocProducer(const RoleInitParam& initParam);
    virtual DocumentBatchConsumer* createBatchDocConsumer(const RoleInitParam& initParam);

    virtual bool initCounterMap(RoleInitParam& initParam, BuildFlowMode mode);

public:
    void setTabletMode(bool isTablet) { _isTablet = isTablet; }
    reader::RawDocumentReader* getReader() const { return _reader; }
    processor::Processor* getProcessor() const { return _processor; }
    builder::Builder* getBuilder() const { return _builder; }
    builder::BuilderV2* getBuilderV2() const { return _builderv2; }
    static std::string clusterInputKey(const std::string& clusterName) { return clusterName + "__input"; }
    static std::string clusterOutputKey(const std::string& clusterName) { return clusterName + "__output"; }
    virtual void collectErrorInfos(std::vector<proto::ErrorInfo>& errorInfos) const;

    virtual void collectUsingResources(common::ResourceKeeperMap& usingKeepers);
    virtual void updateUsingResources(common::ResourceKeeperMap& availKeepers, bool& needRestart);

private:
    reader::RawDocumentReader* getReader(const RoleInitParam& initParam);
    virtual reader::RawDocumentReader* createReader(const RoleInitParam& initParam);

    processor::Processor* getProcessor(const RoleInitParam& initParam);
    virtual processor::Processor* createProcessor(const RoleInitParam& initParam);

    builder::Builder* getBuilder(const RoleInitParam& initParam);
    virtual builder::Builder* createBuilder(const RoleInitParam& initParam);

    builder::BuilderV2* getBuilderV2(const RoleInitParam& initParam);
    virtual builder::BuilderV2* createBuilderV2(const RoleInitParam& initParam);

    SrcDataNode* getSrcNode(const RoleInitParam& initParam);
    virtual SrcDataNode* createSrcNode(const RoleInitParam& param);

    ProcessedDocHandler* getDocHandler(const RoleInitParam& initParam);
    virtual ProcessedDocHandler* createDocHandler(const RoleInitParam& param);

    std::string getClusterName(const RoleInitParam& initParam);
    RawDocConsumer* createRawDocBuilderConsumer(const RoleInitParam& initParam);
    virtual SwiftProcessedDocProducer* doCreateSwiftProcessedDocProducer(const common::SwiftParam& swiftParam,
                                                                         const RoleInitParam& param);
    SwiftProcessedDocProducer* createSingleSwiftProcessedDocProducer(const RoleInitParam& initParam,
                                                                     const common::SwiftResourceKeeperPtr& swiftKeeper,
                                                                     const config::SwiftConfig& swiftConfig);
    SwiftProcessedDocProducer* createMultiSwiftProcessedDocProducer(uint16_t produceDocThreadCount,
                                                                    const RoleInitParam& initParam,
                                                                    const common::SwiftResourceKeeperPtr& swiftKeeper,
                                                                    const config::SwiftConfig& swiftConfig);

    SwiftDocumentBatchProducer* doCreateSwiftDocumentBatchProducer(const common::SwiftParam& swiftParam,
                                                                   const RoleInitParam& param);

    virtual ProcessedDocProducer* createSwiftProcessedDocProducer(const RoleInitParam& initParam);
    virtual ProcessedDocConsumer* createSwiftProcessedDocConsumer(const RoleInitParam& initParam);

    std::map<std::string, SwiftWriterWithMetric> createSwiftWriters(const RoleInitParam& initParam);
    bool createSwiftReaderConfig(const RoleInitParam& initParam, const config::SwiftConfig& swiftConfig,
                                 std::string& swiftReadConfig);
    bool initSwiftProcessedDocProducer(const config::SwiftConfig& swiftConfig, const RoleInitParam& initParam,
                                       const std::string& topicName, SwiftProcessedDocProducer* producer);

    bool initSwiftLinkReporter(const RoleInitParam& initParam, int64_t totalSwiftPartitionCount,
                               const std::string& topicName, SwiftProcessedDocProducer* producer);

private:
    common::ResourceKeeperMap _availableResources, _usingResources;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    indexlib::partition::IndexPartitionPtr _indexPart;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    reader::RawDocumentReaderCreator _readerCreator;
    reader::RawDocumentReader* _reader;
    ProcessedDocHandler* _docHandler;
    SrcDataNode* _srcNode;
    processor::Processor* _processor;
    builder::Builder* _builder;
    builder::BuilderV2* _builderv2;
    indexlib::util::TaskSchedulerPtr _taskScheduler;
    bool _isTablet = false;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FlowFactory);

}} // namespace build_service::workflow

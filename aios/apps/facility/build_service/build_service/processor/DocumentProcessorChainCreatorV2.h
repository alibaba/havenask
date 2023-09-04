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
#ifndef ISEARCH_BS_DOCUMENTPROCESSORCHAINCREATORV2_H
#define ISEARCH_BS_DOCUMENTPROCESSORCHAINCREATORV2_H

#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/common_define.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/partition_meta.h"

namespace indexlibv2::config {
class ITabletSchema;
class TruncateOptionConfig;
} // namespace indexlibv2::config

namespace indexlibv2::document {
class IDocumentRewriter;
}

namespace indexlib { namespace util {
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace build_service { namespace processor {
class DocumentProcessorFactory;
class DocumentProcessorChainCreatorV2
{
public:
    DocumentProcessorChainCreatorV2();
    ~DocumentProcessorChainCreatorV2();

private:
    DocumentProcessorChainCreatorV2(const DocumentProcessorChainCreatorV2&);
    DocumentProcessorChainCreatorV2& operator=(const DocumentProcessorChainCreatorV2&);

public:
    bool init(const config::ResourceReaderPtr& resourceReaderPtr, indexlib::util::MetricProviderPtr metricProvider,
              const indexlib::util::CounterMapPtr& counterMap);
    DocumentProcessorChainPtr create(const config::DocProcessorChainConfig& docProcessorChainConfig,
                                     const std::vector<std::string>& clusterNames) const;

private:
    SingleDocProcessorChain* createSingleChain(const plugin::PlugInManagerPtr& plugInManagerPtr,
                                               const config::ProcessorInfos& processorInfos,
                                               const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                               const std::vector<std::string>& clusterNames,
                                               bool processForSubDoc) const;

    DocumentProcessor* createDocumentProcessor(DocumentProcessorFactory* factory, const std::string& className,
                                               const DocProcessorInitParam& param) const;

    std::string checkAndGetTableName(const std::vector<std::string>& clusterNames) const;
    std::string getTableName(const std::string& clusterName) const;
    bool
    initParamForAdd2UpdateRewriter(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                   const std::vector<std::string>& clusterNames,
                                   std::vector<std::shared_ptr<indexlibv2::config::TruncateOptionConfig>>& optionsVec,
                                   std::vector<indexlibv2::config::SortDescriptions>& sortDescVec) const;

    std::shared_ptr<indexlibv2::document::IDocumentRewriter>
    createAndInitAdd2UpdateRewriter(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                    const std::vector<std::string>& clusterNames) const;

    indexlib::document::DocumentInitParamPtr
    createBuiltInInitParam(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                           const std::vector<std::string>& clusterNames, bool needAdd2Update) const;

    bool initDocumentProcessorChain(const config::DocProcessorChainConfig& docProcessorChainConfig,
                                    const config::ProcessorInfos& mainProcessorInfos,
                                    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                    const std::vector<std::string>& clusterNames, const std::string& tableName,
                                    DocumentProcessorChainPtr retChain) const;
    std::unique_ptr<indexlibv2::document::IDocumentFactory>
    createDocumentFactory(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                          const std::vector<std::string>& clusterNames) const;

private:
    enum ProcessorInfoInsertType { INSERT_TO_FIRST, INSERT_TO_LAST };

private:
    static bool needAdd2UpdateRewriter(const config::ProcessorInfos& processorInfos);

    static config::ProcessorInfos constructProcessorInfos(const config::ProcessorInfos& processorInfoInConfig,
                                                          bool needRegionProcessor, bool needSubParser, bool rawText);

    static void insertProcessorInfo(config::ProcessorInfos& processorInfos,
                                    const config::ProcessorInfo& insertProcessorInfo, ProcessorInfoInsertType type);

public:
    analyzer::AnalyzerFactoryPtr TEST_getAnalyzerFactory() const { return _analyzerFactoryPtr; }

private:
    config::ResourceReaderPtr _resourceReaderPtr;
    proto::PartitionId _partitionId;
    analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::util::CounterMapPtr _counterMap;

private:
    friend class DocumentProcessorChainCreatorTest;
    BS_LOG_DECLARE();
};

}} // namespace build_service::processor

#endif // ISEARCH_BS_DOCUMENTPROCESSORCHAINCREATORV2_H

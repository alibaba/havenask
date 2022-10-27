#ifndef ISEARCH_BS_DOCUMENTPROCESSORCHAINCREATOR_H
#define ISEARCH_BS_DOCUMENTPROCESSORCHAINCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/proto/BasicDefs.pb.h"
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/config/index_partition_options.h>
#include <indexlib/index_base/index_meta/partition_meta.h>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace build_service {
namespace processor {
class DocumentProcessorFactory;
class DocumentProcessorChainCreator
{
public:
    DocumentProcessorChainCreator();
    ~DocumentProcessorChainCreator();
private:
    DocumentProcessorChainCreator(const DocumentProcessorChainCreator &);
    DocumentProcessorChainCreator& operator=(const DocumentProcessorChainCreator &);
public:
    bool init(const config::ResourceReaderPtr &resourceReaderPtr,
              IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
              const IE_NAMESPACE(util)::CounterMapPtr &counterMap);
    DocumentProcessorChainPtr create(
            const config::DocProcessorChainConfig &docProcessorChainConfig,
            const std::vector<std::string> &clusterNames) const;
private:
    SingleDocProcessorChain *createSingleChain(
            const plugin::PlugInManagerPtr &plugInManagerPtr,
            const config::ProcessorInfos &processorInfos,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
            const std::vector<std::string> &clusterNames,
            bool processForSubDoc) const;

    DocumentProcessor *createDocumentProcessor(
            DocumentProcessorFactory *factory, const std::string &className,
            const DocProcessorInitParam &param) const;

    std::string checkAndGetTableName(
            const std::vector<std::string> &clusterNames) const;
    std::string getTableName(const std::string &clusterName) const;

    void initParamForAdd2UpdateRewriter(
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
            const std::vector<std::string> &clusterNames,
            std::vector<IE_NAMESPACE(config)::IndexPartitionOptions> &optionsVec,
            std::vector<IE_NAMESPACE(common)::SortDescriptions> &sortDescVec) const;
            
    IE_NAMESPACE(document)::DocumentRewriterPtr createAdd2UpdateRewriter(
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
            const std::vector<std::string> &clusterNames) const;

    IE_NAMESPACE(document)::DocumentInitParamPtr createBuiltInInitParam(
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
            const std::vector<std::string> &clusterNames, bool needAdd2Update) const;

private:
    enum ProcessorInfoInsertType {
        INSERT_TO_FIRST,
        INSERT_TO_LAST
    };
private:
    static bool needAdd2UpdateRewriter(
            const config::ProcessorInfos &processorInfos);

    static config::ProcessorInfos constructProcessorInfos(
            const config::ProcessorInfos &processorInfoInConfig,
            bool needRegionProcessor, bool needSubParser, bool rawText);

    static void insertProcessorInfo(config::ProcessorInfos &processorInfos,
                                    const config::ProcessorInfo &insertProcessorInfo,
                                    ProcessorInfoInsertType type);
private:
    config::ResourceReaderPtr _resourceReaderPtr;
    proto::PartitionId _partitionId;
    analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
    IE_NAMESPACE(misc)::MetricProviderPtr _metricProvider;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;

private:
    friend class DocumentProcessorChainCreatorTest;
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_DOCUMENTPROCESSORCHAINCREATOR_H

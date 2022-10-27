#ifndef ISEARCH_BS_BUILDERCREATOR_H
#define ISEARCH_BS_BUILDERCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/Builder.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/config/BuilderClusterConfig.h"
#include <indexlib/util/memory_control/quota_control.h>
#include <indexlib/config/index_partition_schema.h>
#include <fslib/fslib.h>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace builder {

class BuilderCreator
{
public:
    BuilderCreator(const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart =
                   IE_NAMESPACE(partition)::IndexPartitionPtr());
    virtual ~BuilderCreator();
private:
    BuilderCreator(const BuilderCreator &);
    BuilderCreator& operator=(const BuilderCreator &);
public:
    virtual Builder *create(const config::ResourceReaderPtr &resourceReader,
                            const KeyValueMap &kvMaps,
                            const proto::PartitionId &partitionId,
                            IE_NAMESPACE(misc)::MetricProviderPtr metricProvider = IE_NAMESPACE(misc)::MetricProviderPtr());
private:
    Builder* doCreateIndexlibBuilder(
            const config::ResourceReaderPtr &resourceReader,
            const config::BuilderClusterConfig &clusterConfig,
            IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
            fslib::fs::FileLock *fileLock);
    
    Builder* doCreateBuilder(
            const IE_NAMESPACE(partition)::IndexBuilderPtr &indexBuilder,
            const config::BuilderConfig &builderConfig, 
            int64_t buildTotalMem,
            IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
            fslib::fs::FileLock *fileLock);
    bool lockPartition(const KeyValueMap &kvMap,
                       const proto::PartitionId &partitionId,
                       std::unique_ptr<fslib::fs::FileLock> &fileLock);
    IE_NAMESPACE(util)::QuotaControlPtr createMemoryQuotaControl(
            int64_t buildTotalMem, const config::BuilderConfig &builderConfig) const;
    int64_t getBuildTotalMemory(const config::BuilderClusterConfig &clusterConfig) const;
private:
    static fslib::fs::FileLock *createPartitionLock(const std::string &zkRoot,
            const proto::PartitionId &pid);
    static bool checkAndStorePartitionMeta(
            const std::string &indexPath, 
            const IE_NAMESPACE(index_base)::SortDescriptions &sortDescs);
private:
    // virtual for test
    virtual IE_NAMESPACE(partition)::IndexBuilderPtr createIndexBuilder(
            const config::ResourceReaderPtr &resourceReader,
            const config::BuilderClusterConfig &clusterConfig,
            const IE_NAMESPACE(util)::QuotaControlPtr& quotaControl,
            IE_NAMESPACE(misc)::MetricProviderPtr metricProvider = IE_NAMESPACE(misc)::MetricProviderPtr());
    virtual Builder* createSortedBuilder(
            const IE_NAMESPACE(partition)::IndexBuilderPtr &indexBuilder,
            int64_t buildTotalMem,
            fslib::fs::FileLock *fileLock);
    virtual Builder* createNormalBuilder(
            const IE_NAMESPACE(partition)::IndexBuilderPtr &indexBuilder,
            fslib::fs::FileLock *fileLock);
    virtual Builder* createAsyncBuilder(
            const IE_NAMESPACE(partition)::IndexBuilderPtr &indexBuilder,
            fslib::fs::FileLock *fileLock);
    Builder *createLineDataBuilder(
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
            const config::BuilderConfig &builderConfig,
            IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
            fslib::fs::FileLock *fileLock);
    virtual bool initBuilder(Builder *builder, 
                             const config::BuilderConfig &builderConfig, 
                             IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);

    IE_NAMESPACE(partition)::IndexBuilderPtr createIndexBuilderForIncBuild(
            const config::ResourceReaderPtr &resourceReader,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
            const config::BuilderClusterConfig &clusterConfig,
            const IE_NAMESPACE(util)::QuotaControlPtr& quotaControl,
            IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);

    bool GetIncBuildParam(const config::ResourceReaderPtr &resourceReader,
                          const config::BuilderClusterConfig &clusterConfig,
                          proto::Range& partitionRange,
                          IE_NAMESPACE(index_base)::ParallelBuildInfo& incBuildInfo);

private:
    IE_NAMESPACE(partition)::IndexPartitionPtr _indexPart;
    proto::PartitionId _partitionId;
    int32_t _workerPathVersion;
    std::string _indexRootPath;

private:
    static const uint32_t LOCK_TIME_OUT = 60;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuilderCreator);

}
}

#endif //ISEARCH_BS_BUILDERCREATOR_H

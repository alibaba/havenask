#ifndef __INDEXLIB_BUILDIN_SEGMENT_ATTRIBUTE_UPDATER_FACTORY_H
#define __INDEXLIB_BUILDIN_SEGMENT_ATTRIBUTE_UPDATER_FACTORY_H

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater_factory.h"
#include "indexlib/indexlib.h"
#include <tr1/memory>

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, ModuleClassConfig);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_BEGIN(index);

class BuildinSegmentMetricsUpdaterFactory : public SegmentMetricsUpdaterFactory
{
public:
    BuildinSegmentMetricsUpdaterFactory();
    ~BuildinSegmentMetricsUpdaterFactory();

public:
    virtual std::unique_ptr<index::SegmentMetricsUpdater> CreateUpdater(
        const std::string& className, const util::KeyValueMap& parameters,
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options) override;

    virtual std::unique_ptr<index::SegmentMetricsUpdater> CreateUpdaterForMerge(
        const std::string& className, const util::KeyValueMap& parameters,
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders) override;

private:
    std::unique_ptr<index::SegmentMetricsUpdater> DoCreate(const std::string& updaterName);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildinSegmentMetricsUpdaterFactory);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUILDIN_SEGMENT_METRICS_UPDATER_FACTORY_H

#include "indexlib/index/buildin_segment_metrics_updater_factory.h"
#include "indexlib/index/max_min_segment_metrics_updater.h"
#include "indexlib/index/lifecycle_segment_metrics_updater.h"
#include "indexlib/index/time_series_segment_metrics_updater.h"
#include "indexlib/index/multi_segment_metrics_updater.h"
#include "indexlib/config/module_class_config.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BuildinSegmentMetricsUpdaterFactory);

BuildinSegmentMetricsUpdaterFactory::BuildinSegmentMetricsUpdaterFactory() {}

BuildinSegmentMetricsUpdaterFactory::~BuildinSegmentMetricsUpdaterFactory() {}

std::unique_ptr<index::SegmentMetricsUpdater> BuildinSegmentMetricsUpdaterFactory::DoCreate(
    const std::string& updaterName)
{
    std::unique_ptr<SegmentMetricsUpdater> ret;
#define ENUM_UPDATER(updater)                                                                     \
    if (updaterName == updater::UPDATER_NAME)                                                     \
    {                                                                                             \
        ret.reset(new updater);                                                                   \
        return ret;                                                                               \
    }

    ENUM_UPDATER(MaxMinSegmentMetricsUpdater);
    ENUM_UPDATER(LifecycleSegmentMetricsUpdater);
    ENUM_UPDATER(TimeSeriesSegmentMetricsUpdater);
    return std::unique_ptr<SegmentMetricsUpdater>();

#undef ENUM_UPDATER
}

std::unique_ptr<index::SegmentMetricsUpdater> BuildinSegmentMetricsUpdaterFactory::CreateUpdater(
    const std::string& className, const util::KeyValueMap& parameters,
    const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options)
{
    if (className.empty())
    {
        IE_LOG(WARN, "segment attribute updater name should not empty");
        return std::unique_ptr<SegmentMetricsUpdater>();
    }
    auto updater = DoCreate(className);
    if (!updater.get())
    {
        INDEXLIB_FATAL_ERROR(Runtime, "create updater [%s] failed", className.c_str());
    }
    if (!updater->Init(schema, options, parameters))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "init updater [%s] failed", className.c_str());
    }
    return updater;
}

std::unique_ptr<index::SegmentMetricsUpdater> BuildinSegmentMetricsUpdaterFactory::CreateUpdaterForMerge(
    const std::string& className, const util::KeyValueMap& parameters,
    const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders)
{
    if (className.empty())
    {
        return std::unique_ptr<SegmentMetricsUpdater>();
    }
    auto updater = DoCreate(className);
    if (!updater.get())
    {
        INDEXLIB_FATAL_ERROR(Runtime, "create updater [%s] failed", className.c_str());
    }
    if (!updater->InitForMerge(schema, options, segMergeInfos, attrReaders, parameters))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "init updater [%s] failed", className.c_str());        
    }
    return updater;    
}

IE_NAMESPACE_END(index);

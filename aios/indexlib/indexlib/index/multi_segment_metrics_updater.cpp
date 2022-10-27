#include "indexlib/index/multi_segment_metrics_updater.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/misc/exception.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/plugin_factory_loader.h"
#include "indexlib/index/buildin_segment_metrics_updater_factory.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiSegmentMetricsUpdater);

MultiSegmentMetricsUpdater::MultiSegmentMetricsUpdater() 
{
}

MultiSegmentMetricsUpdater::~MultiSegmentMetricsUpdater() 
{
}

void MultiSegmentMetricsUpdater::Init(const config::IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options,
    const vector<config::ModuleClassConfig> updaterConfigs,
    const plugin::PluginManagerPtr& pluginManager)
{
    mSegMetricsUpdaters.clear();
    mSegMetricsUpdaters.reserve(updaterConfigs.size());
    for (auto& updaterConfig : updaterConfigs)
    {
        auto factory = plugin::PluginFactoryLoader::GetFactory<SegmentMetricsUpdaterFactory,
            BuildinSegmentMetricsUpdaterFactory>(updaterConfig.moduleName,
            SegmentMetricsUpdaterFactory::SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX, pluginManager);
        if (!factory)
        {
            INDEXLIB_FATAL_ERROR(
                Runtime, "get factory failed for updater module [%s]", updaterConfig.moduleName.c_str());
        }
        auto updater = factory->CreateUpdater(
            updaterConfig.className, updaterConfig.parameters, schema, options);
        if (!updater)
        {
            IE_LOG(WARN, "no segment attribute updater created for [%s]",
                   updaterConfig.className.c_str());
        }
        else
        {
            IE_LOG(
                INFO, "segment attribute updater [%s] created", updaterConfig.className.c_str());
            mSegMetricsUpdaters.push_back(std::move(updater));
        }
    }
}

void MultiSegmentMetricsUpdater::InitForMerge(const config::IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
    const vector<config::ModuleClassConfig> updaterConfigs,
    const plugin::PluginManagerPtr& pluginManager)
{
    mSegMetricsUpdaters.clear();
    mSegMetricsUpdaters.reserve(updaterConfigs.size());
    for (auto& updaterConfig : updaterConfigs)
    {
        auto factory = plugin::PluginFactoryLoader::GetFactory<SegmentMetricsUpdaterFactory,
            BuildinSegmentMetricsUpdaterFactory>(updaterConfig.moduleName,
            SegmentMetricsUpdaterFactory::SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX, pluginManager);
        if (!factory)
        {
            INDEXLIB_FATAL_ERROR(Runtime, "get factory failed for merge updater module [%s]",
                updaterConfig.moduleName.c_str());
        }
        auto updater = factory->CreateUpdaterForMerge(updaterConfig.className,
            updaterConfig.parameters, schema, options, segMergeInfos, attrReaders);
        if (!updater)
        {
            IE_LOG(WARN, "no merge segment attribute updater created for [%s]",
                   updaterConfig.className.c_str());
        }
        else
        {
            IE_LOG(
                INFO, "merge segment attribute updater [%s] created", updaterConfig.className.c_str());
            mSegMetricsUpdaters.push_back(std::move(updater));
        }
    }
}

void MultiSegmentMetricsUpdater::Update(const document::DocumentPtr& doc)
{
    for (auto& updater : mSegMetricsUpdaters)
    {
        updater->Update(doc);
    }
}
void MultiSegmentMetricsUpdater::UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId)
{
    for (auto& updater : mSegMetricsUpdaters)
    {
        updater->UpdateForMerge(oldSegId, oldLocalId);
    }
}
autil::legacy::json::JsonMap MultiSegmentMetricsUpdater::Dump() const
{
    json::JsonMap jsonMap;
    for (auto& updater : mSegMetricsUpdaters)
    {
        const auto& dump = updater->Dump();
        jsonMap.insert(dump.begin(), dump.end());
    }
    return jsonMap;
}

IE_NAMESPACE_END(index);


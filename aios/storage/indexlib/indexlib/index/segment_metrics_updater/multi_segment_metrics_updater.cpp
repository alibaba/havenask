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
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"

#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/segment_metrics_updater/buildin_segment_metrics_updater_factory.h"
#include "indexlib/plugin/plugin_factory_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::plugin;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiSegmentMetricsUpdater);

const string MultiSegmentMetricsUpdater::UPDATER_NAME = "multi";

MultiSegmentMetricsUpdater::~MultiSegmentMetricsUpdater() {}

void MultiSegmentMetricsUpdater::Init(const config::IndexPartitionSchemaPtr& schema,
                                      const config::IndexPartitionOptions& options,
                                      const vector<config::ModuleClassConfig> updaterConfigs,
                                      const plugin::PluginManagerPtr& pluginManager)
{
    if (options.IsOnline()) {
        return;
    }
    mSegMetricsUpdaters.clear();
    vector<config::ModuleClassConfig> tempConfigs = updaterConfigs;
    if (schema->EnableTemperatureLayer()) {
        config::ModuleClassConfig emptyConfig;
        tempConfigs.push_back(emptyConfig);
    }
    for (auto& updaterConfig : tempConfigs) {
        auto factory =
            plugin::PluginFactoryLoader::GetFactory<SegmentMetricsUpdaterFactory, BuildinSegmentMetricsUpdaterFactory>(
                updaterConfig.moduleName, SegmentMetricsUpdaterFactory::SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX,
                pluginManager);
        if (!factory) {
            INDEXLIB_FATAL_ERROR(Runtime, "get factory failed for updater module [%s]",
                                 updaterConfig.moduleName.c_str());
        }
        auto updater =
            factory->CreateUpdater(updaterConfig.className, updaterConfig.parameters, schema, options, mMetricProvider);
        if (!updater) {
            IE_LOG(WARN, "no segment attribute updater created for [%s]", updaterConfig.className.c_str());
        } else {
            IE_LOG(INFO, "segment attribute updater [%s] created", updaterConfig.className.c_str());
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
    if (options.IsOnline()) {
        return;
    }
    mSegMetricsUpdaters.clear();
    vector<config::ModuleClassConfig> tempConfigs = updaterConfigs;
    if (schema->EnableTemperatureLayer()) {
        config::ModuleClassConfig emptyConfig;
        tempConfigs.push_back(emptyConfig);
    }

    for (auto& updaterConfig : tempConfigs) {
        auto factory =
            plugin::PluginFactoryLoader::GetFactory<SegmentMetricsUpdaterFactory, BuildinSegmentMetricsUpdaterFactory>(
                updaterConfig.moduleName, SegmentMetricsUpdaterFactory::SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX,
                pluginManager);
        if (!factory) {
            INDEXLIB_FATAL_ERROR(Runtime, "get factory failed for merge updater module [%s]",
                                 updaterConfig.moduleName.c_str());
        }
        auto updater = factory->CreateUpdaterForMerge(updaterConfig.className, updaterConfig.parameters, schema,
                                                      options, segMergeInfos, attrReaders, mMetricProvider);
        if (!updater) {
            IE_LOG(WARN, "no merge segment attribute updater created for [%s]", updaterConfig.className.c_str());
        } else {
            IE_LOG(INFO, "merge segment attribute updater [%s] created", updaterConfig.className.c_str());
            mSegMetricsUpdaters.push_back(std::move(updater));
        }
    }
}

bool MultiSegmentMetricsUpdater::InitForReCalculator(
    const config::IndexPartitionSchemaPtr& schema, const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
    const index::DeletionMapReaderPtr& deleteMapReader, const std::vector<config::ModuleClassConfig> updaterConfigs,
    const index_base::SegmentMergeInfo& segMergeInfo, segmentid_t segmentId,
    const plugin::PluginManagerPtr& pluginManager)
{
    mSegMetricsUpdaters.clear();
    vector<config::ModuleClassConfig> tempConfigs = updaterConfigs;
    if (schema->EnableTemperatureLayer()) {
        config::ModuleClassConfig emptyConfig;
        tempConfigs.push_back(emptyConfig);
    }

    for (auto& updaterConfig : tempConfigs) {
        auto factory =
            plugin::PluginFactoryLoader::GetFactory<SegmentMetricsUpdaterFactory, BuildinSegmentMetricsUpdaterFactory>(
                updaterConfig.moduleName, SegmentMetricsUpdaterFactory::SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX,
                pluginManager);
        if (!factory) {
            INDEXLIB_FATAL_ERROR(Runtime, "get factory failed for merge updater module [%s]",
                                 updaterConfig.moduleName.c_str());
        }
        auto updater = factory->CreateUpdaterForReCalculator(updaterConfig.className, updaterConfig.parameters, schema,
                                                             segMergeInfo, attrReaders, deleteMapReader, segmentId,
                                                             mMetricProvider);
        if (!updater) {
            IE_LOG(WARN, "no merge segment attribute updater created for [%s]", updaterConfig.className.c_str());
            return false;
        } else {
            IE_LOG(INFO, "merge segment attribute updater [%s] created", updaterConfig.className.c_str());
            mSegMetricsUpdaters.push_back(std::move(updater));
        }
    }
    return true;
}

void MultiSegmentMetricsUpdater::UpdateForCaculator(const string& checkpointDir,
                                                    file_system::FenceContext* fenceContext)
{
    for (auto& updater : mSegMetricsUpdaters) {
        updater->UpdateForCaculator(checkpointDir, fenceContext);
    }
}

bool MultiSegmentMetricsUpdater::UpdateSegmentMetric(framework::SegmentMetrics& segMetric)
{
    bool ret = true;
    for (auto& updater : mSegMetricsUpdaters) {
        if (!updater->UpdateSegmentMetric(segMetric)) {
            ret = false;
        }
    }
    return ret;
}

SegmentMetricsUpdaterPtr MultiSegmentMetricsUpdater::GetMetricUpdate(const std::string& name)
{
    for (auto updater : mSegMetricsUpdaters) {
        if (updater->GetUpdaterName() == name) {
            return updater;
        }
    }
    return nullptr;
}

void MultiSegmentMetricsUpdater::Update(const document::DocumentPtr& doc)
{
    for (auto& updater : mSegMetricsUpdaters) {
        updater->Update(doc);
    }
}
void MultiSegmentMetricsUpdater::UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue)
{
    for (auto& updater : mSegMetricsUpdaters) {
        updater->UpdateForMerge(oldSegId, oldLocalId, hintValue);
    }
}

void MultiSegmentMetricsUpdater::ReportMetrics()
{
    for (auto& updater : mSegMetricsUpdaters) {
        updater->ReportMetrics();
    }
}

int64_t MultiSegmentMetricsUpdater::GetSegmentMetricUpdaterSize() const { return mSegMetricsUpdaters.size(); }

autil::legacy::json::JsonMap MultiSegmentMetricsUpdater::Dump() const
{
    json::JsonMap jsonMap;
    for (auto& updater : mSegMetricsUpdaters) {
        const auto& dump = updater->Dump();
        jsonMap.insert(dump.begin(), dump.end());
    }
    return jsonMap;
}
}} // namespace indexlib::index

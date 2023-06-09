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
#include "indexlib/index/segment_metrics_updater/buildin_segment_metrics_updater_factory.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/module_class_config.h"
#include "indexlib/index/segment_metrics_updater/lifecycle_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/max_min_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/temperature_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/time_series_segment_metrics_updater.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, BuildinSegmentMetricsUpdaterFactory);

BuildinSegmentMetricsUpdaterFactory::BuildinSegmentMetricsUpdaterFactory() {}

BuildinSegmentMetricsUpdaterFactory::~BuildinSegmentMetricsUpdaterFactory() {}

index::SegmentMetricsUpdaterPtr BuildinSegmentMetricsUpdaterFactory::DoCreate(const std::string& updaterName,
                                                                              const util::MetricProviderPtr& metrics)
{
    SegmentMetricsUpdaterPtr ret;
#define ENUM_UPDATER(updater)                                                                                          \
    if (updaterName == updater::UPDATER_NAME) {                                                                        \
        ret.reset(new updater(metrics));                                                                               \
        return ret;                                                                                                    \
    }

    ENUM_UPDATER(MaxMinSegmentMetricsUpdater);
    ENUM_UPDATER(LifecycleSegmentMetricsUpdater);
    ENUM_UPDATER(TimeSeriesSegmentMetricsUpdater);
    ENUM_UPDATER(TemperatureSegmentMetricsUpdater);
    return std::unique_ptr<SegmentMetricsUpdater>();

#undef ENUM_UPDATER
}

index::SegmentMetricsUpdaterPtr BuildinSegmentMetricsUpdaterFactory::CreateUpdater(
    const std::string& className, const util::KeyValueMap& parameters, const IndexPartitionSchemaPtr& schema,
    const IndexPartitionOptions& options, const util::MetricProviderPtr& metrics)
{
    string finalClassName = className;
    if (schema->EnableTemperatureLayer() && className.empty()) {
        finalClassName = TemperatureSegmentMetricsUpdater::UPDATER_NAME;
    }
    if (finalClassName.empty()) {
        IE_LOG(WARN, "segment attribute updater name should not empty");
        return std::unique_ptr<SegmentMetricsUpdater>();
    }
    auto updater = DoCreate(finalClassName, metrics);
    if (!updater.get()) {
        INDEXLIB_FATAL_ERROR(Runtime, "create updater [%s] failed", finalClassName.c_str());
    }
    if (!updater->Init(schema, options, parameters)) {
        INDEXLIB_FATAL_ERROR(Runtime, "init updater [%s] failed", finalClassName.c_str());
    }
    return updater;
}

index::SegmentMetricsUpdaterPtr BuildinSegmentMetricsUpdaterFactory::CreateUpdaterForReCalculator(
    const std::string& className, const util::KeyValueMap& parameters, const config::IndexPartitionSchemaPtr& schema,
    const index_base::SegmentMergeInfo& segMergeInfo,
    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
    const index::DeletionMapReaderPtr& deleteMapReader, segmentid_t segmentId, const util::MetricProviderPtr& metrics)
{
    string finalClassName = className;
    if (schema->EnableTemperatureLayer() && className.empty()) {
        finalClassName = TemperatureSegmentMetricsUpdater::UPDATER_NAME;
    }
    if (finalClassName.empty()) {
        IE_LOG(WARN, "segment attribute updater name should not empty");
        return std::unique_ptr<SegmentMetricsUpdater>();
    }
    auto updater = DoCreate(finalClassName, metrics);
    if (!updater.get()) {
        INDEXLIB_FATAL_ERROR(Runtime, "create updater [%s] failed", finalClassName.c_str());
    }
    if (!updater->InitForReCaculator(schema, segMergeInfo, attrReaders, deleteMapReader, segmentId, parameters)) {
        INDEXLIB_FATAL_ERROR(Runtime, "init updater [%s] failed", finalClassName.c_str());
    }
    return updater;
}

index::SegmentMetricsUpdaterPtr BuildinSegmentMetricsUpdaterFactory::CreateUpdaterForMerge(
    const std::string& className, const util::KeyValueMap& parameters, const IndexPartitionSchemaPtr& schema,
    const IndexPartitionOptions& options, const index_base::SegmentMergeInfos& segMergeInfos,
    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders, const util::MetricProviderPtr& metrics)
{
    string finalClassName = className;
    if (schema->EnableTemperatureLayer() && className.empty()) {
        finalClassName = TemperatureSegmentMetricsUpdater::UPDATER_NAME;
    }
    if (finalClassName.empty()) {
        return std::unique_ptr<SegmentMetricsUpdater>();
    }
    auto updater = DoCreate(finalClassName, metrics);
    if (!updater.get()) {
        INDEXLIB_FATAL_ERROR(Runtime, "create updater [%s] failed", finalClassName.c_str());
    }
    if (!updater->InitForMerge(schema, options, segMergeInfos, attrReaders, parameters)) {
        INDEXLIB_FATAL_ERROR(Runtime, "init updater [%s] failed", finalClassName.c_str());
    }
    return updater;
}
}} // namespace indexlib::index

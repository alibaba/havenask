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
#include "indexlib/index/segment_metrics_updater/temperature_segment_metrics_updater.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "beeper/beeper.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/FenceDirectory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace autil::legacy;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TemperatureSegmentMetricsUpdater);

const string TemperatureSegmentMetricsUpdater::UPDATER_NAME = "temperature";

TemperatureSegmentMetricsUpdater::TemperatureSegmentMetricsUpdater(util::MetricProviderPtr metricProvider)
    : SegmentMetricsUpdater(metricProvider)
    , mHotDocCount(0)
    , mWarmDocCount(0)
    , mColdDocCount(0)
    , mSegmentId(INVALID_SEGMENTID)
    , mTotalDocCount(-1)
    , mOldTemperature(TemperatureProperty::UNKNOWN)
    , mHasCalculator(false)
{
    IE_INIT_METRIC_GROUP(mMetricProvider, SegmentTemperatureProperty, "temperature/segmentProperty", kmonitor::GAUGE,
                         "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, DocTemperatureProperty, "temperature/docProperty", kmonitor::GAUGE, "count");
}

TemperatureSegmentMetricsUpdater::~TemperatureSegmentMetricsUpdater() {}

bool TemperatureSegmentMetricsUpdater::Init(const config::IndexPartitionSchemaPtr& schema,
                                            const config::IndexPartitionOptions& options,
                                            const util::KeyValueMap& parameters)
{
    return mEvaluator.Init(schema->GetAttributeSchema(), schema->GetTemperatureLayerConfig());
}

bool TemperatureSegmentMetricsUpdater::InitForMerge(const config::IndexPartitionSchemaPtr& schema,
                                                    const config::IndexPartitionOptions& options,
                                                    const index_base::SegmentMergeInfos& segMergeInfos,
                                                    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                                                    const util::KeyValueMap& parameters)
{
    return mEvaluator.InitForMerge(schema->GetAttributeSchema(), schema->GetTemperatureLayerConfig(), attrReaders);
}

bool TemperatureSegmentMetricsUpdater::InitForReCaculator(
    const config::IndexPartitionSchemaPtr& schema, const index_base::SegmentMergeInfo& segMergeInfo,
    const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
    const index::DeletionMapReaderPtr& deletionMapReader, segmentid_t segmentId, const util::KeyValueMap& parameters)
{
    mSegmentId = segmentId;
    mTotalDocCount = segMergeInfo.segmentInfo.docCount;
    mDeletionReader = deletionMapReader;
    InitDocDetail((uint32_t)mTotalDocCount);
    auto groupMetrics = segMergeInfo.segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    assert(groupMetrics);
    json::JsonMap temperature = groupMetrics->Get<json::JsonMap>(TEMPERATURE_SEGMENT_METIRC_KRY);
    mOldTemperature = StringToTemperatureProperty(AnyCast<string>(temperature[LIFECYCLE]));
    return mEvaluator.InitForMerge(schema->GetAttributeSchema(), schema->GetTemperatureLayerConfig(), attrReaders);
}

void TemperatureSegmentMetricsUpdater::InitDocDetail(uint32_t docCount)
{
    for (size_t i = 0; i < DOC_TYPE; i++) {
        BitmapPtr temperatureBitmap(new Bitmap(docCount));
        mDocDetail.push_back(temperatureBitmap);
    }
}

TemperatureProperty TemperatureSegmentMetricsUpdater::GetDocTemperatureProperty(segmentid_t oldSegId,
                                                                                docid_t oldLocalId)
{
    if (mOldTemperature == TemperatureProperty::COLD) {
        return TemperatureProperty::COLD;
    }
    size_t i = 0;
    if (mHasCalculator) {
        for (i = 0; i < mDocDetail.size(); i++) {
            if (mDocDetail[i]->Test(oldLocalId)) {
                break;
            }
        }
        if (i == 0) {
            return TemperatureProperty::HOT;
        } else if (i == 1) {
            return TemperatureProperty::WARM;
        } else if (i == 2) {
            return TemperatureProperty::COLD;
        }
        return TemperatureProperty::COLD;
    }
    return mEvaluator.Evaluate(oldSegId, oldLocalId);
}

void TemperatureSegmentMetricsUpdater::UpdateForCaculator(const string& checkpointDir, FenceContext* fenceContext)
{
    if (mOldTemperature == TemperatureProperty::COLD) {
        mColdDocCount = mTotalDocCount;
        return;
    }

    bool checkpointExist = LoadFromCheckPoint(checkpointDir);
    if (!checkpointExist) {
        for (size_t i = 0; i < mTotalDocCount; i++) {
            if (mDeletionReader && mDeletionReader->IsDeleted(mSegmentId, i)) {
                continue;
            }
            UpdateForMerge(mSegmentId, i, -1);
        }
    }
    mHotDocCount = mDocDetail[DOC_TYPE_HOT]->GetSetCount();
    mWarmDocCount = mDocDetail[DOC_TYPE_WARM]->GetSetCount();
    mColdDocCount = mDocDetail[DOC_TYPE_COLD]->GetSetCount();
    mHasCalculator = true;
    if (!checkpointExist) {
        StoreCheckpoint(checkpointDir, fenceContext);
    }
}

bool TemperatureSegmentMetricsUpdater::UpdateSegmentMetric(indexlib::framework::SegmentMetrics& segMetric)
{
    auto groupMetrics = segMetric.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    if (!groupMetrics) {
        IE_LOG(WARN, "no customize group found in segment metrics");
        return false;
    }
    json::JsonMap newMetric;
    newMetric[LIFECYCLE] = GenerateLifeCycleTag();
    newMetric[LIFECYCLE_DETAIL] = GenerateSegmentTemperatureStatus();
    segMetric.Set<json::JsonMap>(SEGMENT_CUSTOMIZE_METRICS_GROUP, TEMPERATURE_SEGMENT_METIRC_KRY, newMetric);
    return true;
}

void TemperatureSegmentMetricsUpdater::Update(const document::DocumentPtr& doc)
{
    TemperatureProperty ret = mEvaluator.Evaluate(doc);
    UpdateMetrics(ret);
}

void TemperatureSegmentMetricsUpdater::UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId, int64_t hintValue)
{
    TemperatureProperty hintProperty = Int64ToTemperatureProperty(hintValue);
    if (hintProperty != TemperatureProperty::UNKNOWN) {
        UpdateMetrics(hintProperty, oldLocalId);
        return;
    }
    TemperatureProperty ret = mEvaluator.Evaluate(oldSegId, oldLocalId);
    UpdateMetrics(ret, oldLocalId);
}

void TemperatureSegmentMetricsUpdater::UpdateMetrics(TemperatureProperty type, docid_t localDocId)
{
    size_t i = DOC_TYPE_HOT;
    switch (type) {
    case TemperatureProperty::HOT:
        mHotDocCount++;
        break;
    case TemperatureProperty::WARM:
        i = DOC_TYPE_WARM;
        mWarmDocCount++;
        break;
    case TemperatureProperty::COLD:
        i = DOC_TYPE_COLD;
        mColdDocCount++;
        break;
    default:
        assert(false);
        break;
    }
    if (localDocId != INVALID_DOCID && mDocDetail.size() != 0) {
        mDocDetail[i]->Set(localDocId);
    }
}

string TemperatureSegmentMetricsUpdater::GenerateLifeCycleTag() const
{
    if (mHotDocCount != 0) {
        return "HOT";
    }
    if (mWarmDocCount != 0) {
        return "WARM";
    }
    if (mColdDocCount != 0) {
        return "COLD";
    }
    return "HOT";
}

void TemperatureSegmentMetricsUpdater::GetSegmentTemperatureMeta(index_base::SegmentTemperatureMeta& temperatureMeta)
{
    temperatureMeta.segTemperature = GenerateLifeCycleTag();
    temperatureMeta.segTemperatureDetail = GenerateSegmentTemperatureStatus();
}

TemperatureProperty TemperatureSegmentMetricsUpdater::Int64ToTemperatureProperty(int64_t temperature)
{
    switch (temperature) {
    case TemperatureProperty::HOT:
        return TemperatureProperty::HOT;
    case TemperatureProperty::WARM:
        return TemperatureProperty::WARM;
    case TemperatureProperty::COLD:
        return TemperatureProperty::COLD;
    default:
        return TemperatureProperty::UNKNOWN;
    }
}

TemperatureProperty TemperatureSegmentMetricsUpdater::StringToTemperatureProperty(const std::string& temperature)
{
    if (temperature == "HOT") {
        return TemperatureProperty::HOT;
    } else if (temperature == "WARM") {
        return TemperatureProperty::WARM;
    } else if (temperature == "COLD") {
        return TemperatureProperty::COLD;
    }
    assert(false);
    return TemperatureProperty::UNKNOWN;
}

string TemperatureSegmentMetricsUpdater::GenerateSegmentTemperatureStatus() const
{
    return "HOT:" + StringUtil::toString(mHotDocCount) + ";WARM:" + StringUtil::toString(mWarmDocCount) +
           ";COLD:" + StringUtil::toString(mColdDocCount);
}

// should call after ReCaculator
void TemperatureSegmentMetricsUpdater::ReportMetrics()
{
    MetricsTags tags;
    tags.AddTag("segment", StringUtil::toString(mSegmentId));
    string temperature = GenerateLifeCycleTag();
    TemperatureProperty property = StringToTemperatureProperty(temperature);
    IE_REPORT_METRIC_WITH_TAGS(SegmentTemperatureProperty, &tags, property);
    MetricsTags hotTags, warmTags, coldTags;
    hotTags.AddTag("temperature", "HOT");
    hotTags.AddTag("segment", StringUtil::toString(mSegmentId));
    warmTags.AddTag("temperature", "WARM");
    warmTags.AddTag("segment", StringUtil::toString(mSegmentId));
    coldTags.AddTag("temperature", "COLD");
    coldTags.AddTag("segment", StringUtil::toString(mSegmentId));
    IE_REPORT_METRIC_WITH_TAGS(DocTemperatureProperty, &hotTags, mHotDocCount);
    IE_REPORT_METRIC_WITH_TAGS(DocTemperatureProperty, &warmTags, mWarmDocCount);
    IE_REPORT_METRIC_WITH_TAGS(DocTemperatureProperty, &coldTags, mColdDocCount);

    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                      "segment [%d] temperature is [%s], detail [%s]", mSegmentId, temperature.c_str(),
                                      GenerateSegmentTemperatureStatus().c_str());
}

autil::legacy::json::JsonMap TemperatureSegmentMetricsUpdater::Dump() const
{
    json::JsonMap jsonMap;
    jsonMap[LIFECYCLE] = GenerateLifeCycleTag();
    jsonMap[LIFECYCLE_DETAIL] = GenerateSegmentTemperatureStatus();
    json::JsonMap ret;
    ret[TEMPERATURE_SEGMENT_METIRC_KRY] = jsonMap;
    return ret;
}

string TemperatureSegmentMetricsUpdater::GetCheckpointFilePath(const string& checkpointDir, int32_t docType)
{
    string fileName = "temperature_updater_segment_" + autil::StringUtil::toString(mSegmentId) + "_type_" +
                      autil::StringUtil::toString(docType);
    return util::PathUtil::JoinPath(checkpointDir, fileName);
}

void TemperatureSegmentMetricsUpdater::StoreCheckpoint(const string& checkpointDir, FenceContext* fenceContext)
{
    if (checkpointDir.empty() || autil::EnvUtil::getEnv("INDEXLIB_DISABLE_RECALCULATE_TEMPERATURE_CHECKPOINT", false)) {
        return;
    }
    for (int32_t i = 0; i < mDocDetail.size(); i++) {
        auto bitmap = mDocDetail[i];
        if (bitmap == nullptr) {
            continue;
        }

        string filePath = GetCheckpointFilePath(checkpointDir, i);
        auto ret = file_system::FslibWrapper::AtomicStore(filePath, (const char*)bitmap->GetData(), bitmap->Size(),
                                                          true, fenceContext)
                       .Code();
        if (ret != file_system::FSEC_OK) {
            INDEXLIB_FATAL_ERROR(FileIO, "atomic store [%s] fail.", filePath.c_str());
        }
    }
}

bool TemperatureSegmentMetricsUpdater::LoadFromCheckPoint(const string& checkpointDir)
{
    if (checkpointDir.empty() || autil::EnvUtil::getEnv("INDEXLIB_DISABLE_RECALCULATE_TEMPERATURE_CHECKPOINT", false)) {
        return false;
    }

    for (int32_t i = 0; i < mDocDetail.size(); i++) {
        auto bitmap = mDocDetail[i];
        if (bitmap == nullptr) {
            continue;
        }
        string filePath = GetCheckpointFilePath(checkpointDir, i);
        string content;
        auto ret = file_system::FslibWrapper::AtomicLoad(filePath, content).Code();
        (void)ret;
        if (content.size() != bitmap->Size()) {
            return false;
        }
        bitmap->Copy(0, (uint32_t*)content.data(), mTotalDocCount);
        IE_LOG(INFO, "load checkpoint [%s] success", filePath.c_str());
    }
    return true;
}

}} // namespace indexlib::index

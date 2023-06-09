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
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"

#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentTemperatureMeta);

void SegmentTemperatureMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("segment_id", segmentId, segmentId);
    json.Jsonize("segment_temperature", segTemperature, segTemperature);
    json.Jsonize("segment_temperature_detail", segTemperatureDetail, segTemperatureDetail);
}

void SegmentTemperatureMeta::RewriterTemperatureMetric(indexlib::framework::SegmentMetrics& metrics)
{
    json::JsonMap newMetric;
    newMetric[index::LIFECYCLE] = segTemperature;
    newMetric[index::LIFECYCLE_DETAIL] = segTemperatureDetail;
    metrics.Set<json::JsonMap>(SEGMENT_CUSTOMIZE_METRICS_GROUP, TEMPERATURE_SEGMENT_METIRC_KRY, newMetric);
}

bool SegmentTemperatureMeta::InitFromSegmentMetric(const indexlib::framework::SegmentMetrics& metrics,
                                                   SegmentTemperatureMeta& meta)
{
    json::JsonMap temperatureMetrics;
    if (!metrics.Get<json::JsonMap>(SEGMENT_CUSTOMIZE_METRICS_GROUP, TEMPERATURE_SEGMENT_METIRC_KRY,
                                    temperatureMetrics)) {
        return false;
    }
    meta.segTemperature = AnyCast<string>(temperatureMetrics[index::LIFECYCLE]);
    meta.segTemperatureDetail = AnyCast<string>(temperatureMetrics[index::LIFECYCLE_DETAIL]);
    return true;
}

bool SegmentTemperatureMeta::InitFromSegmentInfo(const indexlibv2::framework::SegmentInfo& segmentInfo,
                                                 SegmentTemperatureMeta& meta)
{
    string value;
    if (segmentInfo.GetDescription(SEGMENT_CUSTOMIZE_METRICS_GROUP, value)) {
        json::JsonMap temperatureMetrics;
        try {
            FromJsonString(temperatureMetrics, value);
        } catch (...) {
            IE_LOG(ERROR, "jsonize metric [%s] failed", value.c_str());
            return false;
        }
        return InitFromJsonMap(temperatureMetrics, meta);
    }
    return false;
}

bool SegmentTemperatureMeta::InitFromJsonMap(const json::JsonMap& map, SegmentTemperatureMeta& meta)
{
    auto iter = map.find(TEMPERATURE_SEGMENT_METIRC_KRY);
    if (iter != map.end()) {
        json::JsonMap temperatureMetrics = AnyCast<json::JsonMap>(iter->second);
        meta.segTemperature = AnyCast<string>(temperatureMetrics[index::LIFECYCLE]);
        meta.segTemperatureDetail = AnyCast<string>(temperatureMetrics[index::LIFECYCLE_DETAIL]);
        return true;
    }
    return false;
}

SegmentTemperatureMeta SegmentTemperatureMeta::GenerateDefaultSegmentMeta()
{
    SegmentTemperatureMeta ret;
    ret.segTemperature = "HOT";
    ret.segTemperatureDetail = "HOT:0;WARM:0;COLD:0";
    return ret;
}

autil::legacy::json::JsonMap SegmentTemperatureMeta::GenerateCustomizeMetric() const
{
    json::JsonMap jsonMap;
    jsonMap[index::LIFECYCLE] = segTemperature;
    jsonMap[index::LIFECYCLE_DETAIL] = segTemperatureDetail;
    json::JsonMap ret;
    ret[TEMPERATURE_SEGMENT_METIRC_KRY] = jsonMap;
    return ret;
}

SegmentTemperatureMeta SegmentTemperatureMeta::GenerationPureSegmentMeta(segmentid_t segmentId, string temperature,
                                                                         int64_t docCount)
{
    SegmentTemperatureMeta ret;
    ret.segmentId = segmentId;
    ret.segTemperature = temperature;
    if (temperature == "HOT") {
        ret.segTemperatureDetail = "HOT:" + StringUtil::toString(docCount) + ";WARM:0;COLD:0";
    } else if (temperature == "WARM") {
        ret.segTemperatureDetail = "HOT:0;WARM:" + StringUtil::toString(docCount) + ";COLD:0";
    } else {
        ret.segTemperatureDetail = "HOT:0;WARM:0;COLD:" + StringUtil::toString(docCount);
    }
    return ret;
}
}} // namespace indexlib::index_base

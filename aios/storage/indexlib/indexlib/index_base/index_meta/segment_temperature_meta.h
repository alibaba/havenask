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
#ifndef __INDEXLIB_SEGMENT_TEMPERATURE_META_H
#define __INDEXLIB_SEGMENT_TEMPERATURE_META_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::framework {
class SegmentInfo;
}
namespace indexlib { namespace index_base {

/*
{
    "segment_temperature_metas" :
    [
        {
          "segment_id" : 1,
          "segment_temperature" : "HOT",
          "segment_temperature_detail" : "HOT:10;WARM:2;COLD:3"
        },
        {
          "segment_id" : 2,
          "segment_temperature" : "WARM",
          "segment_temperature_detail" : "HOT:0;WARM:2;COLD:3"
        }
    ]
}

 */

class SegmentTemperatureMeta : public autil::legacy::Jsonizable
{
public:
    SegmentTemperatureMeta() : segmentId(INVALID_SEGMENTID) {}
    ~SegmentTemperatureMeta() {}

    SegmentTemperatureMeta(const SegmentTemperatureMeta& other)
        : segmentId(other.segmentId)
        , segTemperature(other.segTemperature)
        , segTemperatureDetail(other.segTemperatureDetail)
    {
    }

    SegmentTemperatureMeta(segmentid_t segmentIdInput, const std::string& segTemperatureInput,
                           const std::string& segTemperatureDetailInput)
        : segmentId(segmentIdInput)
        , segTemperature(segTemperatureInput)
        , segTemperatureDetail(segTemperatureDetailInput)
    {
    }

public:
    void Jsonize(JsonWrapper& json) override;
    void RewriterTemperatureMetric(indexlib::framework::SegmentMetrics& metrics);
    static SegmentTemperatureMeta GenerateDefaultSegmentMeta();
    static bool InitFromSegmentMetric(const indexlib::framework::SegmentMetrics& metrics, SegmentTemperatureMeta& meta);
    static bool InitFromSegmentInfo(const indexlibv2::framework::SegmentInfo& segmentInfo,
                                    SegmentTemperatureMeta& meta);
    static bool InitFromJsonMap(const autil::legacy::json::JsonMap& map, SegmentTemperatureMeta& meta);
    static SegmentTemperatureMeta GenerationPureSegmentMeta(segmentid_t segmentId, std::string temperature,
                                                            int64_t docCount);
    autil::legacy::json::JsonMap GenerateCustomizeMetric() const;

public:
    bool operator<(const SegmentTemperatureMeta& other) const { return segmentId < other.segmentId; }
    bool operator==(const SegmentTemperatureMeta& other) const
    {
        return segmentId == other.segmentId && segTemperature == other.segTemperature &&
               segTemperatureDetail == other.segTemperatureDetail;
    }
    void operator=(const SegmentTemperatureMeta& other)
    {
        segmentId = other.segmentId;
        segTemperature = other.segTemperature;
        segTemperatureDetail = other.segTemperatureDetail;
    }

    segmentid_t GetSegmentId() const { return segmentId; }
    const std::string& GetSegmentTemperature() const { return segTemperature; }

public:
    segmentid_t segmentId;
    std::string segTemperature;
    std::string segTemperatureDetail;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentTemperatureMeta);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_SEGMENT_TEMPERATURE_META_H

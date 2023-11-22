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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/segment/segment_data.h"

namespace indexlib { namespace index {

class SegmentFileCompressUtil
{
public:
    SegmentFileCompressUtil() {}
    ~SegmentFileCompressUtil() {}

    SegmentFileCompressUtil(const SegmentFileCompressUtil&) = delete;
    SegmentFileCompressUtil& operator=(const SegmentFileCompressUtil&) = delete;
    SegmentFileCompressUtil(SegmentFileCompressUtil&&) = delete;
    SegmentFileCompressUtil& operator=(SegmentFileCompressUtil&&) = delete;

public:
    static bool SupportFileCompress(const std::shared_ptr<config::FileCompressConfig>& compressConfig,
                                    const indexlibv2::framework::SegmentInfo& segmentInfo)
    {
        if (!compressConfig) {
            return false;
        }

        std::string compressFingerPrintStr;
        if (!segmentInfo.GetDescription(SEGMENT_COMPRESS_FINGER_PRINT, compressFingerPrintStr)) {
            return true;
        }
        uint64_t compressFingerPrint;
        if (!autil::StringUtil::fromString(compressFingerPrintStr, compressFingerPrint)) {
            AUTIL_LOG(ERROR, "invalid compress_finger_print [%s]", compressFingerPrintStr.c_str());
            return true;
        }
        if (compressFingerPrint != compressConfig->GetGlobalFingerPrint()) {
            // file compress info changed
            return true;
        }

        std::string temperatureStr;
        index_base::SegmentTemperatureMeta meta;
        if (index_base::SegmentTemperatureMeta::InitFromSegmentInfo(segmentInfo, meta)) {
            temperatureStr = meta.segTemperature;
        }
        auto compressType = compressConfig->GetCompressType(temperatureStr);
        return !compressType.empty();
    }
    static bool SupportFileCompress(const std::shared_ptr<config::FileCompressConfig>& compressConfig,
                                    const index_base::SegmentInfo& segInfo)
    {
        if (!compressConfig) {
            return false;
        }

        uint64_t compressFingerPrint;
        if (!segInfo.GetCompressFingerPrint(compressFingerPrint) ||
            compressFingerPrint != compressConfig->GetGlobalFingerPrint()) {
            // file compress info changed
            return true;
        }

        std::string temperatureStr;
        index_base::SegmentTemperatureMeta meta;
        if (segInfo.GetOriginalTemperatureMeta(meta)) {
            temperatureStr = meta.segTemperature;
        }
        auto compressType = compressConfig->GetCompressType(temperatureStr);
        return !compressType.empty();
    }

    static bool SupportFileCompress(const std::shared_ptr<config::FileCompressConfig>& compressConfig,
                                    const index_base::SegmentData& segData)
    {
        if (!compressConfig) {
            return false;
        }

        index_base::SegmentInfo segInfo = *segData.GetSegmentInfo();
        uint64_t compressFingerPrint;
        if (!segInfo.GetCompressFingerPrint(compressFingerPrint) ||
            compressFingerPrint != compressConfig->GetGlobalFingerPrint()) {
            // file compress info changed
            return true;
        }

        auto compressType = compressConfig->GetCompressType(segData.GetOriginalTemperature());
        return !compressType.empty();
    }

    static bool AttributeSupportFileCompress(const config::AttributeConfigPtr& attrConfig,
                                             const index_base::SegmentData& segData)
    {
        return SupportFileCompress(attrConfig->GetFileCompressConfig(), segData);
    }

    static bool IndexSupportFileCompress(const config::IndexConfigPtr& indexConfig,
                                         const index_base::SegmentData& segData)
    {
        return SupportFileCompress(indexConfig->GetFileCompressConfig(), segData);
    }

private:
    AUTIL_LOG_DECLARE();
};
inline AUTIL_LOG_SETUP(indexlib.index, SegmentFileCompressUtil);
DEFINE_SHARED_PTR(SegmentFileCompressUtil);

}} // namespace indexlib::index

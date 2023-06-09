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

#include <cstdint>
#include <string>

namespace indexlib::framework {
class SegmentMetrics;
}

namespace indexlibv2::index {

struct KKVSegmentStatistics {
    size_t totalMemoryUse = 0;

    size_t pkeyCount = 0;
    size_t pkeyMemoryUse = 0;
    double pkeyMemoryRatio = 0; // pkeyMemory / totalMemory

    size_t skeyCount = 0;
    size_t maxSKeyCount = 0;
    size_t skeyMemoryUse = 0;
    double skeyCompressRatio = 1.0;
    double skeyValueMemoryRatio = 0; // skeyMemory / (skeyMemory + valueMemory)

    size_t maxValueLen = 0;
    size_t valueMemoryUse = 0;
    double valueCompressRatio = 1.0; // TODO(xinfei.sxf) this value is wrong in v1

    void Store(indexlib::framework::SegmentMetrics* metrics, const std::string& groupName) const;
    bool Load(const indexlib::framework::SegmentMetrics* metrics, const std::string& groupName);
    static std::string GetTargetGroupNameLegacy(const std::string& groupName,
                                                const indexlib::framework::SegmentMetrics* metrics);

public:
    static constexpr const char KKV_SEGMENT_MEM_USE[] = "kkv_segment_mem_use";
    static constexpr const char KKV_PKEY_MEM_USE[] = "kkv_pkey_mem_use";
    static constexpr const char KKV_PKEY_MEM_RATIO[] = "kkv_pkey_mem_ratio";
    static constexpr const char KKV_PKEY_COUNT[] = "pkey_count";
    static constexpr const char KKV_SKEY_COUNT[] = "skey_count";
    static constexpr const char KKV_MAX_VALUE_LEN[] = "max_value_len";
    static constexpr const char KKV_MAX_SKEY_COUNT[] = "max_skey_count";
    static constexpr const char KKV_SKEY_COMPRESS_RATIO_NAME[] = "kkv_skey_compress_ratio";
    static constexpr const char KKV_VALUE_COMPRESS_RATIO_NAME[] = "kkv_value_compress_ratio";
    static constexpr const char KKV_SKEY_MEM_USE[] = "kkv_skey_mem_use";
    static constexpr const char KKV_VALUE_MEM_USE[] = "kkv_value_mem_use";
    static constexpr const char KKV_SKEY_VALUE_MEM_RATIO[] = "kkv_skey_value_mem_ratio";
};

} // namespace indexlibv2::index

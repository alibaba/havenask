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

struct SegmentStatistics {
    size_t totalMemoryUse = 0;
    size_t keyMemoryUse = 0;
    size_t valueMemoryUse = 0;
    size_t keyCount = 0;
    size_t deletedKeyCount = 0;
    int32_t occupancyPct = 0;
    float keyValueSizeRatio = 0;
    float valueCompressRatio = 1.0;

    void Store(indexlib::framework::SegmentMetrics* metrics, const std::string& groupName) const;
    bool Load(const indexlib::framework::SegmentMetrics* metrics, const std::string& groupName);
    static std::string GetTargetGroupNameLegacy(const std::string& groupName,
                                                const indexlib::framework::SegmentMetrics* metrics);

    static inline const std::string KV_SEGMENT_MEM_USE = "kv_segment_mem_use";
    static inline const std::string KV_KEY_VALUE_MEM_RATIO = "kv_key_value_mem_ratio";
    static inline const std::string KV_HASH_MEM_USE = "kv_hash_mem_use";
    static inline const std::string KV_HASH_OCCUPANCY_PCT = "kv_hash_occupancy_pct";
    static inline const std::string KV_KEY_COUNT = "key_count";
    static inline const std::string KV_KEY_DELETE_COUNT = "kv_key_delete_count";
    static inline const std::string KV_VALUE_MEM_USE = "kv_value_mem_use";
    static inline const std::string KV_VALUE_COMPRESSION_RATIO = "kv_compress_ratio";
};

} // namespace indexlibv2::index

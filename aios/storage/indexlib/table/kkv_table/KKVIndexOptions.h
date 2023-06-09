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

#include "autil/cache/cache.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"

namespace indexlibv2::table {

class KKVIndexOptions
{
public:
    KKVIndexOptions() {}

    virtual ~KKVIndexOptions() {}

public:
    Status Init(std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                const framework::TabletData* tabletData);

    inline uint64_t GetSKeyCountLimits() const __ALWAYS_INLINE
    {
        return (_indexConfig && _indexConfig->NeedSuffixKeyTruncate()) ? _indexConfig->GetSuffixKeyTruncateLimits()
                                                                       : std::numeric_limits<uint32_t>::max();
    }

public:
    uint64_t GetTTL() { return _ttl; }
    int32_t GetFixedValueLen() { return _fixedValueLen; }
    std::shared_ptr<indexlibv2::config::KKVIndexConfig>& GetIndexConfig() { return _indexConfig; }
    const indexlib::config::SortParams& GetSortParams() const { return _sortParams; }
    std::shared_ptr<index::PlainFormatEncoder>& GetPlainFormatEncoder() { return _plainFormatEncoder; }
    autil::CacheBase::Priority GetPriority() { return _cachePriority; }

private:
    bool MatchSortCondition(const framework::TabletData* tabletData) const;

    // TODO(xinfei.sxf) refactor public to private
public:
    uint64_t _ttl = indexlib::DEFAULT_TIME_TO_LIVE;
    int32_t _fixedValueLen = -1;
    autil::CacheBase::Priority _cachePriority = autil::CacheBase::Priority::LOW;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    indexlib::config::SortParams _sortParams;
    std::shared_ptr<index::PlainFormatEncoder> _plainFormatEncoder;
};

} // namespace indexlibv2::table

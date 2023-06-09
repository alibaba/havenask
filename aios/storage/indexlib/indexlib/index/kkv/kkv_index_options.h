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
#ifndef __INDEXLIB_KKV_INDEX_OPTIONS_H
#define __INDEXLIB_KKV_INDEX_OPTIONS_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kv/kv_index_options.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

struct KKVIndexOptions : public KVIndexOptions {
public:
    KKVIndexOptions() {}
    ~KKVIndexOptions() {}

public:
    void Init(const config::KKVIndexConfigPtr& _kkvConfig, const index_base::PartitionDataPtr& partitionData,
              int64_t latestNeedSkipIncTs);

    inline uint64_t GetSKeyCountLimits() const __ALWAYS_INLINE
    {
        return (kkvConfig && kkvConfig->NeedSuffixKeyTruncate()) ? kkvConfig->GetSuffixKeyTruncateLimits()
                                                                 : std::numeric_limits<uint32_t>::max();
    }

private:
    bool MatchSortCondition(const index_base::PartitionDataPtr& partitionData) const;

public:
    config::KKVIndexConfigPtr kkvConfig;
    config::SortParams sortParams;
    config::KKVIndexPreference kkvIndexPreference;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVIndexOptions);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_INDEX_OPTIONS_H

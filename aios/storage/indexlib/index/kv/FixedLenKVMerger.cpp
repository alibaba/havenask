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
#include "indexlib/index/kv/FixedLenKVMerger.h"

#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KeyWriter.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, FixedLenKVMerger);

FixedLenKVMerger::FixedLenKVMerger() {}

FixedLenKVMerger::~FixedLenKVMerger() {}

bool FixedLenKVMerger::NeedCompactBucket(const std::vector<SegmentStatistics>& statVec) const
{
    if (_indexConfig->TTLEnabled()) {
        return false;
    }
    if (!_indexConfig->GetIndexPreference().GetValueParam().IsFixLenAutoInline()) {
        return false;
    }
    int32_t valueSize = _indexConfig->GetValueConfig()->GetFixedLength();
    if (valueSize > 8 || valueSize < 0) {
        return false;
    }
    int32_t keySize = _indexConfig->IsCompactHashKey() ? sizeof(compact_keytype_t) : sizeof(keytype_t);
    if (keySize <= valueSize) {
        return false;
    }
    if (_dropDeleteKey) {
        return true;
    }

    for (auto& stat : statVec) {
        if (stat.deletedKeyCount != 0) {
            return false;
        }
    }

    return true;
}

KVTypeId FixedLenKVMerger::MakeTypeId(const std::vector<SegmentStatistics>& statVec) const
{
    auto typeId = MakeKVTypeId(*_indexConfig, nullptr);
    typeId.shortOffset = false; // disable short offset
    typeId.useCompactBucket = NeedCompactBucket(statVec);
    return typeId;
}

Status FixedLenKVMerger::Dump()
{
    auto s = _keyWriter->Dump(_targetDir, true, _typeId.shortOffset);
    if (!s.IsOK()) {
        return s;
    }
    KVFormatOptions formatOpts;
    formatOpts.SetShortOffset(_typeId.shortOffset);
    formatOpts.SetUseCompactBucket(_typeId.useCompactBucket);
    return formatOpts.Store(_targetDir);
}

void FixedLenKVMerger::FillSegmentMetrics(indexlib::framework::SegmentMetrics* segMetrics)
{
    SegmentStatistics stat;
    _keyWriter->FillStatistics(stat);
    stat.totalMemoryUse = stat.keyMemoryUse;
    stat.keyValueSizeRatio = 1.0f * stat.keyMemoryUse / stat.totalMemoryUse;
    if (segMetrics != nullptr) {
        stat.Store(segMetrics, _indexConfig->GetIndexName());
    }
}

} // namespace indexlibv2::index

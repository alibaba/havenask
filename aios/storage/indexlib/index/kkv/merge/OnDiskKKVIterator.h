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
#include <queue>

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/merge/OnDiskKKVSegmentIterator.h"
#include "indexlib/index/kkv/merge/OnDiskSinglePKeyIterator.h"

namespace indexlibv2::index {

template <typename SKeyType>
class OnDiskKKVIterator
{
public:
    static constexpr size_t RESET_POOL_THRESHOLD = 20 * 1024 * 1024; // 20 Mbytes
public:
    using OnDiskSinglePKeyIteratorTyped = OnDiskSinglePKeyIterator<SKeyType>;
    using OnDiskKKVSegmentIteratorTyped = OnDiskKKVSegmentIterator<SKeyType>;

public:
    OnDiskKKVIterator(const std::shared_ptr<config::KKVIndexConfig>& kkvConfig,
                      const indexlib::file_system::IOConfig& ioConfig = indexlib::file_system::IOConfig());
    ~OnDiskKKVIterator();

public:
    Status Init(const std::vector<IIndexMerger::SourceSegment>& segments);

    void MoveToNext();
    bool IsValid() const { return _curPkeyIterator != nullptr; }
    OnDiskSinglePKeyIteratorTyped* GetCurrentIterator() const { return _curPkeyIterator; }
    size_t GetEstimatePkeyCount() const { return _estimatedPkCount; }

    double GetMergeProgressRatio() const;

    size_t GetPkeyCount(size_t segmentIndex) { return _segIters[segmentIndex]->GetPkeyCount(); }

private:
    bool IsEmptySegment(const std::shared_ptr<framework::Segment>& segment);
    void CreateCurrentPkeyIterator();
    void PushBackToHeap(OnDiskKKVSegmentIteratorTyped* iter);
    size_t DoEstimatePKeyCount(const std::vector<std::unique_ptr<OnDiskKKVSegmentIteratorTyped>>& segIters);
    Status CheckSortSequence(const std::vector<std::unique_ptr<OnDiskKKVSegmentIteratorTyped>>& segIters);

private:
    struct SegIterComparator {
        bool operator()(OnDiskKKVSegmentIteratorTyped*& lft, OnDiskKKVSegmentIteratorTyped*& rht)
        {
            uint64_t lpkey = lft->GetPrefixKey();
            uint64_t rpkey = rht->GetPrefixKey();
            if (lpkey == rpkey) {
                return lft->GetSegmentIdx() < rht->GetSegmentIdx();
            }
            return lpkey > rpkey;
        }
    };

    using SegmentIterHeap = std::priority_queue<OnDiskKKVSegmentIteratorTyped*,
                                                std::vector<OnDiskKKVSegmentIteratorTyped*>, SegIterComparator>;

private:
    autil::mem_pool::Pool _pool;
    indexlib::file_system::IOConfig _ioConfig;
    std::shared_ptr<config::KKVIndexConfig> _kkvConfig;

    SegmentIterHeap _heap;
    std::vector<std::unique_ptr<OnDiskKKVSegmentIteratorTyped>> _segIters;
    OnDiskSinglePKeyIteratorTyped* _curPkeyIterator;
    typename OnDiskSinglePKeyIteratorTyped::MultiSegmentSinglePKeyIterInfo _curPkeySegIterInfo;

    size_t _estimatedPkCount;
    size_t _totalProgress;
    size_t _currentProgress;
};

MARK_EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(OnDiskKKVIterator);

} // namespace indexlibv2::index

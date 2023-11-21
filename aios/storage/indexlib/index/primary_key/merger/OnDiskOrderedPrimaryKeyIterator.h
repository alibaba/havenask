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

#include "autil/Log.h"
#include "indexlib/index/primary_key/IPrimaryKeyIterator.h"
#include "indexlib/index/primary_key/PrimaryKeyDiskIndexer.h"
#include "indexlib/index/primary_key/SegmentDataAdapter.h"

namespace indexlibv2::index {

template <typename Key>
class OnDiskOrderedPrimaryKeyIterator : public IPrimaryKeyIterator<Key>
{
public:
    using PKPairTyped = PKPair<Key, docid64_t>;
    using SegmentPKPairTyped = PKPair<Key, docid32_t>;
    using SegmentIteratorPtr = std::unique_ptr<PrimaryKeyLeafIterator<Key>>;
    using SegmentIteratorPair = std::pair<PrimaryKeyLeafIterator<Key>*, docid64_t>;
    using SegmentIteratorPairVec = std::vector<SegmentIteratorPair>;

public:
    OnDiskOrderedPrimaryKeyIterator() : _isDone(false) {}
    ~OnDiskOrderedPrimaryKeyIterator() {}

public:
    class SegmentIteratorComp
    {
    public:
        // lft.first = SegmentIterator lft.second = segment baseDocid
        bool operator()(const SegmentIteratorPair& lft, const SegmentIteratorPair& rht)
        {
            SegmentPKPairTyped leftPkPair;
            lft.first->GetCurrentPKPair(leftPkPair);

            SegmentPKPairTyped rightPkPair;
            rht.first->GetCurrentPKPair(rightPkPair);

            if (leftPkPair.key == rightPkPair.key) {
                assert(lft.second != rht.second);
                return lft.second > rht.second;
            }
            return leftPkPair.key > rightPkPair.key;
        }
    };
    typedef std::priority_queue<SegmentIteratorPair, SegmentIteratorPairVec, SegmentIteratorComp> Heap;

    [[nodiscard]] bool Init(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                            const std::vector<SegmentDataAdapter::SegmentDataType>& segments) override
    {
        if (indexConfig->GetPrimaryKeyIndexType() == pk_hash_table) {
            assert(false);
            AUTIL_LOG(ERROR, "OnDiskOrderedPrimaryKeyIterator not support hash table");
            return false;
        }
        for (const auto& segment : segments) {
            if (segment._segmentInfo->docCount == 0) {
                continue;
            }
            indexlib::file_system::FileReaderPtr fileReader =
                PrimaryKeyDiskIndexer<Key>::OpenPKDataFile(indexConfig, segment._directory);
            if (!fileReader) {
                AUTIL_LOG(ERROR, "Open PKDataFile for directory [%s] failed",
                          segment._directory->DebugString().c_str());
                return false;
            }
            SegmentIteratorPtr iterator =
                PrimaryKeyDiskIndexer<Key>::CreatePrimaryKeyLeafIterator(indexConfig, fileReader);
            if (!iterator) {
                AUTIL_LOG(ERROR, "Create PK iterator for directory [%s] failed",
                          segment._directory->DebugString().c_str());
                return false;
            }
            if (iterator->HasNext()) {
                _heap.push(std::make_pair(iterator.get(), segment._baseDocId));
            }
            _iterators.push_back(std::move(iterator));
        }
        _isDone = false;
        return Next(_currentPKPair);
    }

    bool HasNext() const override { return !_isDone; }
    [[nodiscard]] bool Next(PKPairTyped& pkPair) override
    {
        pkPair = _currentPKPair;
        if (_heap.empty()) {
            _isDone = true;
            return true;
        }
        auto* segmentIter = _heap.top().first;
        docid64_t baseDocId = _heap.top().second;
        _heap.pop();
        SegmentPKPairTyped segmentPkPair;
        if (!segmentIter->Next(segmentPkPair).IsOK()) {
            return false;
        }
        _currentPKPair.key = segmentPkPair.key;
        _currentPKPair.docid = baseDocId + segmentPkPair.docid;
        if (segmentIter->HasNext()) {
            _heap.push(std::make_pair(segmentIter, baseDocId));
        }
        return true;
    }

private:
    Heap _heap;
    std::vector<SegmentIteratorPtr> _iterators;
    bool _isDone;
    PKPairTyped _currentPKPair;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, OnDiskOrderedPrimaryKeyIterator, T);

} // namespace indexlibv2::index

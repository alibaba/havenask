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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/primary_key/IPrimaryKeyIterator.h"
#include "indexlib/index/primary_key/PrimaryKeyDiskIndexer.h"
#include "indexlib/index/primary_key/PrimaryKeyPair.h"
#include "indexlib/index/primary_key/SegmentDataAdapter.h"

namespace indexlibv2::index {

template <typename Key>
class PrimaryKeyIterator : public IPrimaryKeyIterator<Key>
{
public:
    PrimaryKeyIterator()
        : _totalPkCount(0)
        , _totalDocCount(0)
        , _subIteratorCursor(0)
        , _pkCursor(0)
        , _baseDocId(0)
        , _isDone(false)
    {
    }

    virtual ~PrimaryKeyIterator() {}

public:
    using PKPairTyped = PKPair<Key, docid64_t>;
    using SegmentPKPairTyped = PKPair<Key, docid_t>;

public:
    [[nodiscard]] bool Init(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                            const std::vector<SegmentDataAdapter::SegmentDataType>& segments) override
    {
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
            std::unique_ptr<PrimaryKeyLeafIterator<Key>> iterator =
                PrimaryKeyDiskIndexer<Key>::CreatePrimaryKeyLeafIterator(indexConfig, fileReader);
            if (!iterator) {
                AUTIL_LOG(ERROR, "Create PK iterator for directory [%s] failed",
                          segment._directory->DebugString().c_str());
                return false;
            }
            _subIteratorPairs.emplace_back(GenerateSubIteratorPair(segment, std::move(iterator)));
            _totalPkCount += _subIteratorPairs.rbegin()->first->GetPkCount();
            _totalDocCount += segment._segmentInfo->docCount;
        }
        _isDone = false;
        _subIteratorCursor = 0;
        _baseDocId = _subIteratorPairs.size() > 0 ? _subIteratorPairs[0].second : 0;
        _pkCursor = 0;
        return Next(_currentPKPair);
    }

    bool HasNext() const override { return !_isDone; }
    PKPairTyped GetCurrentPkPair() const { return _currentPKPair; }
    [[nodiscard]] bool Next(PKPairTyped& pkPair) override
    {
        pkPair = _currentPKPair;
        if (_pkCursor >= _totalPkCount) {
            _isDone = true;
            return true;
        }
        while (!_subIteratorPairs[_subIteratorCursor].first->HasNext()) {
            ++_subIteratorCursor;
            _baseDocId = _subIteratorPairs[_subIteratorCursor].second;
        }
        SegmentPKPairTyped segmentPair;
        if (!_subIteratorPairs[_subIteratorCursor].first->Next(segmentPair).IsOK()) {
            AUTIL_LOG(ERROR, "PrimaryKeyIterator do next failed!");
            return false;
        }
        _currentPKPair.key = segmentPair.key;
        _currentPKPair.docid = segmentPair.docid + _baseDocId;
        ++_pkCursor;
        return true;
    }
    uint64_t GetPkCount() const { return _totalPkCount; }
    uint64_t GetDocCount() const { return _totalDocCount; }

protected:
    virtual std::pair<std::unique_ptr<PrimaryKeyLeafIterator<Key>>, docid64_t>
    GenerateSubIteratorPair(const SegmentDataAdapter::SegmentDataType& segment,
                            std::unique_ptr<PrimaryKeyLeafIterator<Key>> leafIterator) const
    {
        return std::make_pair(std::move(leafIterator), _totalDocCount);
    }

private:
    uint64_t _totalPkCount;
    uint64_t _totalDocCount;
    uint64_t _subIteratorCursor;
    uint64_t _pkCursor;
    docid64_t _baseDocId;
    bool _isDone;
    PKPairTyped _currentPKPair;
    std::vector<std::pair<std::unique_ptr<PrimaryKeyLeafIterator<Key>>, docid64_t>> _subIteratorPairs;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyIterator, T);

} // namespace indexlibv2::index

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
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/AttributeIteratorBase.h"

namespace indexlibv2::index {

class SequenceIterator : public AttributeIteratorBase
{
public:
    SequenceIterator(const std::vector<std::shared_ptr<AttributeDiskIndexer>>& diskIndexers,
                     const std::vector<uint64_t>& segmentDocCounts)
        : AttributeIteratorBase(nullptr)
        , _diskIndexers(diskIndexers)
    {
        uint64_t baseDocId = 0;
        for (auto segmentDocCount : segmentDocCounts) {
            _segmentDocIds.push_back({baseDocId, baseDocId + segmentDocCount});
            baseDocId += segmentDocCount;
        }
    }

    ~SequenceIterator() = default;

public:
    void Reset() override { _currentSegmentIdx = 0; }
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> BatchSeek(const std::vector<docid_t>& docIds,
                                                                     indexlib::file_system::ReadOption readOption,
                                                                     std::vector<std::string>* values) noexcept override
    {
        assert(false);
        indexlib::index::ErrorCodeVec ret;
        co_return ret;
    }

    bool Seek(docid_t docId, std::string& attrValue) noexcept override
    {
        for (; _currentSegmentIdx < _segmentDocIds.size(); ++_currentSegmentIdx) {
            auto [baseDocId, endDocId] = _segmentDocIds[_currentSegmentIdx];
            if (docId >= baseDocId && docId < endDocId) {
                return _diskIndexers[_currentSegmentIdx]->Read(docId - baseDocId, &attrValue, /*pool*/ nullptr);
            }
        }
        assert(false);
        return false;
    }

private:
    std::vector<std::shared_ptr<AttributeDiskIndexer>> _diskIndexers;
    int64_t _currentSegmentIdx = 0;
    std::vector<std::pair<docid_t, docid_t>> _segmentDocIds;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index

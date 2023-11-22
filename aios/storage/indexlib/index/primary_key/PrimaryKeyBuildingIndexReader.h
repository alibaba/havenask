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

#include "indexlib/base/Constant.h"
#include "indexlib/util/HashMap.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyBuildingIndexReader
{
public:
    using SegmentReader = util::HashMap<Key, docid_t>;
    using SegmentReaderItem = std::tuple<docid64_t, segmentid_t, std::shared_ptr<const SegmentReader>>;

public:
    PrimaryKeyBuildingIndexReader() {}
    ~PrimaryKeyBuildingIndexReader() {}

public:
    void AddSegmentReader(docid64_t baseDocId, segmentid_t segmentId,
                          const std::shared_ptr<const SegmentReader>& inMemSegReader)
    {
        if (inMemSegReader) {
            mInnerSegReaderItems.push_back(std::make_tuple(baseDocId, segmentId, inMemSegReader));
        }
    }

    docid64_t Lookup(const Key& hashKey) const
    {
        for (auto iter = mInnerSegReaderItems.rbegin(); iter != mInnerSegReaderItems.rend(); ++iter) {
            auto [baseDocId, _, segmentReader] = *iter;
            if (auto docId = segmentReader->Find(hashKey, INVALID_DOCID); docId != INVALID_DOCID) {
                return baseDocId + docId;
            }
        }
        return INVALID_DOCID;
    }

    bool Lookup(const Key& hashKey, segmentid_t specifySegment, docid64_t* docid) const
    {
        for (const auto& [baseDocId, segmentId, segmentReader] : mInnerSegReaderItems) {
            if (segmentId == specifySegment) {
                if (auto localDocId = segmentReader->Find(hashKey, INVALID_DOCID); localDocId != INVALID_DOCID) {
                    *docid = baseDocId + localDocId;
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    const std::vector<SegmentReaderItem>& GetSegReaderItems() const { return mInnerSegReaderItems; }

private:
    std::vector<SegmentReaderItem> mInnerSegReaderItems;

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlib::index

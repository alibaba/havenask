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

#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/util/ObjectPool.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlib::index {
class InvertedIndexSearchTracer;
}

namespace indexlib::index {

class PostingIteratorImpl : public PostingIterator
{
protected:
    using TracerPtr = indexlib::util::PooledUniquePtr<indexlib::index::InvertedIndexSearchTracer>;

public:
    PostingIteratorImpl(autil::mem_pool::Pool* sessionPool, TracerPtr tracer);

    virtual ~PostingIteratorImpl();

private:
    PostingIteratorImpl(const PostingIteratorImpl& other);

public:
    bool Init(const std::shared_ptr<SegmentPostingVector>& segPostings);

public:
    TermMeta* GetTermMeta() const override;
    TermMeta* GetTruncateTermMeta() const override;
    TermPostingInfo GetTermPostingInfo() const override;
    autil::mem_pool::Pool* GetSessionPool() const override;
    bool operator==(const PostingIteratorImpl& right) const;
    size_t GetPostingLength() const override { return GetTotalPostingSize(*mSegmentPostings); }
    indexlib::index::InvertedIndexSearchTracer* GetSearchTracer() const override { return _tracer.get(); }

protected:
    void InitTermMeta();
    static size_t GetTotalPostingSize(const SegmentPostingVector& segmentPostings)
    {
        size_t totalSize = 0;
        for (size_t i = 0; i < segmentPostings.size(); ++i) {
            const SegmentPosting& segPosting = segmentPostings[i];
            util::ByteSlice* singleSlice = segPosting.GetSingleSlice();
            if (singleSlice) {
                totalSize += singleSlice->size;
            } else {
                const util::ByteSliceListPtr& sliceList = segPosting.GetSliceListPtr();
                if (sliceList.get() != nullptr) {
                    totalSize += sliceList->GetTotalSize();
                }
            }
        }
        return totalSize;
    }

protected:
    autil::mem_pool::Pool* _sessionPool;
    TracerPtr _tracer;
    TermMeta _termMeta;
    TermMeta _currentChainTermMeta;
    std::shared_ptr<SegmentPostingVector> mSegmentPostings;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index

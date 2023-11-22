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

#include "indexlib/index/inverted_index/PositionIteratorTyped.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/SingleBitmapPostingIterator.h"

namespace indexlib::index {

class BitmapPostingIterator : public PositionIteratorTyped<SingleBitmapPostingIterator>
{
protected:
    using TracerPtr = indexlib::util::PooledUniquePtr<indexlib::index::InvertedIndexSearchTracer>;

public:
    BitmapPostingIterator(optionflag_t optionFlag = OPTION_FLAG_ALL, autil::mem_pool::Pool* sessionPool = NULL,
                          TracerPtr tracer = nullptr);
    ~BitmapPostingIterator();

public:
    PostingIteratorType GetType() const override { return pi_bitmap; }

    PostingIterator* Clone() const override;
    bool HasPosition() const override { return false; }

public:
    bool Test(docid64_t docId);

public:
    // for test
    docid64_t GetCurrentGlobalDocId() { return _singleIterators[_segmentCursor]->GetCurrentGlobalDocId(); }

protected:
    index::ErrorCode InitSingleIterator(SingleIteratorType* singleIterator) override;

protected:
    docid64_t _curBaseDocId;
    docid64_t _curLastDocId;
    docid64_t _segmentLastDocId;

private:
    friend class BitmapPostingIteratorTest;
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////

inline bool BitmapPostingIterator::Test(docid64_t docId)
{
    while (true) {
        if (unlikely(docId < _curBaseDocId)) {
            return false;
        }
        if (docId < _curLastDocId) {
            return _singleIterators[_segmentCursor]->Test(docId);
        }
        if (docId < _segmentLastDocId) {
            return false;
        }
        if (!MoveToNextSegment().ValueOrThrow()) {
            return false;
        }
    }
    return false;
}
} // namespace indexlib::index

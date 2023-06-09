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

#include <queue>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
namespace indexlib::index {

class SpatialPostingIterator : public PostingIterator
{
public:
    SpatialPostingIterator(autil::mem_pool::Pool* sessionPool);
    ~SpatialPostingIterator();

public:
    index::ErrorCode Init(const std::vector<BufferedPostingIterator*>& postingIterators);

    docid_t SeekDoc(docid_t docid) override;
    ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override { return InnerSeekDoc(docId, result); }
    docid_t InnerSeekDoc(docid_t docId);
    ErrorCode InnerSeekDoc(docid_t docId, docid_t& result);
    bool HasPosition() const override { return false; }
    void Unpack(TermMatchData& termMatchData) override;
    void Reset() override;
    SpatialPostingIterator* Clone() const override;
    PostingIteratorType GetType() const override { return pi_spatial; }
    TermMeta* GetTermMeta() const override { return _termMeta; }
    autil::mem_pool::Pool* GetSessionPool() const override { return _sessionPool; }
    uint32_t GetSeekDocCount() const { return _seekDocCounter; }

private:
    typedef std::pair<PostingIterator*, docid_t> PostingIteratorPair;
    class PostingIteratorComparator
    {
    public:
        bool operator()(const PostingIteratorPair& left, const PostingIteratorPair& right)
        {
            return left.second >= right.second;
        }
    };

    typedef std::priority_queue<PostingIteratorPair, std::vector<PostingIteratorPair>, PostingIteratorComparator>
        PostingIteratorHeap;

private:
    docid_t GetNextDocId(PostingIteratorHeap& heap);
    bool IsValidDocId(docid_t basicDocid, docid_t currentDocid) const;

private:
    // TODO: spatial posting iterator direct manage segment posting
    docid_t _curDocid;
    PostingIteratorHeap _heap;
    std::vector<BufferedPostingIterator*> _postingIterators;
    TermMeta* _termMeta;
    autil::mem_pool::Pool* _sessionPool;
    uint32_t _seekDocCounter;

private:
    AUTIL_LOG_DECLARE();
};
///////////////////////////////////////////////////////////

} // namespace indexlib::index

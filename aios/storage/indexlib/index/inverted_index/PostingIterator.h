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

#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/MatchValue.h"
#include "indexlib/index/inverted_index/TermPostingInfo.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

namespace indexlib::index {

class TermMatchData;
class InvertedIndexSearchTracer;

class PostingIterator
{
public:
    virtual ~PostingIterator() {}

public:
    /**
     * Get term meta information of the posting-list
     * @return internal term meta object
     */
    virtual TermMeta* GetTermMeta() const = 0;

    /**
     * Get truncate term meta information of the posting list
     * @return when no truncate posting, return main chain term meta.
     */
    virtual TermMeta* GetTruncateTermMeta() const { return GetTermMeta(); }

    /**
     * Get match value
     */
    virtual MatchValueType GetMatchValueType() const { return mv_unknown; }

    /**
     * Get match value
     */
    virtual matchvalue_t GetMatchValue() const { return matchvalue_t(); }

    /**
     * Find the docid which is equal to or greater than /docId/ in posting-list
     */
    virtual docid_t SeekDoc(docid_t docId) = 0;

    virtual index::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) = 0;

    /**
     * Return true if position-list exists
     */
    virtual bool HasPosition() const = 0;

    /**
     * Find a in-doc position which is equal to or greater
     * than /pos/ in position-list
     */
    pos_t SeekPosition(pos_t pos)
    {
        pos_t result = INVALID_POSITION;
        auto ec = SeekPositionWithErrorCode(pos, result);
        index::ThrowIfError(ec);
        return result;
    }

    virtual index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextPos)
    {
        nextPos = INVALID_POSITION;
        return index::ErrorCode::OK;
    }

    /**
     * Extract posting data for ranking
     */
    virtual void Unpack(TermMatchData& termMatchData) = 0;

    /**
     * Get doc payload of current sought doc
     */
    virtual docpayload_t GetDocPayload() { return 0; }

    /**
     * Get type
     */
    virtual PostingIteratorType GetType() const { return pi_unknown; }

    virtual index::TermPostingInfo GetTermPostingInfo() const { return index::TermPostingInfo(); }

    virtual void Reset() = 0;

    virtual PostingIterator* Clone() const = 0;

    virtual autil::mem_pool::Pool* GetSessionPool() const { return NULL; }

    virtual size_t GetPostingLength() const { return 0; }

    virtual InvertedIndexSearchTracer* GetSearchTracer() const { return nullptr; }

public:
    index::ErrorCode UnpackWithErrorCode(TermMatchData& termMatchData)
    {
        try {
            Unpack(termMatchData);
            return index::ErrorCode::OK;
        } catch (const util::FileIOException& e) {
            return index::ErrorCode::FileIO;
        }
    }

    index::ErrorCode GetDocPayloadWithErrorCode(docpayload_t& docPayload)
    {
        try {
            docPayload = GetDocPayload();
            return index::ErrorCode::OK;
        } catch (const util::FileIOException& e) {
            return index::ErrorCode::FileIO;
        }
    }

    index::ErrorCode GetTermPostingInfoWithErrorCode(index::TermPostingInfo& termPostingInfo) const
    {
        try {
            termPostingInfo = GetTermPostingInfo();
            return index::ErrorCode::OK;
        } catch (const util::FileIOException& e) {
            return index::ErrorCode::FileIO;
        }
    }
};

} // namespace indexlib::index

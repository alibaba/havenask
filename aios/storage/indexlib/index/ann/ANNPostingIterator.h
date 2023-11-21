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
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/inverted_index/MatchValue.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlibv2::index {
class ANNPostingIterator final : public indexlib::index::PostingIterator, public autil::NoCopyable
{
public:
    ANNPostingIterator(autil::mem_pool::Pool* sessionPool);
    ANNPostingIterator(const std::vector<ANNMatchItem>& annMatchItems, autil::mem_pool::Pool* sessionPool);
    ~ANNPostingIterator();

public:
    indexlib::index::TermMeta* GetTermMeta() const override { return _termMeta; }
    matchvalue_t GetMatchValue() const override { return _curMatchValue; }
    indexlib::MatchValueType GetMatchValueType() const override { return indexlib::MatchValueType::mv_float; }
    docid64_t SeekDoc(docid64_t docId) override;
    indexlib::index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override;
    bool HasPosition() const override { return false; }
    void Unpack(indexlib::index::TermMatchData& termMatchData) override { termMatchData.SetMatched(true); }
    indexlib::index::PostingIteratorType GetType() const override { return indexlib::index::pi_customized; }
    void Reset() override;
    indexlib::index::PostingIterator* Clone() const override;
    autil::mem_pool::Pool* GetSessionPool() const override { return _sessionPool; }

private:
    indexlib::docid64_t* _docIds;
    matchvalue_t* _matchValues;
    size_t _matchCount;
    autil::mem_pool::Pool* _sessionPool;

    int32_t _cursor;
    indexlib::docid64_t _curDocId;
    matchvalue_t _curMatchValue;
    indexlib::index::TermMeta* _termMeta;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index

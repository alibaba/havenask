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

#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingIterator.h"

namespace indexlib::index {

template <typename BufferedIterator>
class CompositePostingIterator final : public PostingIterator
{
public:
    CompositePostingIterator(autil::mem_pool::Pool* sessionPool, BufferedIterator* bufferedIter,
                             DynamicPostingIterator* dynamicIter);
    ~CompositePostingIterator();

    TermMeta* GetTermMeta() const override;
    docid64_t SeekDoc(docid64_t docId) override;
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override;
    bool HasPosition() const override { return false; }
    void Unpack(TermMatchData& termMatchData) override { assert(false); }
    void Reset() override;
    PostingIterator* Clone() const override;
    PostingIteratorType GetType() const override { return pi_composite; }
    autil::mem_pool::Pool* GetSessionPool() const override { return _sessionPool; }
    InvertedIndexSearchTracer* GetSearchTracer() const override
    {
        // TODO: trace dynamic index
        return _bufferedIterator ? _bufferedIterator->GetSearchTracer() : nullptr;
    }

private:
    static constexpr int64_t DOCID_EOF = std::numeric_limits<int64_t>::max();

    index::ErrorCode BufferedAdvance(docid64_t docId);
    void DynamicAdvance(docid64_t docId);

    autil::mem_pool::Pool* _sessionPool;
    BufferedIterator* _bufferedIterator;
    DynamicPostingIterator* _dynamicIterator;
    int64_t _bufferedDocId;
    int64_t _dynamicDocId;
    bool _isDynamicDeleteDoc;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, CompositePostingIterator, BufferedIterator);

/////////////////////////////////////////////////////////////////////////////
template <typename BufferedIterator>
inline index::ErrorCode CompositePostingIterator<BufferedIterator>::BufferedAdvance(docid64_t docId)
{
    if (docId <= _bufferedDocId) {
        return index::ErrorCode::OK;
    }
    docid64_t result = INVALID_DOCID;
    auto ec = _bufferedIterator->InnerSeekDoc(docId, result);
    _bufferedDocId = (result == INVALID_DOCID) ? DOCID_EOF : result;
    return ec;
}

template <typename BufferedIterator>
inline void CompositePostingIterator<BufferedIterator>::DynamicAdvance(docid64_t docId)
{
    if (docId <= _dynamicDocId) {
        return;
    }
    KeyType keyResult;
    if (_dynamicIterator->SeekDocWithDelete(docId, &keyResult)) {
        _dynamicDocId = keyResult.DocId();
        _isDynamicDeleteDoc = keyResult.IsDelete();
    } else {
        _dynamicDocId = DOCID_EOF;
    }
}

template <typename BufferedIterator>
inline index::ErrorCode CompositePostingIterator<BufferedIterator>::SeekDocWithErrorCode(docid64_t docId,
                                                                                         docid64_t& result)
{
    auto ec = index::ErrorCode::OK;
    if (docId != INVALID_DOCID) {
        DynamicAdvance(docId);
        ec = BufferedAdvance(docId);
    }
    if (ec != index::ErrorCode::OK) {
        return ec;
    }
    for (; _bufferedDocId != DOCID_EOF or _dynamicDocId != DOCID_EOF;) {
        if (_bufferedDocId < _dynamicDocId) {
            result = _bufferedDocId; // 5
            return BufferedAdvance(_bufferedDocId + 1);
        } else if (_bufferedDocId > _dynamicDocId) {
            int64_t tmpDocId = _dynamicDocId;
            bool tmpDelete = _isDynamicDeleteDoc;
            DynamicAdvance(_dynamicDocId + 1);
            if (!tmpDelete) {
                result = tmpDocId;
                return index::ErrorCode::OK;
            }
        } else {
            int64_t tmpDocId = _bufferedDocId;
            bool tmpDelete = _isDynamicDeleteDoc;
            if (_dynamicDocId != DOCID_EOF) {
                DynamicAdvance(_dynamicDocId + 1);
            }
            if (_bufferedDocId != DOCID_EOF) {
                auto ec = BufferedAdvance(_bufferedDocId + 1);
                if (ec != index::ErrorCode::OK) {
                    return ec;
                }
            }
            if (!tmpDelete and tmpDocId != INVALID_DOCID) {
                result = tmpDocId;
                return index::ErrorCode::OK;
            }
        }
    }
    result = INVALID_DOCID;
    return index::ErrorCode::OK;
}

template <typename BufferedIterator>
inline docid64_t CompositePostingIterator<BufferedIterator>::SeekDoc(docid64_t docId)
{
    docid64_t ret = INVALID_DOCID;
    auto ec = SeekDocWithErrorCode(docId, ret);
    index::ThrowIfError(ec);
    return ret;
}

template <typename BufferedIterator>
CompositePostingIterator<BufferedIterator>::CompositePostingIterator(autil::mem_pool::Pool* sessionPool,
                                                                     BufferedIterator* bufferedIter,
                                                                     DynamicPostingIterator* dynamicIter)
    : _sessionPool(sessionPool)
    , _bufferedIterator(bufferedIter)
    , _dynamicIterator(dynamicIter)
    , _bufferedDocId(INVALID_DOCID)
    , _dynamicDocId(INVALID_DOCID)
{
    assert(_bufferedIterator and _dynamicIterator);
}

template <typename BufferedIterator>
CompositePostingIterator<BufferedIterator>::~CompositePostingIterator()
{
    if (_bufferedIterator) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _bufferedIterator);
    }
    if (_dynamicIterator) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _dynamicIterator);
    }
}

template <typename BufferedIterator>
TermMeta* CompositePostingIterator<BufferedIterator>::GetTermMeta() const
{
    return _bufferedIterator->GetTermMeta();
}

template <typename BufferedIterator>
PostingIterator* CompositePostingIterator<BufferedIterator>::Clone() const
{
    auto clonedBufferIter = static_cast<BufferedIterator*>(_bufferedIterator->Clone());
    auto clonedDynamiIter = static_cast<DynamicPostingIterator*>(_dynamicIterator->Clone());

    return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, CompositePostingIterator<BufferedIterator>, _sessionPool,
                                        clonedBufferIter, clonedDynamiIter);
}

template <typename BufferedIterator>
void CompositePostingIterator<BufferedIterator>::Reset()
{
    _bufferedIterator->Reset();
    _dynamicIterator->Reset();
    _bufferedDocId = INVALID_DOCID;
    _dynamicDocId = INVALID_DOCID;
    _isDynamicDeleteDoc = false;
}

using BufferedCompositePostingIterator = CompositePostingIterator<BufferedPostingIterator>;
} // namespace indexlib::index

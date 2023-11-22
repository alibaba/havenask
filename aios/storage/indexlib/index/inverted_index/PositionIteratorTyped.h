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
#include "indexlib/index/inverted_index/PostingIteratorImpl.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/SingleBitmapPostingIterator.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {

template <class SingleIterator>
class PositionIteratorTyped : public PostingIteratorImpl
{
public:
    typedef SingleIterator SingleIteratorType;
    typedef typename SingleIterator::InDocPositionStateType StateType;

public:
    PositionIteratorTyped(optionflag_t optionFlag = OPTION_FLAG_ALL, autil::mem_pool::Pool* sessionPool = nullptr,
                          PostingIteratorImpl::TracerPtr tracer = nullptr);
    virtual ~PositionIteratorTyped();

public:
    virtual bool Init(const std::shared_ptr<SegmentPostingVector>& segPostings, const uint32_t statePoolSize);

    docid64_t SeekDoc(docid64_t docId) override;
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override;
    index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos) override;
    void Unpack(TermMatchData& termMatchData) override;

    docpayload_t GetDocPayload() override;
    pospayload_t GetPosPayload() const;
    PostingIteratorType GetType() const override;
    bool HasPosition() const override { return _optionFlag & of_position_list; }

public:
    // for runtime inline.
    docid64_t InnerSeekDoc(docid64_t docId);
    index::ErrorCode InnerSeekDoc(docid64_t docId, docid64_t& result);
    void Reset() override;

protected:
    index::Result<bool> MoveToNextSegment();
    virtual index::ErrorCode InitSingleIterator(SingleIteratorType* singleIterator) = 0;

protected:
    SingleIteratorType* CreateSingleIterator(uint8_t compressMode) const;

protected:
    optionflag_t _optionFlag;
    std::vector<SingleIteratorType*> _singleIterators;
    int32_t _segmentCursor;
    util::ObjectPool<StateType> _statePool;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////
template <class SingleIterator>
PositionIteratorTyped<SingleIterator>::PositionIteratorTyped(optionflag_t optionFlag,
                                                             autil::mem_pool::Pool* sessionPool,
                                                             PostingIteratorImpl::TracerPtr tracer)
    : PostingIteratorImpl(sessionPool, std::move(tracer))
    , _optionFlag(optionFlag)
    , _segmentCursor(-1)
{
}

template <class SingleIterator>
PositionIteratorTyped<SingleIterator>::~PositionIteratorTyped()
{
    for (size_t i = 0; i < _singleIterators.size(); i++) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _singleIterators[i]);
    }
    _singleIterators.clear();
}

template <class SingleIterator>
bool PositionIteratorTyped<SingleIterator>::Init(const std::shared_ptr<SegmentPostingVector>& segPostings,
                                                 const uint32_t statePoolSize)
{
    if (!PostingIteratorImpl::Init(segPostings)) {
        return false;
    }
    _statePool.Init(statePoolSize);
    _segmentCursor = -1;
    return MoveToNextSegment().ValueOrThrow();
}

template <class SingleIterator>
void PositionIteratorTyped<SingleIterator>::Reset()
{
    for (size_t i = 0; i < _singleIterators.size(); i++) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _singleIterators[i]);
    }
    _singleIterators.clear();

    _segmentCursor = -1;
    MoveToNextSegment().ValueOrThrow();
}

template <class SingleIterator>
docid64_t PositionIteratorTyped<SingleIterator>::InnerSeekDoc(docid64_t docId)
{
    docid64_t docRet = INVALID_DOCID;
    auto ec = InnerSeekDoc(docId, docRet);
    index::ThrowIfError(ec);
    return docRet;
}

template <class SingleIterator>
index::ErrorCode PositionIteratorTyped<SingleIterator>::InnerSeekDoc(docid64_t docId, docid64_t& result)
{
    docid64_t docRet = INVALID_DOCID;
    for (;;) {
        SingleIteratorType* singleIterator = _singleIterators[_segmentCursor];
        index::ErrorCode ec = singleIterator->SeekDocWithErrorCode(docId, docRet);
        if (unlikely(ec != index::ErrorCode::OK)) {
            return ec;
        }

        if (docRet != INVALID_DOCID) {
            result = docRet;
            return index::ErrorCode::OK;
        }

        auto ret = MoveToNextSegment();
        if (unlikely(!ret.Ok())) {
            return ret.GetErrorCode();
        }

        if (!ret.Value()) {
            break;
        }
    }
    result = INVALID_DOCID;
    return index::ErrorCode::OK;
}

template <class SingleIterator>
docid64_t PositionIteratorTyped<SingleIterator>::SeekDoc(docid64_t docId)
{
    return InnerSeekDoc(docId);
}

template <class SingleIterator>
index::ErrorCode PositionIteratorTyped<SingleIterator>::SeekDocWithErrorCode(docid64_t docId, docid64_t& result)
{
    return InnerSeekDoc(docId, result);
}

template <class SingleIterator>
index::ErrorCode PositionIteratorTyped<SingleIterator>::SeekPositionWithErrorCode(pos_t pos, pos_t& result)
{
    return _singleIterators[_segmentCursor]->SeekPositionWithErrorCode(pos, result);
}

template <class SingleIterator>
docpayload_t PositionIteratorTyped<SingleIterator>::GetDocPayload()
{
    return _singleIterators[_segmentCursor]->GetDocPayload();
}

template <class SingleIterator>
void PositionIteratorTyped<SingleIterator>::Unpack(TermMatchData& termMatchData)
{
    _singleIterators[_segmentCursor]->Unpack(termMatchData);
}

template <class SingleIterator>
pospayload_t PositionIteratorTyped<SingleIterator>::GetPosPayload() const
{
    return _singleIterators[_segmentCursor]->GetPosPayload();
}

template <class SingleIterator>
PostingIteratorType PositionIteratorTyped<SingleIterator>::GetType() const
{
    return SingleIterator::GetType();
}

template <class SingleIterator>
inline SingleIterator* PositionIteratorTyped<SingleIterator>::CreateSingleIterator(uint8_t compressMode) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SingleIterator, compressMode, _optionFlag, _sessionPool);
}

template <>
inline SingleBitmapPostingIterator*
PositionIteratorTyped<SingleBitmapPostingIterator>::CreateSingleIterator(uint8_t compressMode) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SingleBitmapPostingIterator, _optionFlag);
}

template <typename SingleIterator>
inline index::Result<bool> PositionIteratorTyped<SingleIterator>::MoveToNextSegment()
{
    if ((size_t)_segmentCursor != mSegmentPostings->size() - 1) {
        ++_segmentCursor;
        docid64_t currentBaseDocId = (*mSegmentPostings)[_segmentCursor].GetBaseDocId();
        SingleIteratorType* singleIterator =
            CreateSingleIterator((*mSegmentPostings)[_segmentCursor].GetCompressMode());
        singleIterator->SetBaseDocId(currentBaseDocId);
        index::ErrorCode ec = InitSingleIterator(singleIterator);
        if (unlikely(ec != index::ErrorCode::OK)) {
            return false;
        }
        _singleIterators.push_back(singleIterator);

        return true;
    }

    return false;
}

template <>
inline index::Result<bool> PositionIteratorTyped<SingleBitmapPostingIterator>::MoveToNextSegment()
{
    if ((size_t)_segmentCursor != mSegmentPostings->size() - 1) {
        ++_segmentCursor;
        docid64_t currentBaseDocId = (*mSegmentPostings)[_segmentCursor].GetBaseDocId();
        SingleIteratorType* singleIterator =
            CreateSingleIterator((*mSegmentPostings)[_segmentCursor].GetCompressMode());
        singleIterator->SetBaseDocId(currentBaseDocId);
        index::ErrorCode ec = InitSingleIterator(singleIterator);
        if (unlikely(ec != index::ErrorCode::OK)) {
            return false;
        }
        _singleIterators.push_back(singleIterator);

        return true;
    }

    return false;
}
} // namespace indexlib::index

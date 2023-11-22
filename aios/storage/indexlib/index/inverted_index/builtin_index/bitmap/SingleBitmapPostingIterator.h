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
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapInDocPositionState.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/ExpandableBitmap.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlib::index {

struct BitmapPostingExpandData;

class SingleBitmapPostingIterator
{
public:
    using InDocPositionStateType = BitmapInDocPositionState;

    SingleBitmapPostingIterator(optionflag_t flag = NO_PAYLOAD);
    ~SingleBitmapPostingIterator();

    void Init(const util::ByteSliceListPtr& sliceListPtr, BitmapPostingExpandData* expandData,
              util::ObjectPool<InDocPositionStateType>* statePool);

    void Init(util::ByteSlice* singleSlice, BitmapPostingExpandData* expandData,
              util::ObjectPool<InDocPositionStateType>* statePool);

    void Init(BitmapPostingExpandData* expandData, util::ObjectPool<InDocPositionStateType>* statePool);

    // for realtime segment init
    void Init(const PostingWriter* postingWriter, util::ObjectPool<InDocPositionStateType>* statePool);

    inline docid64_t SeekDoc(docid64_t docId);
    inline index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result);

    inline void Unpack(TermMatchData& termMatchData);
    inline bool HasPosition() const { return false; }

    inline pos_t SeekPosition(pos_t pos) { return INVALID_POSITION; }
    inline index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextPos)
    {
        nextPos = INVALID_POSITION;
        return index::ErrorCode::OK;
    }
    inline void SetBaseDocId(docid64_t baseDocId) { _baseDocId = baseDocId; }
    inline docid64_t GetLastDocId() const { return _lastDocId; }
    inline bool Test(docid64_t docId)
    {
        docid64_t localDocId = docId - _baseDocId;
        if (unlikely(localDocId >= static_cast<docid64_t>(_bitmap.GetItemCount()))) {
            if (!_expandBitmap.Test(localDocId - _bitmap.GetItemCount())) {
                return false;
            }
        } else {
            if (!_bitmap.Test(localDocId)) {
                return false;
            }
        }
        _currentLocalId = localDocId;
        return true;
    }

    TermMeta* GetTermMeta() const { return const_cast<TermMeta*>(&_termMeta); }
    docpayload_t GetDocPayload() const { return 0; }

    tf_t GetTf() const { return 0; }

    static PostingIteratorType GetType() { return pi_bitmap; }

    void Reset();

public:
    static docid64_t SeekBitmap(const util::Bitmap& bitmap, docid64_t docId);

public:
    // for test.
    void Init(const util::Bitmap& _bitmap, TermMeta* termMeta, util::ObjectPool<InDocPositionStateType>* statePool);
    // for test
    docid64_t GetCurrentGlobalDocId() { return _currentLocalId + _baseDocId; }

private:
    void SetStatePool(util::ObjectPool<InDocPositionStateType>* statePool) { _statePool = statePool; }
    void InitExpandBitmap(BitmapPostingExpandData* expandWriter);

private:
    docid64_t _currentLocalId;
    docid64_t _baseDocId;
    util::Bitmap _bitmap;
    docid64_t _lastDocId;
    util::Bitmap _expandBitmap;
    util::ObjectPool<InDocPositionStateType>* _statePool;
    TermMeta _termMeta;

private:
    friend class SingleBitmapPostingIteratorTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SingleBitmapPostingIterator> SingleBitmapPostingIteratorPtr;

inline index::ErrorCode SingleBitmapPostingIterator::SeekDocWithErrorCode(docid64_t docId, docid64_t& result)
{
    result = SeekDoc(docId);
    return index::ErrorCode::OK;
}

inline docid64_t SingleBitmapPostingIterator::SeekDoc(docid64_t docId)
{
    docId -= _baseDocId;
    docId = std::max(_currentLocalId + 1, docId);

    if (docId >= static_cast<docid64_t>(_bitmap.GetItemCount())) {
        docid64_t expandDocId = INVALID_DOCID;
        if (_expandBitmap.Size() != 0) {
            // has expand bitmap
            expandDocId = SeekBitmap(_expandBitmap, docId - _bitmap.GetItemCount());
        }
        if (expandDocId != INVALID_DOCID) {
            _currentLocalId = expandDocId + _bitmap.GetItemCount();
            return _currentLocalId + _baseDocId;
        }
        return INVALID_DOCID;
    }
    docid64_t localDocId = SeekBitmap(_bitmap, docId);
    if (localDocId != INVALID_DOCID) {
        _currentLocalId = localDocId;
        return _currentLocalId + _baseDocId;
    }
    if (_expandBitmap.Size() != 0) {
        docid64_t expandDocId = SeekBitmap(_expandBitmap, 0);
        if (expandDocId != INVALID_DOCID) {
            _currentLocalId = expandDocId + _bitmap.GetItemCount();
            return _currentLocalId + _baseDocId;
        }
    }
    return INVALID_DOCID;
}

inline docid64_t SingleBitmapPostingIterator::SeekBitmap(const util::Bitmap& bitmap, docid64_t docId)
{
    if (unlikely(docId >= static_cast<docid64_t>(bitmap.GetItemCount()))) {
        return INVALID_DOCID;
    }
    uint32_t blockId = docId / 32;
    uint32_t blockOffset = docId % 32;
    uint32_t* bitmapData = bitmap.GetData();
    uint32_t data = bitmapData[blockId];
    if (blockOffset) {
        data &= (0xffffffff >> blockOffset);
    }
    if (data) {
        int i = __builtin_clz(data);
        return (blockId << 5) + i;
    }
    uint32_t blockCount = bitmap.GetSlotCount();
    while (true) {
        ++blockId;
        if (blockId >= blockCount) {
            break;
        }
        if (bitmapData[blockId]) {
            int i = __builtin_clz(bitmapData[blockId]);
            return (blockId << 5) + i;
        }
    }
    return INVALID_DOCID;
}

inline void SingleBitmapPostingIterator::Unpack(TermMatchData& termMatchData)
{
    InDocPositionStateType* state = _statePool->Alloc();
    state->SetDocId(_currentLocalId + _baseDocId);
    state->SetTermFreq(1);
    termMatchData.SetInDocPositionState(state);
}

inline void SingleBitmapPostingIterator::Reset() { _currentLocalId = INVALID_DOCID; }
} // namespace indexlib::index

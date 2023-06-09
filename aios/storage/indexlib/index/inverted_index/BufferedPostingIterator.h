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

#include "indexlib/index/common/numeric_compress/ReferenceCompressIntEncoder.h"
#include "indexlib/index/inverted_index/BufferedIndexDecoder.h"
#include "indexlib/index/inverted_index/PostingIteratorImpl.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/format/NormalInDocState.h"

namespace indexlib::index {

class BufferedPostingIterator : public PostingIteratorImpl
{
public:
    BufferedPostingIterator(const PostingFormatOption& postingFormatOption, autil::mem_pool::Pool* sessionPool,
                            indexlib::util::PooledUniquePtr<indexlib::index::InvertedIndexSearchTracer> tracer);

    virtual ~BufferedPostingIterator();

private:
    BufferedPostingIterator(const BufferedPostingIterator& other);

public:
    virtual bool Init(const std::shared_ptr<SegmentPostingVector>& segPostings,
                      const SectionAttributeReader* sectionReader, const uint32_t statePoolSize);

    docid_t SeekDoc(docid_t docId) override;
    index::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;
    void Unpack(TermMatchData& termMatchData) override;

    docpayload_t GetDocPayload() override { return InnerGetDocPayload(); }
    pospayload_t GetPosPayload();
    PostingIteratorType GetType() const override { return pi_buffered; }
    index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextPos) override;
    BufferedPostingIterator* Clone() const override;

    bool HasPosition() const override { return _postingFormatOption.HasPositionList(); }
    void AllocateBuffers();

public:
    // for runtime inline.
    docid_t InnerSeekDoc(docid_t docId);
    index::ErrorCode InnerSeekDoc(docid_t docId, docid_t& result);
    fieldmap_t GetFieldMap();
    index::ErrorCode GetFieldMap(fieldmap_t& fieldMap);
    void Reset() override;

private:
    index::ErrorCode SeekDocForNormal(docid_t docId, docid_t& result);
    index::ErrorCode SeekDocForReference(docid_t docId, docid_t& result);

private:
    uint32_t GetCurrentSeekedDocCount() const;
    docpayload_t InnerGetDocPayload();
    void DecodeTFBuffer();
    void DecodeDocPayloadBuffer();
    void DecodeFieldMapBuffer();
    int32_t GetDocOffsetInBuffer() const;
    tf_t InnerGetTF();
    fieldmap_t InnerGetFieldMap() const;
    ttf_t GetCurrentTTF();
    void MoveToCurrentDoc();

private:
    // virtual for test
    virtual BufferedPostingDecoder* CreateBufferedPostingDecoder();

protected:
    PostingFormatOption _postingFormatOption;
    docid_t _lastDocIdInBuffer;
    docid_t _currentDocId;
    docid_t* _docBufferCursor;
    docid_t _docBuffer[MAX_DOC_PER_RECORD];
    docid_t* _docBufferBase;
    ttf_t _currentTTF;
    int32_t _tfBufferCursor;
    tf_t* _tfBuffer;
    docpayload_t* _docPayloadBuffer;
    fieldmap_t* _fieldMapBuffer;

    BufferedPostingDecoder* _decoder;

    ReferenceCompressIntReader<uint32_t> _refCompressReader;

    bool _needMoveToCurrentDoc;
    bool _inDocPosIterInited;
    NormalInDocPositionIterator* _inDocPositionIterator;

    NormalInDocState _state;
    util::ObjectPool<NormalInDocState> _statePool;

private:
    friend class BufferedPostingIteratorTest;
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////////
// inline functions

inline docpayload_t BufferedPostingIterator::InnerGetDocPayload()
{
    if (_postingFormatOption.HasDocPayload()) {
        DecodeTFBuffer();
        DecodeDocPayloadBuffer();
        return _docPayloadBuffer[GetDocOffsetInBuffer()];
    }
    return 0;
}

inline fieldmap_t BufferedPostingIterator::GetFieldMap()
{
    fieldmap_t fieldMap = fieldmap_t();
    auto ec = GetFieldMap(fieldMap);
    index::ThrowIfError(ec);
    return fieldMap;
}

inline tf_t BufferedPostingIterator::InnerGetTF()
{
    if (_postingFormatOption.HasTfList()) {
        DecodeTFBuffer();
        return _tfBuffer[GetDocOffsetInBuffer()];
    }
    return 0;
}

inline ttf_t BufferedPostingIterator::GetCurrentTTF()
{
    if (_postingFormatOption.HasTfList()) {
        DecodeTFBuffer();
        int32_t offset = GetDocOffsetInBuffer(); // get ttf before this doc.
        for (; _tfBufferCursor < offset; ++_tfBufferCursor) {
            _currentTTF += _tfBuffer[_tfBufferCursor];
        }
    }
    return _currentTTF;
}

inline void BufferedPostingIterator::DecodeTFBuffer()
{
    if (_postingFormatOption.HasTfList()) {
        if (_decoder->DecodeCurrentTFBuffer(_tfBuffer)) {
            _tfBufferCursor = 0;
        }
    }
}

inline void BufferedPostingIterator::DecodeDocPayloadBuffer()
{
    if (_postingFormatOption.HasDocPayload()) {
        _decoder->DecodeCurrentDocPayloadBuffer(_docPayloadBuffer);
    }
}

inline void BufferedPostingIterator::DecodeFieldMapBuffer()
{
    if (_postingFormatOption.HasFieldMap()) {
        _decoder->DecodeCurrentFieldMapBuffer(_fieldMapBuffer);
    }
}

inline void BufferedPostingIterator::MoveToCurrentDoc()
{
    _needMoveToCurrentDoc = false;
    _inDocPosIterInited = false;
    if (_postingFormatOption.HasTermFrequency()) {
        _state.SetDocId(_currentDocId);
        _state.SetTermFreq(InnerGetTF());

        uint32_t seekedDocCount = GetCurrentSeekedDocCount();
        _state.SetSeekedDocCount(seekedDocCount);

        if (_postingFormatOption.HasPositionList()) {
            ttf_t currentTTF = 0;
            currentTTF = GetCurrentTTF();
            _decoder->MoveToCurrentDocPosition(currentTTF);
        }
    }
}

inline int32_t BufferedPostingIterator::GetDocOffsetInBuffer() const
{
    if (_postingFormatOption.IsReferenceCompress()) {
        return _refCompressReader.GetCursor() - 1;
    }
    return _docBufferCursor - _docBufferBase - 1;
}

inline fieldmap_t BufferedPostingIterator::InnerGetFieldMap() const { return _fieldMapBuffer[GetDocOffsetInBuffer()]; }

// TODO: opt with using ReferencePostingIterator instead of if
inline docid_t BufferedPostingIterator::InnerSeekDoc(docid_t docId)
{
    docid_t ret = INVALID_DOCID;
    auto ec = InnerSeekDoc(docId, ret);
    index::ThrowIfError(ec);
    return ret;
}

inline index::ErrorCode BufferedPostingIterator::InnerSeekDoc(docid_t docId, docid_t& result)
{
    if (_postingFormatOption.IsReferenceCompress()) {
        return SeekDocForReference(docId, result);
    }
    return SeekDocForNormal(docId, result);
}

inline index::ErrorCode BufferedPostingIterator::SeekDocForNormal(docid_t docId, docid_t& result)
{
    docid_t curDocId = _currentDocId;
    docId = std::max(curDocId + 1, docId);
    if (unlikely(docId > _lastDocIdInBuffer)) {
        try {
            if (!_decoder->DecodeDocBuffer(docId, _docBuffer, curDocId, _lastDocIdInBuffer, _currentTTF)) {
                result = INVALID_DOCID;
                return index::ErrorCode::OK;
            }
            _docBufferCursor = _docBuffer + 1;
            _postingFormatOption = _decoder->GetPostingFormatOption();
        } catch (const util::FileIOException& e) {
            return index::ErrorCode::FileIO;
        }
    }
    docid_t* cursor = _docBufferCursor;
    while (curDocId < docId) {
        curDocId += *(cursor++);
    }
    _currentDocId = curDocId;
    _docBufferCursor = cursor;
    _needMoveToCurrentDoc = true;
    result = curDocId;
    return index::ErrorCode::OK;
}

inline index::ErrorCode BufferedPostingIterator::SeekDocForReference(docid_t docId, docid_t& result)
{
    docid_t curDocId = _currentDocId;
    if (unlikely(curDocId + 1 > docId)) {
        docId = curDocId + 1;
    }
    if (unlikely(docId > _lastDocIdInBuffer)) {
        _docBufferBase = _docBuffer; // copy when use block file only
        try {
            if (!_decoder->DecodeDocBufferMayCopy(docId, _docBufferBase, curDocId, _lastDocIdInBuffer, _currentTTF)) {
                result = INVALID_DOCID;
                return index::ErrorCode::OK;
            }
            _postingFormatOption = _decoder->GetPostingFormatOption();
            _refCompressReader.Reset((char*)_docBufferBase);
        } catch (const util::FileIOException& e) {
            return index::ErrorCode::FileIO;
        }
    }
    // TODO: optimze
    if (curDocId < docId) {
        docid_t baseDocId = _decoder->GetCurrentSegmentBaseDocId();
        assert(docId > baseDocId);
        curDocId = _refCompressReader.Seek(docId - baseDocId);
        curDocId += baseDocId;
    }
    _currentDocId = curDocId;
    _needMoveToCurrentDoc = true;
    result = curDocId;
    return index::ErrorCode::OK;
}

inline index::ErrorCode BufferedPostingIterator::GetFieldMap(fieldmap_t& fieldMap)
{
    if (_postingFormatOption.HasFieldMap()) {
        try {
            DecodeTFBuffer();
            DecodeDocPayloadBuffer();
            DecodeFieldMapBuffer();
            fieldMap = InnerGetFieldMap();
            return index::ErrorCode::OK;
        } catch (const util::FileIOException& e) {
            return index::ErrorCode::FileIO;
        }
    }
    fieldMap = fieldmap_t();
    return index::ErrorCode::OK;
}

inline void BufferedPostingIterator::AllocateBuffers()
{
    _tfBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, tf_t, MAX_DOC_PER_RECORD);
    _docPayloadBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, docpayload_t, MAX_DOC_PER_RECORD);
    _fieldMapBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, fieldmap_t, MAX_DOC_PER_RECORD);
}

inline uint32_t BufferedPostingIterator::GetCurrentSeekedDocCount() const
{
    return _decoder->InnerGetSeekedDocCount() + (GetDocOffsetInBuffer() + 1);
}

} // namespace indexlib::index

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
#include "indexlib/index/inverted_index/format/BufferedSegmentIndexDecoder.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {

template <class SkipListType>
class SkipListSegmentDecoder : public BufferedSegmentIndexDecoder
{
public:
    SkipListSegmentDecoder(autil::mem_pool::Pool* sessionPool, file_system::ByteSliceReader* docListReader,
                           uint32_t docListBegin, uint8_t docCompressMode);
    ~SkipListSegmentDecoder();

protected:
    void InitSkipList(uint32_t start, uint32_t end, util::ByteSliceList* postingList, df_t df,
                      CompressMode compressMode) override;

    void InitSkipList(uint32_t start, uint32_t end, util::ByteSlice* postingList, df_t df,
                      CompressMode compressMode) override;

    bool DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                         ttf_t& currentTTF) override;
    bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t*& docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                ttf_t& currentTTF) override;

    bool DecodeCurrentTFBuffer(tf_t* tfBuffer) override;
    void DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer) override;
    void DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer) override;

private:
    SkipListType* _skipListReader;
    autil::mem_pool::Pool* _sessionPool;
    file_system::ByteSliceReader* _docListReader;
    uint32_t _docListBeginPos;
    const Int32Encoder* _docEncoder;
    const Int32Encoder* _tfEncoder;
    const Int16Encoder* _docPayloadEncoder;
    const Int8Encoder* _fieldMapEncoder;

    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////////
template <class SkipListType>
SkipListSegmentDecoder<SkipListType>::SkipListSegmentDecoder(autil::mem_pool::Pool* sessionPool,
                                                             file_system::ByteSliceReader* docListReader,
                                                             uint32_t docListBegin, uint8_t docCompressMode)
    : _skipListReader(nullptr)
    , _sessionPool(sessionPool)
    , _docListReader(docListReader)
    , _docListBeginPos(docListBegin)
{
    assert(docCompressMode != SHORT_LIST_COMPRESS_MODE);
    EncoderProvider::EncoderParam param(docCompressMode, false, true);
    _docEncoder = EncoderProvider::GetInstance()->GetDocListEncoder(param);
    _tfEncoder = EncoderProvider::GetInstance()->GetTfListEncoder(param);
    _docPayloadEncoder = EncoderProvider::GetInstance()->GetDocPayloadEncoder(param);
    _fieldMapEncoder = EncoderProvider::GetInstance()->GetFieldMapEncoder(param);
}

template <class SkipListType>
SkipListSegmentDecoder<SkipListType>::~SkipListSegmentDecoder()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _skipListReader);
}

template <class SkipListType>
void SkipListSegmentDecoder<SkipListType>::InitSkipList(uint32_t start, uint32_t end, util::ByteSliceList* postingList,
                                                        df_t df, CompressMode compressMode)
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _skipListReader);
    _skipListReader = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SkipListType, compressMode == REFERENCE_COMPRESS_MODE);
    _skipListReader->Load(postingList, start, end, (df - 1) / MAX_DOC_PER_RECORD + 1);
}

template <class SkipListType>
void SkipListSegmentDecoder<SkipListType>::InitSkipList(uint32_t start, uint32_t end, util::ByteSlice* postingList,
                                                        df_t df, CompressMode compressMode)
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _skipListReader);
    _skipListReader = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SkipListType, compressMode == REFERENCE_COMPRESS_MODE);
    _skipListReader->Load(postingList, start, end, (df - 1) / MAX_DOC_PER_RECORD + 1);
}

template <class SkipListType>
bool SkipListSegmentDecoder<SkipListType>::DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId,
                                                           docid_t& lastDocId, ttf_t& currentTTF)
{
    uint32_t offset;
    uint32_t recordLen;
    typename SkipListType::keytype_t lastDocIdInPrevRecord;

    auto [status, ret] =
        _skipListReader->SkipTo((typename SkipListType::keytype_t)startDocId,
                                (typename SkipListType::keytype_t&)lastDocId, lastDocIdInPrevRecord, offset, recordLen);
    THROW_IF_STATUS_ERROR(status);
    if (!ret) {
        return false;
    }
    currentTTF = _skipListReader->GetPrevTTF();
    _skipedItemCount = _skipListReader->GetSkippedItemCount();

    _docListReader->Seek(offset + this->_docListBeginPos);
    _docEncoder->Decode((uint32_t*)docBuffer, MAX_DOC_PER_RECORD, *_docListReader);

    firstDocId = docBuffer[0] + lastDocIdInPrevRecord;
    return true;
}

template <class SkipListType>
bool SkipListSegmentDecoder<SkipListType>::DecodeDocBufferMayCopy(docid_t startDocId, docid_t*& docBuffer,
                                                                  docid_t& firstDocId, docid_t& lastDocId,
                                                                  ttf_t& currentTTF)
{
    uint32_t offset;
    uint32_t recordLen;
    typename SkipListType::keytype_t lastDocIdInPrevRecord;

    auto [status, ret] =
        _skipListReader->SkipTo((typename SkipListType::keytype_t)startDocId,
                                (typename SkipListType::keytype_t&)lastDocId, lastDocIdInPrevRecord, offset, recordLen);
    THROW_IF_STATUS_ERROR(status);
    if (!ret) {
        return false;
    }
    currentTTF = _skipListReader->GetPrevTTF();
    _skipedItemCount = _skipListReader->GetSkippedItemCount();

    _docListReader->Seek(offset + this->_docListBeginPos);
    _docEncoder->DecodeMayCopy((uint32_t*&)docBuffer, MAX_DOC_PER_RECORD, *_docListReader);

    // TODO: merge with DecodeDocBuffer
    firstDocId = ReferenceCompressInt32Encoder::GetFirstValue((char*)docBuffer);
    return true;
}

template <class SkipListType>
bool SkipListSegmentDecoder<SkipListType>::DecodeCurrentTFBuffer(tf_t* tfBuffer)
{
    _tfEncoder->Decode((uint32_t*)tfBuffer, MAX_DOC_PER_RECORD, *_docListReader);
    return true;
}

template <class SkipListType>
void SkipListSegmentDecoder<SkipListType>::DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer)
{
    _docPayloadEncoder->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, *_docListReader);
}

template <class SkipListType>
void SkipListSegmentDecoder<SkipListType>::DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer)
{
    _fieldMapEncoder->Decode(fieldBitmapBuffer, MAX_DOC_PER_RECORD, *_docListReader);
}

} // namespace indexlib::index

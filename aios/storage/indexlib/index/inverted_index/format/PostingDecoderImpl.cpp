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
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"

#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/inverted_index/format/DictInlineFormatter.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PostingDecoderImpl);

PostingDecoderImpl::PostingDecoderImpl(const PostingFormatOption& postingFormatOption)
    : _inlineDictValue(INVALID_DICT_VALUE)
    , _isDocListDictInline(false)
    , _docIdEncoder(nullptr)
    , _tfListEncoder(nullptr)
    , _docPayloadEncoder(nullptr)
    , _fieldMapEncoder(nullptr)
    , _positionEncoder(nullptr)
    , _posPayloadEncoder(nullptr)
    , _decodedDocCount(0)
    , _decodedPosCount(0)
    , _postingDataLength(0)
    , _postingFormatOption(postingFormatOption)
{
}

PostingDecoderImpl::~PostingDecoderImpl() {}

void PostingDecoderImpl::Init(TermMeta* termMeta, const file_system::ByteSliceReaderPtr& postingListReader,
                              const file_system::ByteSliceReaderPtr& positionListReader,
                              const std::shared_ptr<util::Bitmap>& tfBitmap, size_t postingDataLen)
{
    _termMeta = termMeta;
    _postingListReader = postingListReader;
    _positionListReader = positionListReader;
    _tfBitmap = tfBitmap;
    _inlineDictValue = INVALID_DICT_VALUE;
    _isDocListDictInline = false;
    _decodedDocCount = 0;
    _decodedPosCount = 0;

    _docIdEncoder = nullptr;
    _tfListEncoder = nullptr;
    _docPayloadEncoder = nullptr;
    _fieldMapEncoder = nullptr;
    _positionEncoder = nullptr;
    _posPayloadEncoder = nullptr;

    _postingDataLength = postingDataLen;
    InitDocListEncoder(_postingFormatOption.GetDocListFormatOption(), _termMeta->GetDocFreq());
    InitPosListEncoder(_postingFormatOption.GetPosListFormatOption(), _termMeta->GetTotalTermFreq());
}

void PostingDecoderImpl::Init(TermMeta* termMeta, dictvalue_t dictValue, bool isDocList, bool dfFirst)
{
    _termMeta = termMeta;
    _postingListReader.reset();
    _positionListReader.reset();
    _tfBitmap.reset();
    _inlineDictValue = dictValue;
    _isDocListDictInline = isDocList;
    _dfFirstDictInline = dfFirst;
    _decodedDocCount = 0;
    _decodedPosCount = 0;

    _docIdEncoder = nullptr;
    _tfListEncoder = nullptr;
    _docPayloadEncoder = nullptr;
    _fieldMapEncoder = nullptr;
    _positionEncoder = nullptr;
    _posPayloadEncoder = nullptr;

    if (_isDocListDictInline) {
        docid32_t beginDocid;
        df_t df;
        DictInlineDecoder::DecodeContinuousDocId({dfFirst, _inlineDictValue}, beginDocid, df);
        _termMeta->SetDocFreq(df);
        _termMeta->SetPayload(0);
        _termMeta->SetTotalTermFreq(df);
    } else {
        DictInlineFormatter formatter(_postingFormatOption, dictValue);
        _termMeta->SetDocFreq(formatter.GetDocFreq());
        _termMeta->SetPayload(formatter.GetTermPayload());
        _termMeta->SetTotalTermFreq(formatter.GetTermFreq());
    }
    _postingDataLength = 0;
}

uint32_t PostingDecoderImpl::DecodeDocList(docid32_t* docIdBuf, tf_t* tfListBuf, docpayload_t* docPayloadBuf,
                                           fieldmap_t* fieldMapBuf, size_t len)
{
    if (_decodedDocCount >= _termMeta->GetDocFreq()) {
        return 0;
    }

    if (_isDocListDictInline) {
        docid32_t beginDocid;
        df_t df;
        assert(_dfFirstDictInline != std::nullopt);
        DictInlineDecoder::DecodeContinuousDocId({_dfFirstDictInline.value(), _inlineDictValue}, beginDocid, df);
        uint32_t remainDocCount = df - _decodedDocCount;
        beginDocid += _decodedDocCount;
        uint32_t docLen = std::min((uint32_t)len, (uint32_t)remainDocCount);
        if (_postingFormatOption.IsReferenceCompress()) {
            for (uint32_t i = 0; i < docLen; i++) {
                docIdBuf[i] = beginDocid + (docid32_t)i;
            }
        } else {
            docIdBuf[0] = (_decodedDocCount == 0) ? beginDocid : 1;
            DictInlineDecoder::FastMemSet(docIdBuf + 1, docLen - 1, 1);
        }

        _decodedDocCount += docLen;
        return docLen;
    }

    // decode dict inline doclist
    if (_inlineDictValue != INVALID_DICT_VALUE) {
        DictInlineFormatter formatter(_postingFormatOption, _inlineDictValue);
        docIdBuf[0] = formatter.GetDocId();
        docPayloadBuf[0] = formatter.GetDocPayload();
        tfListBuf[0] = formatter.GetTermFreq();
        fieldMapBuf[0] = formatter.GetFieldMap();
        _decodedDocCount++;
        return 1;
    }

    // decode normal doclist
    auto [status, docLen] = _docIdEncoder->Decode((uint32_t*)docIdBuf, len, *_postingListReader);
    THROW_IF_STATUS_ERROR(status);
    if (_tfListEncoder) {
        auto [status, tfLen] = _tfListEncoder->Decode((uint32_t*)tfListBuf, len, *_postingListReader);
        THROW_IF_STATUS_ERROR(status);
        if (docLen != tfLen) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "doc/tf-list collapsed: [%u]/[%u]", docLen, tfLen);
        }
    }

    if (_docPayloadEncoder) {
        auto [status, payloadLen] = _docPayloadEncoder->Decode(docPayloadBuf, len, *_postingListReader);
        THROW_IF_STATUS_ERROR(status);
        if (payloadLen != docLen) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "doc/docpayload-list collapsed: [%u]/[%u]", docLen, payloadLen);
        }
    }

    if (_fieldMapEncoder) {
        auto [status, fieldmapLen] = _fieldMapEncoder->Decode(fieldMapBuf, len, *_postingListReader);
        THROW_IF_STATUS_ERROR(status);
        if (fieldmapLen != docLen) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "doc/fieldmap-list collapsed: [%u]/[%u]", docLen, fieldmapLen);
        }
    }
    _decodedDocCount += docLen;
    return docLen;
}

uint32_t PostingDecoderImpl::DecodePosList(pos_t* posListBuf, pospayload_t* posPayloadBuf, size_t len)
{
    if (_isDocListDictInline || _inlineDictValue != INVALID_DICT_VALUE) {
        return 0;
    }

    if (_decodedPosCount >= _termMeta->GetTotalTermFreq()) {
        return 0;
    }
    uint32_t decodePosCount = 0;
    if (_positionEncoder) {
        auto statusWith = _positionEncoder->Decode(posListBuf, len, *_positionListReader);
        THROW_IF_STATUS_ERROR(statusWith.first);
        decodePosCount = statusWith.second;
    }

    if (_posPayloadEncoder) {
        auto [status, payloadLen] = _posPayloadEncoder->Decode(posPayloadBuf, len, *_positionListReader);
        THROW_IF_STATUS_ERROR(status);

        if (payloadLen != decodePosCount) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "pos/pospayload-list collapsed: [%u]/[%u]", decodePosCount,
                                 payloadLen);
        }
    }
    _decodedPosCount += decodePosCount;
    return decodePosCount;
}

void PostingDecoderImpl::InitDocListEncoder(const DocListFormatOption& docListFormatOption, df_t df)
{
    uint8_t docCompressMode =
        ShortListOptimizeUtil::GetDocListCompressMode(df, _postingFormatOption.GetDocListCompressMode());

    EncoderProvider::EncoderParam param(docCompressMode, docListFormatOption.IsShortListVbyteCompress(), true);
    _docIdEncoder = EncoderProvider::GetInstance()->GetDocListEncoder(param);
    if (docListFormatOption.HasTfList()) {
        _tfListEncoder = EncoderProvider::GetInstance()->GetTfListEncoder(param);
    }

    if (docListFormatOption.HasDocPayload()) {
        _docPayloadEncoder = EncoderProvider::GetInstance()->GetDocPayloadEncoder(param);
    }

    if (docListFormatOption.HasFieldMap()) {
        _fieldMapEncoder = EncoderProvider::GetInstance()->GetFieldMapEncoder(param);
    }

    if (docListFormatOption.HasTfBitmap() && _tfBitmap.get() == nullptr) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "PositionBitmap is Null when HasTfBitmap!");
    }
}

void PostingDecoderImpl::InitPosListEncoder(const PositionListFormatOption& posListFormatOption, ttf_t totalTF)
{
    if (!posListFormatOption.HasPositionList()) {
        return;
    }

    uint8_t posCompressMode = ShortListOptimizeUtil::GetPosListCompressMode(totalTF);
    EncoderProvider::EncoderParam param(posCompressMode, false, true);
    _positionEncoder = EncoderProvider::GetInstance()->GetPosListEncoder(param);
    if (posListFormatOption.HasPositionPayload()) {
        _posPayloadEncoder = EncoderProvider::GetInstance()->GetPosPayloadEncoder(param);
    }
}
} // namespace indexlib::index

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
#include "indexlib/index/inverted_index/format/ShortListSegmentDecoder.h"

#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, ShortListSegmentDecoder);

ShortListSegmentDecoder::ShortListSegmentDecoder(file_system::ByteSliceReader* docListReader, uint32_t docListBegin,
                                                 uint8_t docCompressMode, bool enableShortListVbyteCompress)
    : _decodeShortList(false)
    , _docListReader(docListReader)
    , _docListBeginPos(docListBegin)
{
    EncoderProvider::EncoderParam param(docCompressMode, enableShortListVbyteCompress, true);
    _docEncoder = EncoderProvider::GetInstance()->GetDocListEncoder(param);
    _tfEncoder = EncoderProvider::GetInstance()->GetTfListEncoder(param);
    _docPayloadEncoder = EncoderProvider::GetInstance()->GetDocPayloadEncoder(param);
    _fieldMapEncoder = EncoderProvider::GetInstance()->GetFieldMapEncoder(param);
}

bool ShortListSegmentDecoder::DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId,
                                              docid_t& lastDocId, ttf_t& currentTTF)
{
    if (_decodeShortList) {
        // only decode once. for performance purpose
        return false;
    }
    _decodeShortList = true;
    currentTTF = 0;
    _docListReader->Seek(_docListBeginPos);
    auto [status, docNum] = _docEncoder->Decode((uint32_t*)docBuffer, MAX_DOC_PER_RECORD, *_docListReader);
    THROW_IF_STATUS_ERROR(status);
    lastDocId = 0;
    for (uint32_t i = 0; i < docNum; ++i) {
        lastDocId += docBuffer[i];
    }

    if (startDocId > lastDocId) {
        return false;
    }

    firstDocId = docBuffer[0];
    return true;
}

bool ShortListSegmentDecoder::DecodeDocBufferMayCopy(docid_t startDocId, docid_t*& docBuffer, docid_t& firstDocId,
                                                     docid_t& lastDocId, ttf_t& currentTTF)
{
    if (_decodeShortList) {
        // only decode once. for performance purpose
        return false;
    }
    _decodeShortList = true;
    currentTTF = 0;
    _docListReader->Seek(_docListBeginPos);
    auto [status, docNum] = _docEncoder->DecodeMayCopy((uint32_t*&)docBuffer, MAX_DOC_PER_RECORD, *_docListReader);
    THROW_IF_STATUS_ERROR(status);
    lastDocId = docNum > 0 ? docBuffer[docNum - 1] : 0;

    if (startDocId > lastDocId) {
        return false;
    }

    firstDocId = docBuffer[0];
    return true;
}
} // namespace indexlib::index

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
#include "indexlib/index/inverted_index/format/InMemDocListDecoder.h"

#include "indexlib/index/common/numeric_compress/ReferenceCompressIntEncoder.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InMemDocListDecoder);

InMemDocListDecoder::InMemDocListDecoder(autil::mem_pool::Pool* sessionPool, bool isReferenceCompress)
    : _sessionPool(sessionPool)
    , _skipListReader(nullptr)
    , _docListBuffer(nullptr)
    , _df(0)
    , _finishDecoded(false)
    , _isReferenceCompress(isReferenceCompress)
{
}

InMemDocListDecoder::~InMemDocListDecoder()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _skipListReader);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _docListBuffer);
}

void InMemDocListDecoder::Init(df_t df, SkipListReader* skipListReader, BufferedByteSlice* docListBuffer)
{
    _df = df;
    _skipListReader = skipListReader;
    _docListBuffer = docListBuffer;
    _docListReader.Open(docListBuffer);
}

bool InMemDocListDecoder::DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId,
                                          docid_t& lastDocId, ttf_t& currentTTF)
{
    DocBufferInfo docBufferInfo(docBuffer, firstDocId, lastDocId, currentTTF);
    if (_skipListReader == nullptr) {
        currentTTF = 0;
        return DecodeDocBufferWithoutSkipList(0, 0, startDocId, docBufferInfo);
    }

    uint32_t offset;
    uint32_t recordLen;
    uint32_t lastDocIdInPrevRecord;

    auto [status, ret] =
        _skipListReader->SkipTo((uint32_t)startDocId, (uint32_t&)lastDocId, lastDocIdInPrevRecord, offset, recordLen);
    THROW_IF_STATUS_ERROR(status);
    if (!ret) {
        // we should decode buffer
        lastDocIdInPrevRecord = _skipListReader->GetLastKeyInBuffer();
        offset = _skipListReader->GetLastValueInBuffer();
        currentTTF = _skipListReader->GetCurrentTTF();
        _skipedItemCount = _skipListReader->GetSkippedItemCount();
        return DecodeDocBufferWithoutSkipList(lastDocIdInPrevRecord, offset, startDocId, docBufferInfo);
    }

    _skipedItemCount = _skipListReader->GetSkippedItemCount();
    currentTTF = _skipListReader->GetPrevTTF();
    _docListReader.Seek(offset);

    size_t acutalDecodeCount = 0;
    _docListReader.Decode(docBuffer, MAX_DOC_PER_RECORD, acutalDecodeCount);

    if (_isReferenceCompress) {
        firstDocId = ReferenceCompressInt32Encoder::GetFirstValue((char*)docBuffer);
    } else {
        firstDocId = docBuffer[0] + lastDocIdInPrevRecord;
    }
    return true;
}

bool InMemDocListDecoder::DecodeDocBufferWithoutSkipList(docid_t lastDocIdInPrevRecord, uint32_t offset,
                                                         docid_t startDocId, DocBufferInfo& docBufferInfo)
{
    // only decode one time
    if (_finishDecoded) {
        return false;
    }

    // TODO: seek return value
    _docListReader.Seek(offset);

    // short list when no skip
    size_t decodeCount;
    if (!_docListReader.Decode(docBufferInfo.docBuffer, MAX_DOC_PER_RECORD, decodeCount)) {
        return false;
    }

    if (_isReferenceCompress) {
        assert(decodeCount > 0);
        ReferenceCompressIntReader<uint32_t> bufferReader;
        bufferReader.Reset((char*)docBufferInfo.docBuffer);

        docBufferInfo.lastDocId = bufferReader[decodeCount - 1];
        docBufferInfo.firstDocId = bufferReader[0];
    } else {
        docBufferInfo.lastDocId = lastDocIdInPrevRecord;
        for (size_t i = 0; i < decodeCount; ++i) {
            docBufferInfo.lastDocId += docBufferInfo.docBuffer[i];
        }
        docBufferInfo.firstDocId = docBufferInfo.docBuffer[0] + lastDocIdInPrevRecord;
    }
    if (startDocId > docBufferInfo.lastDocId) {
        return false;
    }

    _finishDecoded = true;
    return true;
}

bool InMemDocListDecoder::DecodeCurrentTFBuffer(tf_t* tfBuffer)
{
    size_t decodeCount;
    return _docListReader.Decode(tfBuffer, MAX_DOC_PER_RECORD, decodeCount);
}

void InMemDocListDecoder::DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer)
{
    size_t decodeCount;
    _docListReader.Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeCount);
}

void InMemDocListDecoder::DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer)
{
    size_t decodeCount;
    _docListReader.Decode(fieldBitmapBuffer, MAX_DOC_PER_RECORD, decodeCount);
}
} // namespace indexlib::index

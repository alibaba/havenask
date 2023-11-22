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
#include "indexlib/index/inverted_index/format/DictInlinePostingDecoder.h"

#include "indexlib/index/inverted_index/format/DictInlineDecoder.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DictInlinePostingDecoder);

DictInlinePostingDecoder::DictInlinePostingDecoder(const PostingFormatOption& postingFormatOption,
                                                   int64_t dictInlinePostingData, bool isDocList, bool dfFirst)
    : _dictInlineFormatter(postingFormatOption)
    , _beginDocId(INVALID_DOCID)
    , _df(0)
    , _isDocList(isDocList)
    , _isReferenceCompress(postingFormatOption.IsReferenceCompress())
{
    if (!isDocList) {
        _dictInlineFormatter.Init(dictInlinePostingData);
    } else {
        DictInlineDecoder::DecodeContinuousDocId({dfFirst, dictInlinePostingData}, _beginDocId, _df);
    }
}

bool DictInlinePostingDecoder::DecodeDocBuffer(docid32_t startDocId, docid32_t* docBuffer, docid32_t& firstDocId,
                                               docid32_t& lastDocId, ttf_t& currentTTF)
{
    if (_isDocList) {
        return DecodeDocBufferForDocList(startDocId, docBuffer, firstDocId, lastDocId, currentTTF);
    }
    return DecodeDocBufferForSingleDoc(startDocId, docBuffer, firstDocId, lastDocId, currentTTF);
}

bool DictInlinePostingDecoder::DecodeDocBufferForDocList(docid32_t startDocId, docid32_t* docBuffer,
                                                         docid32_t& firstDocId, docid32_t& lastDocId, ttf_t& currentTTF)
{
    docid32_t endDocId = _beginDocId + _df - 1;
    if (startDocId > endDocId) {
        return false;
    }

    firstDocId = (startDocId > _beginDocId) ? startDocId : _beginDocId;
    lastDocId = std::min(endDocId, (docid32_t)(firstDocId + MAX_DOC_PER_RECORD - 1));
    currentTTF = firstDocId - _beginDocId;
    int len = lastDocId - firstDocId + 1;

    if (_isReferenceCompress) {
        for (int i = 0; i < len; i++) {
            docBuffer[i] = firstDocId + i;
        }
    } else {
        docBuffer[0] = (firstDocId == _beginDocId) ? _beginDocId : 1;
        DictInlineDecoder::FastMemSet(docBuffer + 1, len - 1, 1);
    }
    return true;
}

bool DictInlinePostingDecoder::DecodeDocBufferForSingleDoc(docid32_t startDocId, docid32_t* docBuffer,
                                                           docid32_t& firstDocId, docid32_t& lastDocId,
                                                           ttf_t& currentTTF)
{
    assert(_dictInlineFormatter.GetDocFreq() == 1);
    // df == 1, tf == ttf
    lastDocId = _dictInlineFormatter.GetDocId();
    if (startDocId > lastDocId) {
        return false;
    }

    docBuffer[0] = _dictInlineFormatter.GetDocId();
    firstDocId = _dictInlineFormatter.GetDocId();
    currentTTF = _dictInlineFormatter.GetTermFreq();
    return true;
}
} // namespace indexlib::index

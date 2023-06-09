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
#include "indexlib/index/inverted_index/format/InMemDictInlineDocListDecoder.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InMemDictInlineDocListDecoder);

void InMemDictInlineDocListDecoder::Init(const PostingFormatOption& postingFormatOption, int64_t dictInlinePostingData)
{
    assert(_dictInlineDecoder == nullptr);
    _dictInlineDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, DictInlinePostingDecoder, postingFormatOption,
                                                      dictInlinePostingData, false, true /*dfFirst*/);
}

bool InMemDictInlineDocListDecoder::DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId,
                                                    docid_t& lastDocId, ttf_t& currentTTF)
{
    if (!_dictInlineDecoder) {
        return false;
    }
    return _dictInlineDecoder->DecodeDocBuffer(startDocId, docBuffer, firstDocId, lastDocId, currentTTF);
}

bool InMemDictInlineDocListDecoder::DecodeCurrentTFBuffer(tf_t* tfBuffer)
{
    if (!_dictInlineDecoder) {
        return false;
    }
    return _dictInlineDecoder->DecodeCurrentTFBuffer(tfBuffer);
}

void InMemDictInlineDocListDecoder::DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer)
{
    if (!_dictInlineDecoder) {
        return;
    }
    return _dictInlineDecoder->DecodeCurrentDocPayloadBuffer(docPayloadBuffer);
}

void InMemDictInlineDocListDecoder::DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer)
{
    if (!_dictInlineDecoder) {
        return;
    }
    return _dictInlineDecoder->DecodeCurrentFieldMapBuffer(fieldBitmapBuffer);
}
} // namespace indexlib::index

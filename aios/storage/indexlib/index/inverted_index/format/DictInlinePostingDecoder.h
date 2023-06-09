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

#include "indexlib/index/inverted_index/format/BufferedSegmentIndexDecoder.h"
#include "indexlib/index/inverted_index/format/DictInlineFormatter.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::index {

class DictInlinePostingDecoder : public BufferedSegmentIndexDecoder
{
public:
    DictInlinePostingDecoder(const PostingFormatOption& postingFormatOption, int64_t dictInlinePostingData,
                             bool isDocList, bool dfFirst);
    ~DictInlinePostingDecoder() = default;

    bool DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                         ttf_t& currentTTF) override;
    bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t*& docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                ttf_t& currentTTF) override
    {
        return DecodeDocBuffer(startDocId, docBuffer, firstDocId, lastDocId, currentTTF);
    }

    bool DecodeCurrentTFBuffer(tf_t* tfBuffer) override
    {
        assert(tfBuffer);
        tfBuffer[0] = _dictInlineFormatter.GetTermFreq();
        return true;
    }

    void DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer) override
    {
        assert(docPayloadBuffer);
        docPayloadBuffer[0] = _dictInlineFormatter.GetDocPayload();
    }

    void DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer) override
    {
        assert(fieldBitmapBuffer);
        fieldBitmapBuffer[0] = _dictInlineFormatter.GetFieldMap();
    }

private:
    bool DecodeDocBufferForSingleDoc(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                     ttf_t& currentTTF);

    bool DecodeDocBufferForDocList(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                   ttf_t& currentTTF);

    DictInlineFormatter _dictInlineFormatter;
    docid_t _beginDocId;
    df_t _df;
    bool _isDocList;
    bool _isReferenceCompress;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index

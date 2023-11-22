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

#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"
#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"
#include "indexlib/index/inverted_index/format/BufferedSegmentIndexDecoder.h"
#include "indexlib/index/inverted_index/format/skiplist/SkipListReader.h"

namespace indexlib::index {

class InMemDocListDecoder : public BufferedSegmentIndexDecoder
{
public:
    struct DocBufferInfo {
        DocBufferInfo(docid32_t*& _docBuffer, docid32_t& _firstDocId, docid32_t& _lastDocId, ttf_t& _currentTTF)
            : docBuffer(_docBuffer)
            , firstDocId(_firstDocId)
            , lastDocId(_lastDocId)
            , currentTTF(_currentTTF)
        {
        }
        docid32_t*& docBuffer;
        docid32_t& firstDocId;
        docid32_t& lastDocId;
        ttf_t& currentTTF;
    };

    InMemDocListDecoder(autil::mem_pool::Pool* sessionPool, bool isReferenceCompress);
    ~InMemDocListDecoder();

    // skipListReader and docListBuffer must be allocated by the same pool
    void Init(df_t df, SkipListReader* skipListReader, BufferedByteSlice* docListBuffer);

    bool DecodeDocBuffer(docid32_t startDocId, docid32_t* docBuffer, docid32_t& firstDocId, docid32_t& lastDocId,
                         ttf_t& currentTTF) override;
    bool DecodeDocBufferMayCopy(docid32_t startDocId, docid32_t*& docBuffer, docid32_t& firstDocId,
                                docid32_t& lastDocId, ttf_t& currentTTF) override
    {
        return DecodeDocBuffer(startDocId, docBuffer, firstDocId, lastDocId, currentTTF);
    }
    bool DecodeCurrentTFBuffer(tf_t* tfBuffer) override;
    void DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer) override;
    void DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer) override;

private:
    bool DecodeDocBufferWithoutSkipList(docid32_t lastDocIdInPrevRecord, uint32_t offset, docid32_t startDocId,
                                        DocBufferInfo& docBufferInfo);

    autil::mem_pool::Pool* _sessionPool;
    SkipListReader* _skipListReader;
    BufferedByteSlice* _docListBuffer;
    BufferedByteSliceReader _docListReader;
    df_t _df;
    bool _finishDecoded;
    bool _isReferenceCompress;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index

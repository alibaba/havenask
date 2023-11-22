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
#include "indexlib/index/inverted_index/Constant.h"

namespace indexlib::index {

class BufferedSegmentIndexDecoder
{
public:
    BufferedSegmentIndexDecoder() : _skipedItemCount(0) {}
    virtual ~BufferedSegmentIndexDecoder() = default;

    virtual bool DecodeDocBuffer(docid32_t startDocId, docid32_t* docBuffer, docid32_t& firstDocId,
                                 docid32_t& lastDocId, ttf_t& currentTTF) = 0;
    virtual bool DecodeDocBufferMayCopy(docid32_t startDocId, docid32_t*& docBuffer, docid32_t& firstDocId,
                                        docid32_t& lastDocId, ttf_t& currentTTF) = 0;
    virtual bool DecodeCurrentTFBuffer(tf_t* tfBuffer) = 0;
    virtual void DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer) = 0;
    virtual void DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer) = 0;

    virtual void InitSkipList(uint32_t start, uint32_t end, util::ByteSliceList* postingList, df_t df,
                              CompressMode compressMode)
    {
    }

    virtual void InitSkipList(uint32_t start, uint32_t end, util::ByteSlice* postingList, df_t df,
                              CompressMode compressMode)
    {
    }

    virtual uint32_t GetSeekedDocCount() const { return InnerGetSeekedDocCount(); }

    uint32_t InnerGetSeekedDocCount() const { return _skipedItemCount << MAX_DOC_PER_RECORD_BIT_NUM; }

protected:
    uint32_t _skipedItemCount;
};

} // namespace indexlib::index

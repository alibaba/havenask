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

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/inverted_index/format/BufferedSegmentIndexDecoder.h"

namespace indexlib::index {

class ShortListSegmentDecoder : public BufferedSegmentIndexDecoder
{
public:
    ShortListSegmentDecoder(file_system::ByteSliceReader* docListReader, uint32_t docListBegin, uint8_t docCompressMode,
                            bool enableShortListVbyteCompress);
    ~ShortListSegmentDecoder() = default;

    bool DecodeDocBuffer(docid_t startDocId, docid_t* docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                         ttf_t& currentTTF) override;
    bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t*& docBuffer, docid_t& firstDocId, docid_t& lastDocId,
                                ttf_t& currentTTF) override;

    bool DecodeCurrentTFBuffer(tf_t* tfBuffer) override
    {
        _tfEncoder->Decode((uint32_t*)tfBuffer, MAX_DOC_PER_RECORD, *_docListReader);
        return true;
    }

    void DecodeCurrentDocPayloadBuffer(docpayload_t* docPayloadBuffer) override
    {
        _docPayloadEncoder->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, *_docListReader);
    }

    void DecodeCurrentFieldMapBuffer(fieldmap_t* fieldBitmapBuffer) override
    {
        _fieldMapEncoder->Decode(fieldBitmapBuffer, MAX_DOC_PER_RECORD, *_docListReader);
    }

private:
    bool _decodeShortList;
    indexlib::file_system::ByteSliceReader* _docListReader;
    uint32_t _docListBeginPos;
    const Int32Encoder* _docEncoder;
    const Int32Encoder* _tfEncoder;
    const Int16Encoder* _docPayloadEncoder;
    const Int8Encoder* _fieldMapEncoder;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index

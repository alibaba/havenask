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
#include "indexlib/index/inverted_index/format/PostingDecoder.h"
#include "indexlib/util/Bitmap.h"

namespace indexlib::index {

class BitmapPostingDecoder : public PostingDecoder
{
public:
    BitmapPostingDecoder();
    ~BitmapPostingDecoder();

public:
    void Init(TermMeta* termMeta, uint8_t* data, uint32_t size);
    uint32_t DecodeDocList(docid_t* docBuffer, size_t len);

private:
    util::BitmapPtr _bitmap;
    docid_t _docIdCursor;
    docid_t _endDocId;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index

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
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDecoder.h"

namespace indexlib::index {
namespace {
using util::Bitmap;
}

AUTIL_LOG_SETUP(indexlib.index, BitmapPostingDecoder);

BitmapPostingDecoder::BitmapPostingDecoder() : _docIdCursor(INVALID_DOCID), _endDocId(INVALID_DOCID) {}

BitmapPostingDecoder::~BitmapPostingDecoder() {}

void BitmapPostingDecoder::Init(TermMeta* termMeta, uint8_t* data, uint32_t size)
{
    _termMeta = termMeta;
    _bitmap.reset(new Bitmap);
    _bitmap->Mount(size * Bitmap::BYTE_SLOT_NUM, (uint32_t*)data);
    _docIdCursor = INVALID_DOCID;
    _endDocId = size * Bitmap::BYTE_SLOT_NUM;
}

uint32_t BitmapPostingDecoder::DecodeDocList(docid_t* docBuffer, size_t len)
{
    uint32_t retDocCount = 0;
    while (_docIdCursor < _endDocId && retDocCount < len) {
        uint32_t nextId = _bitmap->Next(_docIdCursor);
        if (nextId != Bitmap::INVALID_INDEX) {
            docBuffer[retDocCount] = (docid_t)nextId;
            retDocCount++;
            _docIdCursor = nextId;
        } else {
            _docIdCursor = _endDocId;
        }
    }

    return retDocCount;
}
} // namespace indexlib::index

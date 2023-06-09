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
#include "indexlib/index/inverted_index/format/PositionBitmapWriter.h"

#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PositionBitmapWriter);

void PositionBitmapWriter::Set(uint32_t index) { _bitmap.Set(index); }

void PositionBitmapWriter::Resize(uint32_t size) { _bitmap.ReSize(size); }

void PositionBitmapWriter::EndDocument(uint32_t df, uint32_t totalPosCount)
{
    if (df % indexlib::MAX_DOC_PER_BITMAP_BLOCK == 0) {
        _blockOffsets.push_back(totalPosCount - 1);
    }
}

uint32_t PositionBitmapWriter::GetDumpLength(uint32_t bitCount) const
{
    return VByteCompressor::GetVInt32Length(_blockOffsets.size()) + VByteCompressor::GetVInt32Length(bitCount) +
           _blockOffsets.size() * sizeof(uint32_t) + util::Bitmap::GetDumpSize(bitCount);
}

void PositionBitmapWriter::Dump(const std::shared_ptr<file_system::FileWriter>& file, uint32_t bitCount)
{
    file->WriteVUInt32(_blockOffsets.size()).GetOrThrow();
    file->WriteVUInt32(bitCount).GetOrThrow();
    for (uint32_t i = 0; i < _blockOffsets.size(); ++i) {
        file->Write((void*)(&_blockOffsets[i]), sizeof(uint32_t)).GetOrThrow();
    }
    file->Write((void*)_bitmap.GetData(), util::Bitmap::GetDumpSize(bitCount)).GetOrThrow();
}
} // namespace indexlib::index

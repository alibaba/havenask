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
#include "indexlib/index/inverted_index/format/PositionBitmapReader.h"

#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/util/Bitmap.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PositionBitmapReader);

PositionBitmapReader::PositionBitmapReader()
    : _blockCount(0)
    , _blockOffsets(nullptr)
    , _ownBlockOffsets(false)
    , _bitmapSlots(nullptr)
    , _ownBitmapSlots(false)
    , _bitmapListSize(0)
    , _totalBitCount(0)
    , _lastBlockIndex(-1)
    , _currentSlotIndex(0)
    , _preSlotSignedBitSum(0)
{
}

PositionBitmapReader::~PositionBitmapReader()
{
    if (_ownBlockOffsets && _blockOffsets) {
        delete[] _blockOffsets;
        _blockOffsets = nullptr;
    }
    if (_ownBitmapSlots && _bitmapSlots) {
        delete[] _bitmapSlots;
        _bitmapSlots = nullptr;
    }
}

uint32_t PositionBitmapReader::Init(util::ByteSlice* sliceList, uint32_t offset)
{
    _sliceReader.Open(sliceList);
    _sliceReader.Seek(offset);
    return DoInit();
}

uint32_t PositionBitmapReader::Init(const util::ByteSliceList* sliceList, uint32_t offset)
{
    _sliceReader.Open(const_cast<util::ByteSliceList*>(sliceList));
    _sliceReader.Seek(offset);
    return DoInit();
}

uint32_t PositionBitmapReader::DoInit()
{
    _blockCount = _sliceReader.ReadVUInt32();
    _totalBitCount = _sliceReader.ReadVUInt32();
    uint32_t bitmapSizeInByte = util::Bitmap::GetDumpSize(_totalBitCount);
    assert(bitmapSizeInByte % sizeof(uint32_t) == 0);

    _bitmapListSize = VByteCompressor::GetVInt32Length(_blockCount) + VByteCompressor::GetVInt32Length(_totalBitCount) +
                      _blockCount * sizeof(uint32_t) + bitmapSizeInByte;

    uint32_t blockOffsetsSizeInByte = _blockCount * sizeof(uint32_t);
    if (_sliceReader.CurrentSliceEnough(blockOffsetsSizeInByte)) {
        _ownBlockOffsets = false;
        _blockOffsets = (uint32_t*)_sliceReader.GetCurrentSliceData();
        _sliceReader.Seek(_sliceReader.Tell() + blockOffsetsSizeInByte);
    } else {
        _ownBlockOffsets = true;
        _blockOffsets = new uint32_t[_blockCount];
        for (uint32_t i = 0; i < _blockCount; ++i) {
            _blockOffsets[i] = _sliceReader.ReadUInt32();
        }
    }

    if (_sliceReader.CurrentSliceEnough(bitmapSizeInByte)) {
        _ownBitmapSlots = false;
        _bitmapSlots = (uint32_t*)_sliceReader.GetCurrentSliceData();
        _sliceReader.Seek(_sliceReader.Tell() + bitmapSizeInByte);
    } else {
        _ownBitmapSlots = true;
        _bitmapSlots = (uint32_t*)(new uint8_t[bitmapSizeInByte]);
        _sliceReader.Read((void*)_bitmapSlots, bitmapSizeInByte);
    }
    _bitmap.MountWithoutRefreshSetCount(_totalBitCount, _bitmapSlots);

    return _totalBitCount;
}
} // namespace indexlib::index

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
#include "indexlib/index/inverted_index/format/skiplist/PairValueSkipListReader.h"

#include <sstream>

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {
namespace {
}

AUTIL_LOG_SETUP(indexlib.index, PairValueSkipListReader);

PairValueSkipListReader::PairValueSkipListReader(bool isReferenceCompress)
    : _currentKey(0)
    , _currentValue(0)
    , _prevKey(0)
    , _prevValue(0)
    , _currentCursor(0)
    , _numInBuffer(0)
    , _keyBufferBase(_keyBuffer)
    , _valueBufferBase(_valueBuffer)
    , _isReferenceCompress(isReferenceCompress)
{
}

PairValueSkipListReader::PairValueSkipListReader(const PairValueSkipListReader& other)
    : _currentKey(other._currentKey)
    , _currentValue(other._currentValue)
    , _prevKey(other._prevKey)
    , _prevValue(other._prevValue)
    , _currentCursor(0)
    , _numInBuffer(0)
    , _keyBufferBase(_keyBuffer)
    , _valueBufferBase(_valueBuffer)
    , _isReferenceCompress(other._isReferenceCompress)
{
}

PairValueSkipListReader::~PairValueSkipListReader() {}

void PairValueSkipListReader::Load(const util::ByteSliceList* byteSliceList, uint32_t start, uint32_t end,
                                   const uint32_t& itemCount)
{
    SkipListReader::Load(byteSliceList, start, end);
    InnerLoad(start, end, itemCount);
}

void PairValueSkipListReader::Load(util::ByteSlice* byteSlice, uint32_t start, uint32_t end, const uint32_t& itemCount)
{
    SkipListReader::Load(byteSlice, start, end);
    InnerLoad(start, end, itemCount);
}

void PairValueSkipListReader::InnerLoad(uint32_t start, uint32_t end, const uint32_t& itemCount)
{
    _skippedItemCount = -1;
    _currentKey = 0;
    _currentValue = 0;
    _prevKey = 0;
    _prevValue = 0;
    _currentCursor = 0;
    _numInBuffer = 0;
    _keyBufferBase = _keyBuffer;
    _valueBufferBase = _valueBuffer;
    if (start >= end) {
        return;
    }
    if (itemCount <= MAX_UNCOMPRESSED_SKIP_LIST_SIZE) {
        _byteSliceReader.Read(_keyBuffer, itemCount * sizeof(_keyBuffer[0])).GetOrThrow();
        _byteSliceReader.Read(_valueBuffer, itemCount * sizeof(_valueBuffer[0])).GetOrThrow();

        _numInBuffer = itemCount;
        assert(_end == _byteSliceReader.Tell());
    }
}

std::pair<Status, bool> PairValueSkipListReader::SkipTo(uint32_t queryKey, uint32_t& key, uint32_t& prevKey,
                                                        uint32_t& value, uint32_t& delta)
{
    assert(_currentKey <= queryKey);

    uint32_t localPrevKey, localPrevValue;
    uint32_t currentKey = _currentKey;
    uint32_t currentValue = _currentValue;
    uint32_t currentCursor = _currentCursor;
    int32_t skippedItemCount = _skippedItemCount;
    uint32_t numInBuffer = _numInBuffer;
    while (true) {
        // TODO: skippedItemCount should not add after skipto failed
        skippedItemCount++;

        localPrevKey = currentKey;
        localPrevValue = currentValue;

        if (currentCursor >= numInBuffer) {
            auto [status, ret] = LoadBuffer();
            RETURN2_IF_STATUS_ERROR(status, false, "load buffer fail");
            if (!ret) {
                break;
            }
            currentCursor = _currentCursor;
            numInBuffer = _numInBuffer;
        }
        currentKey = _isReferenceCompress ? _keyBufferBase[currentCursor] : _keyBufferBase[currentCursor] + currentKey;
        currentValue += _valueBufferBase[currentCursor];
        currentCursor++;

        if (currentKey >= queryKey) {
            key = currentKey;
            prevKey = _prevKey = localPrevKey;
            value = _prevValue = localPrevValue;
            delta = currentValue - localPrevValue;
            _currentKey = currentKey;
            _currentValue = currentValue;
            _currentCursor = currentCursor;
            _skippedItemCount = skippedItemCount;

            return std::make_pair(Status::OK(), true);
        }
    }

    _currentKey = currentKey;
    _currentValue = currentValue;
    _currentCursor = currentCursor;

    _skippedItemCount = skippedItemCount;
    return std::make_pair(Status::OK(), false);
}

std::pair<Status, bool> PairValueSkipListReader::LoadBuffer()
{
    uint32_t end = _byteSliceReader.Tell();
    if (end < _end) {
        CompressMode mode = _isReferenceCompress ? REFERENCE_COMPRESS_MODE : PFOR_DELTA_COMPRESS_MODE;
        _keyBufferBase = _keyBuffer;
        EncoderProvider::EncoderParam param(mode, false, true);
        const Int32Encoder* keyEncoder = EncoderProvider::GetInstance()->GetSkipListEncoder(param);
        auto [status, keyNum] =
            keyEncoder->DecodeMayCopy(_keyBufferBase, sizeof(_keyBuffer) / sizeof(_keyBuffer[0]), _byteSliceReader);
        RETURN2_IF_STATUS_ERROR(status, false, "decode fail");

        _valueBufferBase = _valueBuffer;
        const Int32Encoder* offsetEncoder = EncoderProvider::GetInstance()->GetSkipListEncoder(param);
        auto [status1, valueNum] = offsetEncoder->DecodeMayCopy(
            _valueBufferBase, sizeof(_valueBuffer) / sizeof(_valueBuffer[0]), _byteSliceReader);
        RETURN2_IF_STATUS_ERROR(status1, false, "decode fail");

        if (keyNum != valueNum) {
            std::stringstream ss;
            ss << "SKipList decode error,"
               << "keyNum = " << keyNum << "offsetNum = " << valueNum;
            RETURN2_IF_STATUS_ERROR(Status::Corruption(), false, "%s", ss.str().c_str());
        }
        _numInBuffer = keyNum;
        _currentCursor = 0;
        return std::make_pair(Status::OK(), true);
    }
    return std::make_pair(Status::OK(), false);
}

uint32_t PairValueSkipListReader::GetLastValueInBuffer() const
{
    uint32_t lastValueInBuffer = _currentValue;
    uint32_t currentCursor = _currentCursor;
    while (currentCursor < _numInBuffer) {
        lastValueInBuffer = _isReferenceCompress ? _valueBufferBase[currentCursor]
                                                 : _valueBufferBase[currentCursor] + lastValueInBuffer;
        currentCursor++;
    }
    return lastValueInBuffer;
}

uint32_t PairValueSkipListReader::GetLastKeyInBuffer() const
{
    uint32_t lastKeyInBuffer = _currentKey;
    uint32_t currentCursor = _currentCursor;
    while (currentCursor < _numInBuffer) {
        lastKeyInBuffer =
            _isReferenceCompress ? _keyBufferBase[currentCursor] : _keyBufferBase[currentCursor] + lastKeyInBuffer;
        currentCursor++;
    }
    return lastKeyInBuffer;
}
} // namespace indexlib::index

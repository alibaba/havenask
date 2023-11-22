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
#include "indexlib/index/inverted_index/format/skiplist/TriValueSkipListReader.h"

#include <sstream>

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, TriValueSkipListReader);

TriValueSkipListReader::TriValueSkipListReader(bool isReferenceCompress)
    : _currentDocId(0)
    , _currentOffset(0)
    , _currentTTF(0)
    , _prevDocId(0)
    , _prevOffset(0)
    , _prevTTF(0)
    , _currentCursor(0)
    , _numInBuffer(0)
{
    assert(!isReferenceCompress);
}

TriValueSkipListReader::TriValueSkipListReader(const TriValueSkipListReader& other)
    : _currentDocId(other._currentDocId)
    , _currentOffset(other._currentOffset)
    , _currentTTF(other._currentTTF)
    , _prevDocId(other._prevDocId)
    , _prevOffset(other._prevOffset)
    , _prevTTF(other._prevTTF)
    , _currentCursor(0)
    , _numInBuffer(0)
{
}

TriValueSkipListReader::~TriValueSkipListReader() {}

void TriValueSkipListReader::Load(const util::ByteSliceList* byteSliceList, uint32_t start, uint32_t end,
                                  const uint32_t& itemCount)
{
    SkipListReader::Load(byteSliceList, start, end);
    InnerLoad(start, end, itemCount);
}

void TriValueSkipListReader::Load(util::ByteSlice* byteSlice, uint32_t start, uint32_t end, const uint32_t& itemCount)
{
    SkipListReader::Load(byteSlice, start, end);
    InnerLoad(start, end, itemCount);
}

void TriValueSkipListReader::InnerLoad(uint32_t start, uint32_t end, const uint32_t& itemCount)
{
    _skippedItemCount = -1;
    _currentDocId = 0;
    _currentOffset = 0;
    _currentTTF = 0;
    _prevDocId = 0;
    _prevOffset = 0;
    _prevTTF = 0;
    _currentCursor = 0;
    _numInBuffer = 0;

    if (start >= end) {
        return;
    }
    if (itemCount <= MAX_UNCOMPRESSED_SKIP_LIST_SIZE) {
        _byteSliceReader.Read(_docIdBuffer, itemCount * sizeof(_docIdBuffer[0])).GetOrThrow();
        _byteSliceReader.Read(_ttfBuffer, (itemCount - 1) * sizeof(_ttfBuffer[0])).GetOrThrow();
        _byteSliceReader.Read(_offsetBuffer, (itemCount - 1) * sizeof(_offsetBuffer[0])).GetOrThrow();

        _numInBuffer = itemCount;
        assert(_end == _byteSliceReader.Tell());
    }
}

std::pair<Status, bool> TriValueSkipListReader::SkipTo(uint32_t queryDocId, uint32_t& docId, uint32_t& prevDocId,
                                                       uint32_t& offset, uint32_t& delta)
{
    assert(_currentDocId <= queryDocId);

    int32_t skippedItemCount = _skippedItemCount;
    uint32_t currentCursor = _currentCursor;
    uint32_t currentDocId = _currentDocId;
    uint32_t currentOffset = _currentOffset;
    uint32_t currentTTF = _currentTTF;
    uint32_t numInBuffer = _numInBuffer;

    uint32_t localPrevDocId = _prevDocId;
    uint32_t localPrevOffset = _prevOffset;
    uint32_t localPrevTTF = _prevTTF;

    while (true) {
        // TODO: skippedItemCount should not add after skipto failed
        skippedItemCount++;

        localPrevDocId = currentDocId;
        localPrevOffset = currentOffset;
        localPrevTTF = currentTTF;

        if (currentCursor >= numInBuffer) {
            auto [status, ret] = LoadBuffer();
            RETURN2_IF_STATUS_ERROR(status, false, "load buffer fail");
            if (!ret) {
                break;
            }
            numInBuffer = _numInBuffer;
            currentCursor = _currentCursor;
        }
        currentDocId += _docIdBuffer[currentCursor];
        currentOffset += _offsetBuffer[currentCursor];
        currentTTF += _ttfBuffer[currentCursor];

        currentCursor++;

        if (currentDocId >= queryDocId) {
            docId = currentDocId;
            prevDocId = _prevDocId = localPrevDocId;
            offset = _prevOffset = localPrevOffset;
            delta = currentOffset - localPrevOffset;
            _prevTTF = localPrevTTF;

            _currentDocId = currentDocId;
            _currentCursor = currentCursor;
            _currentOffset = currentOffset;
            _currentTTF = currentTTF;
            _skippedItemCount = skippedItemCount;
            return std::make_pair(Status::OK(), true);
        }
    }

    _currentDocId = currentDocId;
    _currentTTF = currentTTF;
    _currentOffset = currentOffset;
    _currentCursor = currentCursor;
    _skippedItemCount = skippedItemCount;
    return std::make_pair(Status::OK(), false);
}

std::pair<Status, bool> TriValueSkipListReader::LoadBuffer()
{
    if (_byteSliceReader.Tell() < _end) {
        const Int32Encoder* docIdEncoder = EncoderProvider::GetInstance()->GetSkipListEncoder();
        auto [status, docNum] = docIdEncoder->Decode((uint32_t*)_docIdBuffer,
                                                     sizeof(_docIdBuffer) / sizeof(_docIdBuffer[0]), _byteSliceReader);
        RETURN2_IF_STATUS_ERROR(status, false, "docid decode fail");

        const Int32Encoder* tfEncoder = EncoderProvider::GetInstance()->GetSkipListEncoder();
        auto statusWith = tfEncoder->Decode(_ttfBuffer, sizeof(_ttfBuffer) / sizeof(_ttfBuffer[0]), _byteSliceReader);
        RETURN2_IF_STATUS_ERROR(statusWith.first, false, "ttf decode fail");
        uint32_t ttfNum = statusWith.second;

        const Int32Encoder* offsetEncoder = EncoderProvider::GetInstance()->GetSkipListEncoder();
        statusWith =
            offsetEncoder->Decode(_offsetBuffer, sizeof(_offsetBuffer) / sizeof(_offsetBuffer[0]), _byteSliceReader);
        RETURN2_IF_STATUS_ERROR(statusWith.first, false, "offset decode fail");
        uint32_t lenNum = statusWith.second;

        if (docNum != ttfNum || ttfNum != lenNum) {
            std::stringstream ss;
            ss << "TriValueSKipList decode error,"
               << "docNum = " << docNum << "ttfNum = " << ttfNum << "offsetNum = " << lenNum;
            RETURN2_IF_STATUS_ERROR(Status::Corruption(), false, "%s", ss.str().c_str());
        }
        _numInBuffer = docNum;
        _currentCursor = 0;
        return std::make_pair(Status::OK(), true);
    }
    return std::make_pair(Status::OK(), false);
}
} // namespace indexlib::index

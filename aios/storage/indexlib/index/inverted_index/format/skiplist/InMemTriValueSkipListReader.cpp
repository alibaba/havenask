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
#include "indexlib/index/inverted_index/format/skiplist/InMemTriValueSkipListReader.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, InMemTriValueSkipListReader);

InMemTriValueSkipListReader::InMemTriValueSkipListReader(autil::mem_pool::Pool* sessionPool)
    : _sessionPool(sessionPool)
    , _skipListBuffer(nullptr)
{
}

InMemTriValueSkipListReader::~InMemTriValueSkipListReader()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _skipListBuffer);
}

void InMemTriValueSkipListReader::Load(BufferedByteSlice* postingBuffer)
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

    BufferedByteSlice* skipListBuffer =
        IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, BufferedByteSlice, _sessionPool, _sessionPool);
    postingBuffer->SnapShot(skipListBuffer);
    _skipListBuffer = skipListBuffer;
    _skipListReader.Open(_skipListBuffer);
}

std::pair<Status, bool> InMemTriValueSkipListReader::LoadBuffer()
{
    size_t flushCount = _skipListBuffer->GetTotalCount();
    FlushInfo flushInfo = _skipListBuffer->GetFlushInfo();

    size_t decodeCount = SKIP_LIST_BUFFER_SIZE;
    // TODO: skip list reader should not use flushInfo.IsValidShortBuffer()
    if (flushInfo.GetCompressMode() == index::SHORT_LIST_COMPRESS_MODE && flushInfo.IsValidShortBuffer() == false) {
        decodeCount = flushCount;
    }
    if (decodeCount == 0) {
        return std::make_pair(Status::OK(), false);
    }

    size_t keyNum = 0;
    if (!_skipListReader.Decode(_docIdBuffer, decodeCount, keyNum)) {
        return std::make_pair(Status::OK(), false);
    }

    size_t ttfNum = 0;
    if (!_skipListReader.Decode(_ttfBuffer, decodeCount, ttfNum)) {
        return std::make_pair(Status::OK(), false);
    }

    size_t valueNum = 0;
    if (!_skipListReader.Decode(_offsetBuffer, decodeCount, valueNum)) {
        return std::make_pair(Status::OK(), false);
    }

    if (keyNum != ttfNum || ttfNum != valueNum) {
        std::stringstream ss;
        ss << "SKipList decode error,"
           << "keyNum = " << keyNum << "ttfNum = " << ttfNum << "offsetNum = " << valueNum;
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), false, "%s", ss.str().c_str());
    }
    _numInBuffer = keyNum;
    _currentCursor = 0;
    return std::make_pair(Status::OK(), true);
}

uint32_t InMemTriValueSkipListReader::GetLastValueInBuffer() const
{
    uint32_t lastValueInBuffer = _currentOffset;
    uint32_t currentCursor = _currentCursor;
    while (currentCursor < _numInBuffer) {
        lastValueInBuffer += _offsetBuffer[currentCursor];
        currentCursor++;
    }
    return lastValueInBuffer;
}

uint32_t InMemTriValueSkipListReader::GetLastKeyInBuffer() const
{
    uint32_t lastKeyInBuffer = _currentDocId;
    uint32_t currentCursor = _currentCursor;
    while (currentCursor < _numInBuffer) {
        lastKeyInBuffer += _docIdBuffer[currentCursor];
        currentCursor++;
    }
    return lastKeyInBuffer;
}
} // namespace indexlib::index

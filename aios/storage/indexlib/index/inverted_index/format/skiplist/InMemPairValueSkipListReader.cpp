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
#include "indexlib/index/inverted_index/format/skiplist/InMemPairValueSkipListReader.h"

#include <sstream>

#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, InMemPairValueSkipListReader);

InMemPairValueSkipListReader::InMemPairValueSkipListReader(autil::mem_pool::Pool* sessionPool, bool isReferenceCompress)
    : PairValueSkipListReader(isReferenceCompress)
    , _sessionPool(sessionPool)
    , _skipListBuffer(nullptr)
{
}

InMemPairValueSkipListReader::~InMemPairValueSkipListReader()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _skipListBuffer);
}

void InMemPairValueSkipListReader::Load(BufferedByteSlice* postingBuffer)
{
    _skippedItemCount = -1;
    _currentKey = 0;
    _currentValue = 0;
    _prevKey = 0;
    _prevValue = 0;
    _currentCursor = 0;
    _numInBuffer = 0;

    BufferedByteSlice* skipListBuffer =
        IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, BufferedByteSlice, _sessionPool, _sessionPool);
    postingBuffer->SnapShot(skipListBuffer);

    _skipListBuffer = skipListBuffer;
    _skipListReader.Open(_skipListBuffer);
}

std::pair<Status, bool> InMemPairValueSkipListReader::LoadBuffer()
{
    size_t keyNum = 0;
    size_t flushCount = _skipListBuffer->GetTotalCount();
    size_t decodeCount = ShortListOptimizeUtil::IsShortSkipList(flushCount) ? flushCount : SKIP_LIST_BUFFER_SIZE;
    if (!_skipListReader.Decode(_keyBuffer, decodeCount, keyNum)) {
        return std::make_pair(Status::OK(), false);
    }
    size_t valueNum = 0;
    if (!_skipListReader.Decode(_valueBuffer, decodeCount, valueNum)) {
        return std::make_pair(Status::OK(), false);
    }
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
} // namespace indexlib::index

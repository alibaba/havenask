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
#include "indexlib/util/buffer_compressor/CompressHintDataTrainer.h"

#include <math.h>

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, CompressHintDataTrainer);

CompressHintDataTrainer::CompressHintDataTrainer()
    : _maxBlockSize(0)
    , _maxBlockCount(0)
    , _sampleStepNum(0)
    , _trainBufferCapacity(0)
    , _hintData(autil::StringView::empty_instance())
{
}

CompressHintDataTrainer::~CompressHintDataTrainer() {}

bool CompressHintDataTrainer::Init(size_t maxBlockSize, size_t trainBlockCount, size_t trainBufferCapacity,
                                   float sampleRatio) noexcept
{
    assert(trainBlockCount > 0);
    if (sampleRatio <= 0 && sampleRatio > 1.0f) {
        AUTIL_LOG(ERROR, "invalid sampleRatio [%f], should be (0, 1.0f]", sampleRatio);
        return false;
    }

    _maxBlockSize = maxBlockSize;
    _maxBlockCount = trainBlockCount;
    _trainBufferCapacity = trainBufferCapacity;

    if (_maxBlockSize < 1024) {
        AUTIL_LOG(WARN, "invalid block size [%lu], should not less than 1K, set 1K by default", _maxBlockSize);
        _maxBlockSize = 1024;
    }
    if (_maxBlockSize > 1 * 1024 * 1024) {
        AUTIL_LOG(WARN, "invalid block size [%lu], should not greater than 1MB, set 1MB by default", _maxBlockSize);
        _maxBlockSize = 1 * 1024 * 1024;
    }

    size_t sampleBlockCount = (size_t)ceil((float)_maxBlockCount * sampleRatio);
    _sampleStepNum = _maxBlockCount / sampleBlockCount;
    if (_sampleStepNum < 1) {
        _sampleStepNum = 1;
    }
    AUTIL_LOG(INFO, "compress hint trainer sample step is [%lu]", _sampleStepNum);
    _trainingDataBuf.reserve(_maxBlockSize * _maxBlockCount);
    _dataOffsetVec.reserve(_maxBlockCount);
    return PrepareHintDataBuffer();
}

bool CompressHintDataTrainer::AddOneBlockData(const char* data, size_t dataLen)
{
    if (_dataOffsetVec.size() >= _maxBlockCount) {
        return false;
    }
    if (dataLen > _maxBlockSize) {
        return false;
    }
    size_t curLen = _trainingDataBuf.size();
    _trainingDataBuf.resize(curLen + dataLen);
    char* buffer = (char*)_trainingDataBuf.data();
    memcpy(buffer + curLen, data, dataLen);
    _dataOffsetVec.push_back(curLen);
    return true;
}

autil::StringView CompressHintDataTrainer::GetBlockData(size_t idx) const
{
    if (idx >= _dataOffsetVec.size()) {
        return autil::StringView::empty_instance();
    }
    char* addr = (char*)_trainingDataBuf.data() + _dataOffsetVec[idx];
    size_t len = (idx + 1) == _dataOffsetVec.size() ? _trainingDataBuf.size() - _dataOffsetVec[idx]
                                                    : _dataOffsetVec[idx + 1] - _dataOffsetVec[idx];
    return autil::StringView(addr, len);
}

void CompressHintDataTrainer::Reset()
{
    _trainingDataBuf.clear();
    _dataOffsetVec.clear();
    _hintData = autil::StringView::empty_instance();
}

}} // namespace indexlib::util

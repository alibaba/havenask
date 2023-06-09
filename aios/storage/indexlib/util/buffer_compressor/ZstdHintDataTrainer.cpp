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
#include "indexlib/util/buffer_compressor/ZstdHintDataTrainer.h"

#include <zdict.h>

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, ZstdHintDataTrainer);

ZstdHintDataTrainer::ZstdHintDataTrainer() : _cctx(nullptr) { _cctx = ZSTD_createCCtx(); }

ZstdHintDataTrainer::~ZstdHintDataTrainer()
{
    if (_cctx) {
        ZSTD_freeCCtx(_cctx);
    }
}

bool ZstdHintDataTrainer::PrepareHintDataBuffer() noexcept
{
    size_t blockCount = (_maxBlockCount + _sampleStepNum - 1) / _sampleStepNum;
    if (_trainBufferCapacity == 0) {
        // 0 means use dynamic capacity which is (default max train data data)
        _trainBufferCapacity = (size_t)(_maxBlockSize * _maxBlockCount * 0.0025f);
        AUTIL_LOG(INFO, "zstd train buffer capacity is [%lu]", _trainBufferCapacity);
    }
    _hintDataBuf.resize(_trainBufferCapacity);
    _sampleDataBuf.reserve(_maxBlockSize * blockCount);
    _sampleDataLenVec.reserve(blockCount);
    return true;
}

bool ZstdHintDataTrainer::TrainHintData()
{
    _hintData = autil::StringView::empty_instance();
    if (_trainingDataBuf.empty()) {
        return false;
    }

    _sampleDataBuf.clear();
    _sampleDataLenVec.clear();
    for (size_t idx = 0; idx < GetCurrentBlockCount(); idx += _sampleStepNum) {
        assert(IsSampleBlock(idx));
        auto sampleBlockData = GetBlockData(idx);
        size_t curLen = _sampleDataBuf.size();
        _sampleDataBuf.resize(curLen + sampleBlockData.size());
        char* buffer = (char*)_sampleDataBuf.data();
        memcpy(buffer + curLen, sampleBlockData.data(), sampleBlockData.size());
        _sampleDataLenVec.push_back(sampleBlockData.size());
    }

    char* dictPtr = (char*)_hintDataBuf.data();
    size_t len = ZDICT_trainFromBuffer(dictPtr, _hintDataBuf.size(), _sampleDataBuf.data(),
                                       (const size_t*)_sampleDataLenVec.data(), _sampleDataLenVec.size());
    if (!ZDICT_isError(len)) {
        _hintData = autil::StringView(dictPtr, len);
        return true;
    }
    AUTIL_LOG(WARN, "ZDICT_trainFromBuffer fail!");
    return false;
}

}} // namespace indexlib::util

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

#include "autil/ConstString.h"
#include "autil/Log.h"

namespace indexlib { namespace util {

class CompressHintDataTrainer
{
public:
    CompressHintDataTrainer();
    virtual ~CompressHintDataTrainer();

    CompressHintDataTrainer(const CompressHintDataTrainer&) = delete;
    CompressHintDataTrainer& operator=(const CompressHintDataTrainer&) = delete;
    CompressHintDataTrainer(CompressHintDataTrainer&&) = delete;
    CompressHintDataTrainer& operator=(CompressHintDataTrainer&&) = delete;

public:
    bool Init(size_t maxBlockSize, size_t trainBlockCount, size_t trainBufferCapacity, float sampleRatio) noexcept;

    bool AddOneBlockData(const char* data, size_t dataLen);
    size_t GetMaxTrainBlockCount() const { return _maxBlockCount; }
    size_t GetCurrentBlockCount() const { return _dataOffsetVec.size(); }
    autil::StringView GetBlockData(size_t idx) const;
    const autil::StringView& GetHintData() const { return _hintData; }

    virtual bool TrainHintData() = 0; // inner set hint data
    virtual void Reset();

protected:
    virtual bool PrepareHintDataBuffer() noexcept = 0; // should resize _hintDataBuf
    bool IsSampleBlock(size_t idx) const { return (idx % _sampleStepNum) == 0; }

protected:
    size_t _maxBlockSize;
    size_t _maxBlockCount;
    size_t _sampleStepNum;
    size_t _trainBufferCapacity;

    std::vector<char> _hintDataBuf;
    autil::StringView _hintData;

    std::vector<char> _trainingDataBuf;
    std::vector<size_t> _dataOffsetVec;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressHintDataTrainer> CompressHintDataTrainerPtr;

}} // namespace indexlib::util

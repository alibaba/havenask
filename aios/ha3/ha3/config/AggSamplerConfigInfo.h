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

#include <cstdint>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "ha3/util/TypeDefine.h"

namespace isearch {
namespace config {

class AggSamplerConfigInfo : public autil::legacy::Jsonizable
{
public:
    AggSamplerConfigInfo() {
        _aggThreshold = 0;
        _sampleStep = 1;
        _maxSortCount = 0;
        _aggBatchMode = false;
        _batchSampleMaxCount = _aggThreshold;
        _enableJit = false;
        _enableDenseMode = false;
        _tupleSep = "|";
    }

    AggSamplerConfigInfo(uint32_t aggThreadhold, uint32_t sampleStep,
                         uint32_t maxSortCount, bool batchMode = false,
                         uint32_t batchSampleMaxCount = 0)
        : _aggThreshold(aggThreadhold)
        , _sampleStep(sampleStep)
        , _maxSortCount(maxSortCount)
        , _aggBatchMode(batchMode)
        , _batchSampleMaxCount(batchSampleMaxCount)
        , _enableJit(false)
        , _enableDenseMode(false)
        , _tupleSep("|")
    {
    }

    ~AggSamplerConfigInfo() {}

public:
    void setAggThreshold(uint32_t aggThreadhold) {
        _aggThreshold = aggThreadhold;
    }

    uint32_t getAggThreshold() const {
        return _aggThreshold;
    }

    void setSampleStep(uint32_t sampleStep) {
        _sampleStep = sampleStep;
    }

    uint32_t getSampleStep() const {
        return _sampleStep;
    }

    void setMaxSortCount(uint32_t maxSortCount) {
        _maxSortCount = maxSortCount;
    }

    uint32_t getMaxSortCount() const {
        return _maxSortCount;
    }

    void setAggBatchMode(bool batchMode) {
        _aggBatchMode = batchMode;
    }

    bool getAggBatchMode() const {
        return _aggBatchMode;
    }

    uint32_t getBatchSampleMaxCount() const {
        return _batchSampleMaxCount;
    }

    void setBatchSampleMaxCount(uint32_t batchSampleMaxCount) {
        _batchSampleMaxCount = batchSampleMaxCount;
    }

    bool isEnableJit() {
        return _enableJit;
    }

    void setEnableJit(bool enableJit) {
        _enableJit = enableJit;
    }

    bool isEnableDenseMode() {
        return _enableDenseMode;
    }

    void setEnableDenseMode(bool enableDenseMode) {
        _enableDenseMode = enableDenseMode;
    }

    const std::string &getTupleSep() const {
        return _tupleSep;
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        JSONIZE(json, "aggThreshold", _aggThreshold);
        JSONIZE(json, "aggBatchMode", _aggBatchMode);
        if (json.GetMode() == FROM_JSON) {
            _batchSampleMaxCount = _aggThreshold;
        }
        JSONIZE(json, "batchSampleMaxCount", _batchSampleMaxCount);
        JSONIZE(json, "sampleStep", _sampleStep);
        JSONIZE(json, "maxSortCount", _maxSortCount);
        JSONIZE(json, "enableJit", _enableJit);
        JSONIZE(json, "enableDenseMode", _enableDenseMode);
        JSONIZE(json, "tupleSep", _tupleSep);
    }
private:
    uint32_t _aggThreshold;
    uint32_t _sampleStep;
    uint32_t _maxSortCount;
    bool _aggBatchMode;
    uint32_t _batchSampleMaxCount;
    bool _enableJit;
    bool _enableDenseMode;
    std::string _tupleSep;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggSamplerConfigInfo> AggSamplerConfigInfoPtr;

} // namespace config
} // namespace isearch

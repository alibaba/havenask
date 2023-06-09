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

#include <limits.h>
#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlibv2::config {

class DiversityConstrain : public autil::legacy::Jsonizable
{
public:
    static const int64_t DEFAULT_FILTER_MIN_VALUE = LONG_MIN;
    static const int64_t DEFAULT_FILTER_MAX_VALUE = LONG_MAX;
    static const uint64_t DEFAULT_FILTER_MASK = 0xFFFFFFFFFFFFFFFF;

public:
    DiversityConstrain() = default;
    ~DiversityConstrain() = default;

public:
    void Init();
    const std::string& GetDistField() const { return _distField; }
    uint64_t GetDistCount() const { return _distCount; }
    uint64_t GetDistExpandLimit() const { return _distExpandLimit; }
    const std::string& GetFilterField() const { return _filterField; }
    int64_t GetFilterMaxValue() const { return _filterMaxValue; }
    int64_t GetFilterMinValue() const { return _filterMinValue; }
    uint64_t GetFilterMask() const { return _filterMask; }
    const std::string& GetFilterType() const { return _filterType; }

    bool NeedDistinct() const { return !_distField.empty(); }
    bool NeedFilter() const { return !_filterField.empty(); }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool Check() const;
    bool operator==(const DiversityConstrain& other) const;

    bool ParseTimeStampFilter(int64_t beginTime, int64_t baseTime);

public:
    void SetFilterField(const std::string filterField) { _filterField = filterField; }
    bool IsFilterByMeta() const { return _filterByMeta; }
    bool IsFilterByTimeStamp() const { return _filterByTimeStamp; }

public:
    // for test
    void TEST_SetFilterType(const std::string& filterType) { _filterType = filterType; }
    void TEST_SetFilterParameter(const std::string& filterParameter) { _filterParameter = filterParameter; }
    void TEST_SetFilterMaxValue(int64_t filterMaxValue) { _filterMaxValue = filterMaxValue; }
    void TEST_SetFilterMinValue(int64_t filterMinValue) { _filterMinValue = filterMinValue; }
    void TEST_SetFilterMask(uint64_t filterMask) { _filterMask = filterMask; }

private:
    using KVMap = std::map<std::string, std::string>;
    bool ParseFilterParam();
    bool ParseDefaultFilterParam(const KVMap& paramMap);
    bool ParseFilterByTimeStampParam(const KVMap& paramMap, int64_t beginTime, int64_t baseTime);
    bool ParseFilterByMetaParam(const KVMap& paramMap);
    bool UpdateFilterFieldName(const KVMap& paramMap);
    KVMap GetKVMap(const std::string& paramsStr);
    int64_t GetTimeFromString(const std::string& str) const;

private:
    std::string _distField;
    std::string _filterField;
    uint64_t _distCount = 0;
    uint64_t _distExpandLimit = 0;
    int64_t _filterMaxValue = DEFAULT_FILTER_MAX_VALUE;
    int64_t _filterMinValue = DEFAULT_FILTER_MIN_VALUE;
    uint64_t _filterMask = DEFAULT_FILTER_MASK;
    std::string _filterType;
    std::string _filterParameter;
    bool _filterByMeta = false;
    bool _filterByTimeStamp = false;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config

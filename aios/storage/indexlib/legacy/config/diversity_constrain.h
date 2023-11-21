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
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class DiversityConstrain : public autil::legacy::Jsonizable
{
public:
    static const int64_t DEFAULT_FILTER_MIN_VALUE = LONG_MIN;
    static const int64_t DEFAULT_FILTER_MAX_VALUE = LONG_MAX;
    static const uint64_t DEFAULT_FILTER_MASK = 0xFFFFFFFFFFFFFFFF;

private:
    typedef std::map<std::string, std::string> KVMap;

public:
    DiversityConstrain();
    ~DiversityConstrain();

public:
    void Init();
    const std::string& GetDistField() const { return mDistField; }
    uint64_t GetDistCount() const { return mDistCount; }
    uint64_t GetDistExpandLimit() const { return mDistExpandLimit; }
    const std::string& GetFilterField() const { return mFilterField; }
    int64_t GetFilterMaxValue() const { return mFilterMaxValue; }
    int64_t GetFilterMinValue() const { return mFilterMinValue; }
    uint64_t GetFilterMask() const { return mFilterMask; }

    bool NeedDistinct() const { return !mDistField.empty(); }
    bool NeedFilter() const { return !mFilterField.empty(); }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator==(const DiversityConstrain& other) const;

public:
    // for test
    void SetDistField(const std::string& distField) { mDistField = distField; }

    void SetDistCount(uint64_t distCount) { mDistCount = distCount; }

    void SetDistExpandLimit(uint64_t distExpandLimit) { mDistExpandLimit = distExpandLimit; }

    void SetFilterField(const std::string filterField) { mFilterField = filterField; }

    void SetFilterMaxValue(int64_t filterMaxValue) { mFilterMaxValue = filterMaxValue; }

    void SetFilterMinValue(int64_t filterMinValue) { mFilterMinValue = filterMinValue; }

    void SetFilterMask(uint64_t filterMask) { mFilterMask = filterMask; }

    void SetFilterByMeta(bool flag) { mFilterByMeta = flag; }

    void SetFilterByTimeStamp(bool flag) { mFilterByTimeStamp = flag; }

    const std::string& GetFilterType() const { return mFilterType; }

    bool IsFilterByMeta() const { return mFilterByMeta; }

    bool IsFilterByTimeStamp() const { return mFilterByTimeStamp; }

    void ParseTimeStampFilter(int64_t beginTime, int64_t baseTime);

private:
    void ParseDefaultFilterParam(KVMap& paramMap);

    void GetFilterFieldName(KVMap& paramMap);

    void ParseFilterByTimeStampParam(KVMap& paramMap, int64_t beginTime, int64_t baseTime);

    void ParseFilterByMetaParam(KVMap& paramMap);

    void ParseFilterParam();

    KVMap GetKVMap(const std::string& paramsStr);

    int64_t GetTimeFromString(const std::string& str);

private:
    std::string mDistField;
    std::string mFilterField;
    uint64_t mDistCount;
    uint64_t mDistExpandLimit;
    int64_t mFilterMaxValue;
    int64_t mFilterMinValue;
    uint64_t mFilterMask;
    std::string mFilterType;
    std::string mFilterParameter;
    bool mFilterByMeta;
    bool mFilterByTimeStamp;

private:
    AUTIL_LOG_DECLARE();
    friend class DiversityConstrainTest;
};

typedef std::shared_ptr<DiversityConstrain> DiversityConstrainPtr;
}} // namespace indexlib::config

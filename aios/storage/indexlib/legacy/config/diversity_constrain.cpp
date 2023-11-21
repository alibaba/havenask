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
#include "indexlib/config/diversity_constrain.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/config/config_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, DiversityConstrain);

#define FIELD_NAME "fieldName"
#define FILTER_MASK "filterMask"
#define FILTER_MIN_VALUE "filterMinValue"
#define FILTER_MAX_VALUE "filterMaxValue"

#define BEGIN_TIME "beginTime"
#define ADVISE_END_TIME "adviseEndTime"
#define BASE_DATE "baseDate"
#define MIN_INTERVAL "minInterval"

DiversityConstrain::DiversityConstrain()
    : mDistCount(0)
    , mDistExpandLimit(0)
    , mFilterMaxValue(DEFAULT_FILTER_MAX_VALUE)
    , mFilterMinValue(DEFAULT_FILTER_MIN_VALUE)
    , mFilterMask(DEFAULT_FILTER_MASK)
    , mFilterByMeta(false)
    , mFilterByTimeStamp(false)
{
}

DiversityConstrain::~DiversityConstrain() {}

void DiversityConstrain::Init()
{
    if (!mFilterType.empty()) {
        // new config format
        ParseFilterParam();
        return;
    }
}

void DiversityConstrain::ParseDefaultFilterParam(KVMap& paramMap)
{
    // "FilterParameter" : "fieldName=DOC_PAYLOAD;filterMask=0xFFFF;filterMinValue=1;filterMaxValue=100"
    GetFilterFieldName(paramMap);

    string filterMin = paramMap[FILTER_MIN_VALUE];
    string filterMax = paramMap[FILTER_MAX_VALUE];
    string filterMask = paramMap[FILTER_MASK];

    if (!filterMask.empty() && !StringUtil::hexStrToUint64(filterMask.c_str(), mFilterMask)) {
        IE_CONFIG_ASSERT_PARAMETER_VALID(false, "parse filterMask fail in Default FilterType");
    }

    if (!filterMin.empty() && !StringUtil::fromString(filterMin.c_str(), mFilterMinValue)) {
        IE_CONFIG_ASSERT_PARAMETER_VALID(false, "parse filterMinValue fail in Default FilterType");
    }

    if (!filterMax.empty() && !StringUtil::fromString(filterMax.c_str(), mFilterMaxValue)) {
        IE_CONFIG_ASSERT_PARAMETER_VALID(false, "parse filterMask fail in Default FilterType");
    }
}

void DiversityConstrain::GetFilterFieldName(KVMap& paramMap)
{
    if (paramMap.find(FIELD_NAME) != paramMap.end()) {
        mFilterField = paramMap[FIELD_NAME];
    }
    if (mFilterField.empty()) {
        IE_CONFIG_ASSERT_PARAMETER_VALID(false, "fieldName not exist");
    }
}

int64_t DiversityConstrain::GetTimeFromString(const string& str)
{
    struct tm tm;
    strptime(str.c_str(), "%H:%M:%S", &tm);
    return tm.tm_hour * 3600 + tm.tm_min * 60 + tm.tm_sec;
}

void DiversityConstrain::ParseTimeStampFilter(int64_t beginTime, int64_t baseTime)
{
    if (mFilterType != "FilterByTimeStamp") {
        return;
    }
    KVMap paramMap = GetKVMap(mFilterParameter);
    ParseFilterByTimeStampParam(paramMap, beginTime, baseTime);
    Check();
}

void DiversityConstrain::ParseFilterByTimeStampParam(KVMap& paramMap, int64_t beginTime, int64_t baseTime)
{
    //"fieldName=ends_1;beginTime=18:00:00;adviseEndTime=23:00:00;minInterval=6"
    GetFilterFieldName(paramMap);
    mFilterByTimeStamp = true;

    string beginTimeStr = paramMap[BEGIN_TIME];
    string adviseEndTime = paramMap[ADVISE_END_TIME];
    string minInterval = paramMap[MIN_INTERVAL];

    if (beginTimeStr.empty() || (adviseEndTime.empty() && minInterval.empty())) {
        stringstream ss;
        ss << "beginTime:" << beginTimeStr << ", adviseEndTime:" << adviseEndTime << ", minInterval: " << minInterval;
        IE_CONFIG_ASSERT_PARAMETER_VALID(false, ss.str());
    }

    if (beginTimeStr == "now") {
        mFilterMinValue = beginTime;
    } else {
        mFilterMinValue = GetTimeFromString(beginTimeStr) + baseTime;
    }
    IE_CONFIG_ASSERT_PARAMETER_VALID(mFilterMinValue > 0, "beginTime should greater than 0");
    int64_t maxTime = 0;
    if (!adviseEndTime.empty()) {
        maxTime = GetTimeFromString(adviseEndTime) + baseTime;
    }
    if (!minInterval.empty()) {
        int64_t t = 0;
        if (!StringUtil::fromString(minInterval, t)) {
            IE_CONFIG_ASSERT_PARAMETER_VALID(false, "invalid minInterval, check it!");
        }
        t *= 3600; // minInterval is measured as hour
        t += mFilterMinValue;
        maxTime = t > maxTime ? t : maxTime;
    }
    mFilterMaxValue = maxTime;
    AUTIL_LOG(INFO, "filterByTimestamp baseTime %ld, beginTime %ld, endTime %ld", baseTime, mFilterMinValue,
              mFilterMaxValue);
}

void DiversityConstrain::ParseFilterParam()
{
    KVMap paramMap = GetKVMap(mFilterParameter);
    if (mFilterType == "Default") {
        if (paramMap.empty()) {
            stringstream ss;
            ss << "FilterType: " << mFilterType << ", FilterParameter: " << mFilterParameter;
            IE_CONFIG_ASSERT_PARAMETER_VALID(false, ss.str());
        }
        ParseDefaultFilterParam(paramMap);
        return;
    } else if (mFilterType == "FilterByMeta") {
        ParseFilterByMetaParam(paramMap);
        return;
    } else if (mFilterType == "FilterByTimeStamp") {
        if (paramMap.empty()) {
            stringstream ss;
            ss << "FilterType: " << mFilterType << ", FilterParameter: " << mFilterParameter;
            IE_CONFIG_ASSERT_PARAMETER_VALID(false, ss.str());
        }
        // delay parse in TruncateIndexWriterCreator
        return;
    }
    IE_CONFIG_ASSERT_PARAMETER_VALID(false, "unsupport FilterType");
}

void DiversityConstrain::ParseFilterByMetaParam(KVMap& paramMap)
{
    // "FilterParameter" : "fieldName=ends"
    GetFilterFieldName(paramMap);
    mFilterByMeta = true;
}

DiversityConstrain::KVMap DiversityConstrain::GetKVMap(const string& paramsStr)
{
    KVMap paramMap;
    vector<std::string> params;
    params = StringUtil::split(paramsStr, string(";"));
    for (size_t i = 0; i < params.size(); ++i) {
        string key;
        string value;
        StringUtil::getKVValue(params[i], key, value, "=", true);
        if (!key.empty() && !value.empty()) {
            paramMap[key] = value;
        }
    }
    return paramMap;
}

void DiversityConstrain::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("distinct_field", mDistField, mDistField);
    json.Jsonize("distinct_count", mDistCount, mDistCount);
    json.Jsonize("distinct_expand_limit", mDistExpandLimit, mDistExpandLimit);

    json.Jsonize("filter_type", mFilterType, mFilterType);
    json.Jsonize("filter_parameter", mFilterParameter, mFilterParameter);

    json.Jsonize("filter_field", mFilterField, mFilterField);
    json.Jsonize("filter_max_value", mFilterMaxValue, mFilterMaxValue);
    json.Jsonize("filter_min_value", mFilterMinValue, mFilterMinValue);
    json.Jsonize("filter_by_meta", mFilterByMeta, mFilterByMeta);
    json.Jsonize("filter_by_time_stamp", mFilterByTimeStamp, mFilterByTimeStamp);

    if (FROM_JSON == json.GetMode()) {
        std::string filterMaskStr;
        json.Jsonize("filter_mask", filterMaskStr, filterMaskStr);
        if (!filterMaskStr.empty() && !autil::StringUtil::hexStrToUint64(filterMaskStr.c_str(), mFilterMask)) {
            mFilterMask = 0;
        }
    } else {
        char filterMaskBuff[32] = {0};
        autil::StringUtil::uint64ToHexStr(mFilterMask, filterMaskBuff, sizeof(filterMaskBuff));
        std::string filterMaskStr(filterMaskBuff);
        json.Jsonize("filter_mask", filterMaskStr, filterMaskStr);
    }
}

void DiversityConstrain::Check() const
{
    bool valid = true;
    if (NeedDistinct()) {
        valid = mDistCount > 0 && mDistExpandLimit >= mDistCount;

        IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "invalid distinct config for DiversityConstrain");
    }

    if (NeedFilter()) {
        valid = mFilterMinValue <= mFilterMaxValue && mFilterMask > 0 && !(mFilterByTimeStamp && mFilterByMeta);
        IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "invalid filter config for DiversityConstrain");
    }
}

bool DiversityConstrain::operator==(const DiversityConstrain& other) const
{
    return mDistCount == other.mDistCount && mDistExpandLimit == other.mDistExpandLimit &&
           mDistField == other.mDistField && mFilterField == other.mFilterField &&
           mFilterMaxValue == other.mFilterMaxValue && mFilterMinValue == other.mFilterMinValue &&
           mFilterMask == other.mFilterMask && mFilterByTimeStamp == other.mFilterByTimeStamp &&
           mFilterByMeta == other.mFilterByMeta;
}
}} // namespace indexlib::config

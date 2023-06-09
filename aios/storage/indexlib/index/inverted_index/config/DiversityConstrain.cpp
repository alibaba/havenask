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
#include "indexlib/index/inverted_index/config/DiversityConstrain.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, DiversityConstrain);

#define FIELD_NAME "fieldName"
#define FILTER_MASK "filterMask"
#define FILTER_MIN_VALUE "filterMinValue"
#define FILTER_MAX_VALUE "filterMaxValue"

#define BEGIN_TIME "beginTime"
#define ADVISE_END_TIME "adviseEndTime"
#define BASE_DATE "baseDate"
#define MIN_INTERVAL "minInterval"

void DiversityConstrain::Init()
{
    if (!_filterType.empty()) {
        // new config format
        ParseFilterParam();
        return;
    }
}

void DiversityConstrain::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("distinct_field", _distField, _distField);
    json.Jsonize("distinct_count", _distCount, _distCount);
    json.Jsonize("distinct_expand_limit", _distExpandLimit, _distExpandLimit);

    json.Jsonize("filter_type", _filterType, _filterType);
    json.Jsonize("filter_parameter", _filterParameter, _filterParameter);

    json.Jsonize("filter_field", _filterField, _filterField);
    json.Jsonize("filter_max_value", _filterMaxValue, _filterMaxValue);
    json.Jsonize("filter_min_value", _filterMinValue, _filterMinValue);
    json.Jsonize("filter_by_meta", _filterByMeta, _filterByMeta);
    json.Jsonize("filter_by_time_stamp", _filterByTimeStamp, _filterByTimeStamp);

    if (FROM_JSON == json.GetMode()) {
        std::string filterMaskStr;
        json.Jsonize("filter_mask", filterMaskStr, filterMaskStr);
        if (!filterMaskStr.empty() && !autil::StringUtil::hexStrToUint64(filterMaskStr.c_str(), _filterMask)) {
            _filterMask = 0;
        }
    } else {
        char filterMaskBuff[32] = {0};
        autil::StringUtil::uint64ToHexStr(_filterMask, filterMaskBuff, sizeof(filterMaskBuff));
        std::string filterMaskStr(filterMaskBuff);
        json.Jsonize("filter_mask", filterMaskStr, filterMaskStr);
    }
}

bool DiversityConstrain::ParseDefaultFilterParam(const KVMap& paramMap)
{
    // "FilterParameter" : "fieldName=DOC_PAYLOAD;filterMask=0xFFFF;filterMinValue=1;filterMaxValue=100"
    if (!UpdateFilterFieldName(paramMap)) {
        AUTIL_LOG(ERROR, "update filter field failed.");
        return false;
    }
    auto getValueFromMap = [](const KVMap& paramMap, const std::string& key) {
        auto it = paramMap.find(key);
        if (it != paramMap.end()) {
            return it->second;
        } else {
            return std::string("");
        }
    };

    const std::string filterMin = getValueFromMap(paramMap, FILTER_MIN_VALUE);
    const std::string filterMax = getValueFromMap(paramMap, FILTER_MAX_VALUE);
    const std::string filterMask = getValueFromMap(paramMap, FILTER_MASK);

    if (!filterMask.empty() && !autil::StringUtil::hexStrToUint64(filterMask.c_str(), _filterMask)) {
        AUTIL_LOG(ERROR, "parse filter mask failed.");
        return false;
    }

    if (!filterMin.empty() && !autil::StringUtil::fromString(filterMin.c_str(), _filterMinValue)) {
        AUTIL_LOG(ERROR, "parse filter min value failed.");
        return false;
    }

    if (!filterMax.empty() && !autil::StringUtil::fromString(filterMax.c_str(), _filterMaxValue)) {
        AUTIL_LOG(ERROR, "parse filter mask failed.");
        return false;
    }
    return true;
}

bool DiversityConstrain::UpdateFilterFieldName(const KVMap& paramMap)
{
    auto it = paramMap.find(FIELD_NAME);
    if (it == paramMap.end()) {
        AUTIL_LOG(ERROR, "field_name not exist in map.");
        return false;
    }
    _filterField = it->second;

    if (_filterField.empty()) {
        AUTIL_LOG(ERROR, "empty filter field.");
        return false;
    }
    return true;
}

int64_t DiversityConstrain::GetTimeFromString(const std::string& str) const
{
    struct tm tm;
    strptime(str.c_str(), "%H:%M:%S", &tm);
    return tm.tm_hour * 3600 + tm.tm_min * 60 + tm.tm_sec;
}

bool DiversityConstrain::ParseTimeStampFilter(int64_t beginTime, int64_t baseTime)
{
    if (_filterType != "FilterByTimeStamp") {
        return false;
    }
    KVMap paramMap = GetKVMap(_filterParameter);
    if (!ParseFilterByTimeStampParam(paramMap, beginTime, baseTime)) {
        AUTIL_LOG(ERROR, "parse filter by timestamp failed.");
        return false;
    }
    if (!Check()) {
        AUTIL_LOG(ERROR, "check param failed.");
        return false;
    }
    return true;
}

bool DiversityConstrain::ParseFilterByTimeStampParam(const KVMap& paramMap, int64_t beginTime, int64_t baseTime)
{
    //"fieldName=ends_1;beginTime=18:00:00;adviseEndTime=23:00:00;minInterval=6"
    if (!UpdateFilterFieldName(paramMap)) {
        AUTIL_LOG(ERROR, "update filter field by timestamp failed.");
        return false;
    }
    _filterByTimeStamp = true;
    auto getValueFromMap = [](const KVMap& paramMap, const std::string& key) {
        auto it = paramMap.find(key);
        if (it != paramMap.end()) {
            return it->second;
        } else {
            return std::string("");
        }
    };

    const std::string beginTimeStr = getValueFromMap(paramMap, BEGIN_TIME);
    const std::string adviseEndTime = getValueFromMap(paramMap, ADVISE_END_TIME);
    const std::string minInterval = getValueFromMap(paramMap, MIN_INTERVAL);
    if (beginTimeStr.empty() || (adviseEndTime.empty() && minInterval.empty())) {
        AUTIL_LOG(ERROR, "invalid param. beginTime:%s adviseEndTime:%s minInterval:%s", beginTimeStr.c_str(),
                  adviseEndTime.c_str(), minInterval.c_str());
        return false;
    }

    if (beginTimeStr == "now") {
        _filterMinValue = beginTime;
    } else {
        _filterMinValue = GetTimeFromString(beginTimeStr) + baseTime;
    }
    if (_filterMinValue <= 0) {
        AUTIL_LOG(ERROR, "invalid filter min value, filterMinValue:%ld begin:%ld base:%ld beginTimeStr:%s",
                  _filterMinValue, beginTime, baseTime, beginTimeStr.c_str());
        return false;
    }
    int64_t maxTime = 0;
    if (!adviseEndTime.empty()) {
        maxTime = GetTimeFromString(adviseEndTime) + baseTime;
    }
    if (!minInterval.empty()) {
        int64_t t = 0;
        if (!autil::StringUtil::fromString(minInterval, t)) {
            AUTIL_LOG(ERROR, "invalid minInterval.");
            return false;
        }
        t *= 3600; // minInterval is measured as hour
        t += _filterMinValue;
        maxTime = t > maxTime ? t : maxTime;
    }
    _filterMaxValue = maxTime;
    AUTIL_LOG(INFO, "filterByTimestamp baseTime %ld, beginTime %ld, endTime %ld", baseTime, _filterMinValue,
              _filterMaxValue);
    return true;
}

bool DiversityConstrain::ParseFilterParam()
{
    KVMap paramMap = GetKVMap(_filterParameter);
    if (paramMap.empty()) {
        AUTIL_LOG(ERROR, "empty params after parsed, type:%s, param:%s", _filterType.c_str(), _filterParameter.c_str());
        return false;
    }
    if (_filterType == "Default") {
        if (!ParseDefaultFilterParam(paramMap)) {
            AUTIL_LOG(ERROR, "parse default filter param failed.");
            return false;
        }
        return true;
    } else if (_filterType == "FilterByMeta") {
        if (!ParseFilterByMetaParam(paramMap)) {
            AUTIL_LOG(ERROR, "parse filter by meta failed.");
            return false;
        }
        return true;
    } else if (_filterType == "FilterByTimeStamp") {
        // delay parse in TruncateIndexWriterCreator
        return true;
    } else {
        AUTIL_LOG(ERROR, "unsupport filter type:%s", _filterType.c_str());
        assert(false);
        return false;
    }
}

bool DiversityConstrain::ParseFilterByMetaParam(const KVMap& paramMap)
{
    // "FilterParameter" : "fieldName=ends"
    if (!UpdateFilterFieldName(paramMap)) {
        AUTIL_LOG(ERROR, "update filter field failed.");
        return false;
    }
    _filterByMeta = true;
    return true;
}

DiversityConstrain::KVMap DiversityConstrain::GetKVMap(const std::string& paramsStr)
{
    KVMap paramMap;
    std::vector<std::string> params;
    params = autil::StringUtil::split(paramsStr, std::string(";"));
    for (size_t i = 0; i < params.size(); ++i) {
        std::string key;
        std::string value;
        autil::StringUtil::getKVValue(params[i], key, value, "=", true);
        if (!key.empty() && !value.empty()) {
            paramMap[key] = value;
        }
    }
    return paramMap;
}

bool DiversityConstrain::Check() const
{
    if (NeedDistinct()) {
        if (_distCount <= 0 || _distExpandLimit < _distCount) {
            AUTIL_LOG(ERROR, "invalid distinct config.");
            return false;
        }
    }

    if (NeedFilter()) {
        if (_filterMinValue > _filterMaxValue || _filterMask <= 0 || (_filterByTimeStamp && _filterByMeta)) {
            AUTIL_LOG(ERROR, "invalid filter config.");
            return false;
        }
    }
    return true;
}

bool DiversityConstrain::operator==(const DiversityConstrain& other) const
{
    return _distCount == other._distCount && _distExpandLimit == other._distExpandLimit &&
           _distField == other._distField && _filterField == other._filterField &&
           _filterMaxValue == other._filterMaxValue && _filterMinValue == other._filterMinValue &&
           _filterMask == other._filterMask && _filterByTimeStamp == other._filterByTimeStamp &&
           _filterByMeta == other._filterByMeta;
}

} // namespace indexlibv2::config

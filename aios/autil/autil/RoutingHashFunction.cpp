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
#include "autil/RoutingHashFunction.h"

#include <stdio.h>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <set>
#include <cmath>

#include "autil/CommonMacros.h"
#include "autil/HashAlgorithm.h"
#include "autil/HashFuncFactory.h"
#include "autil/StringUtil.h"

using namespace std;
namespace autil {
AUTIL_LOG_SETUP(autil, RoutingHashFunction);

static const string ROUTING_RATIO_KEY = "routing_ratio";
static const string HOT_VALUES_KEY = "hot_values";
static const string HOT_VALUES_RATIO_KEY = "hot_values_ratio";
static const string HOT_RANGES_KEY = "hot_ranges";
static const string HOT_RANGES_RATIO_KEY = "hot_ranges_ratio";
static const string CAN_EMPTY_FIELDS_KEY = "can_empty_fields";
static const string HASH_FUNC_KEY = "hash_func";
static const string MY_HASH_FUNC_NAME = "ROUTING_HASH";
static const string VALUES_SEP = ";";
static const string VALUE_SEP = ",";
static const string RANGE_SEP = "-";

bool RoutingHashFunction::init() {
    if (_hashSize == 0) {
        return false;
    }
    auto iter = _params.find(ROUTING_RATIO_KEY);
    if (iter != _params.end()) {
        float routingRatio = 0;
        if (!StringUtil::fromString(iter->second, routingRatio)) {
            AUTIL_LOG(WARN, "parse ratio [%s] failed", iter->second.c_str());
            return false;
        }
        if (routingRatio < 0 || routingRatio > 1.0) {
            AUTIL_LOG(WARN, "hot values ratio [%f] not in [0, 1].]", routingRatio);
            return false;
        }
        _routingRatio = routingRatio;
    }
    _hotRangeVec.resize(_hashSize, _routingRatio);
    iter = _params.find(CAN_EMPTY_FIELDS_KEY);
    if (iter != _params.end()) {
        vector<string> fields = StringUtil::split(iter->second, VALUE_SEP);
        for (auto field : fields) {
            StringUtil::trim(field);
            _canEmptyFields.insert(field);
        }
    }
    iter = _params.find(HASH_FUNC_KEY);
    if (iter != _params.end()) {
        if (!initFuncBase(iter->second)) {
            AUTIL_LOG(WARN, "init hash_func failed [%s].", iter->second.c_str());
            return false;
        }
    }
    auto iter1 = _params.find(HOT_VALUES_KEY);
    auto iter2 = _params.find(HOT_VALUES_RATIO_KEY);
    if (iter1 != _params.end() && iter2 != _params.end()) {
        if (!parseHotValues(iter1->second, iter2->second)) {
            return false;
        } else {
            AUTIL_LOG(DEBUG, "add hot values [%s], ratio [%s]",
                      iter1->second.c_str(), iter2->second.c_str());
        }
    }
    iter1 = _params.find(HOT_RANGES_KEY);
    iter2 = _params.find(HOT_RANGES_RATIO_KEY);
    if (iter1 != _params.end() && iter2 != _params.end()) {
        if (!parseHotRanges(iter1->second, iter2->second)) {
            return false;
        } else {
            AUTIL_LOG(DEBUG, "add hot ranges [%s], ratio [%s]",
                      iter1->second.c_str(), iter2->second.c_str());
        }
    }
    return true;
}

bool RoutingHashFunction::parseHotValues(const string& key, const string& value) {
    const vector<string> &hotValuesVec = StringUtil::split(key, VALUES_SEP);
    vector<string> hotValuesRatioVec = StringUtil::split(value, VALUES_SEP);
    if (hotValuesVec.size() != hotValuesRatioVec.size()) {
        AUTIL_LOG(WARN, "size of hot values/ratios not equal [%s, %s]",
                  key.c_str(), value.c_str());
        return false;
    }
    for (size_t i = 0; i < hotValuesVec.size(); i++) {
        float routingRatio;
        StringUtil::trim(hotValuesRatioVec[i]);
        if (!StringUtil::fromString(hotValuesRatioVec[i], routingRatio)) {
            AUTIL_LOG(WARN, "parse hot values ratio [%s] failed.]", hotValuesRatioVec[i].c_str());
            return false;
        }
        if (routingRatio < 0 || routingRatio > 1) {
            AUTIL_LOG(WARN, "hot values ratio [%f] not in [0, 1].]", routingRatio);
            return false;
        }
        vector<string> hotValues = StringUtil::split(hotValuesVec[i], VALUE_SEP);
        for (size_t j = 0; j < hotValues.size(); j++) {
            StringUtil::trim(hotValues[j]);
            _hotValueMap[hotValues[j]] = routingRatio;
        }
    }
    return true;
}

bool RoutingHashFunction:: parseHotRanges(const string& key, const string& value) {
    const vector<string> &hotRangesVec = StringUtil::split(key, VALUES_SEP);
    vector<string> hotRangesRatioVec = StringUtil::split(value, VALUES_SEP);
    if (hotRangesVec.size() != hotRangesRatioVec.size()) {
        AUTIL_LOG(WARN, "size of hot ranges/ratios not equal [%s, %s]",
                  key.c_str(), value.c_str());
        return false;
    }
    for (size_t i = 0; i < hotRangesVec.size(); i++) {
        float routingRatio;
        StringUtil::trim(hotRangesRatioVec[i]);
        if (!StringUtil::fromString(hotRangesRatioVec[i], routingRatio)) {
            AUTIL_LOG(WARN, "parse hot ranges ratio [%s] failed.]", hotRangesRatioVec[i].c_str());
            return false;
        }
        if (routingRatio < 0 || routingRatio > 1) {
            AUTIL_LOG(WARN, "hot values ratio [%f] not in [0, 1].]", routingRatio);
            return false;
        }
        vector<string> hotRanges = StringUtil::split(hotRangesVec[i], VALUE_SEP);
        uint32_t start, end;
        for (size_t j = 0; j < hotRanges.size(); j++) {
            StringUtil::trim(hotRanges[j]);
            int ret = sscanf(hotRanges[j].c_str(), "%u-%u", &start, &end);
            if (ret != 2) {
                AUTIL_LOG(WARN, "parse hot ranges [%s] faild.", hotRanges[j].c_str());
                return false;
            }
            if (start > end ) {
                AUTIL_LOG(WARN, "range start [%u] greater than end [%u].", start, end);
                return false;
            }
            if (end >= _hashSize ) {
                AUTIL_LOG(WARN, "end [%u] greater than hash size [%u].", end, _hashSize);
                return false;
            }
            for (uint32_t k = start; k <= end; k++) {
                _hotRangeVec[k] = routingRatio;
            }
        }
    }
    return true;
}

bool RoutingHashFunction::initFuncBase(const std::string& funcName) {
    string name = funcName;
    StringUtil::trim(name);
    if (name.empty() || name == MY_HASH_FUNC_NAME) {
        return true;
    }
    _funcBase = HashFuncFactory::createHashFunc(name, _hashSize);
    return _funcBase != nullptr;
}

uint32_t RoutingHashFunction::getHashId(const string& str) const {
    return getLogicId(str);
}

uint32_t RoutingHashFunction::getHashId(const char *buf, size_t len) const {
    return getLogicId(buf, len);
}

uint32_t RoutingHashFunction::getHashId(const vector<string>& strVec) const {
    if (strVec.size() == 0) {
        return 0;
    } else if (strVec.size() == 1) {
        return getHashId(strVec[0]);
    } else if (strVec.size() == 2) {
        return getHashId(strVec[0], strVec[1]);
    } else {
        if (strVec[2].empty()){ 
            return getHashId(strVec[0], strVec[1]);
        } else {
            float routingRatio = 0;
            if (!StringUtil::fromString(strVec[2], routingRatio)) {
                routingRatio = _routingRatio;
                AUTIL_LOG(WARN, "parse ratio [%s] failed, use default ratio [%f]",
                        strVec[2].c_str(), routingRatio);
            }
            if (routingRatio < 0) {
                AUTIL_LOG(WARN, "ratio [%f] is less than 0, use 0.", routingRatio);
                routingRatio = 0;
            }
            if (routingRatio > 1) {
                AUTIL_LOG(WARN, "ratio [%f] is greater than 1, use 1", routingRatio);
                routingRatio = 1;
            }
            return getHashId(strVec[0], strVec[1], routingRatio);
        }
    }
}

uint32_t RoutingHashFunction::getHashId(const string& str1, const string& str2) const {
    uint32_t first = getLogicId(str1);
    uint32_t secondLogicId = getLogicId(str2);
    uint32_t second = floor(secondLogicId * _hotRangeVec[first]);
    auto iter = _hotValueMap.find(str1);
    if (iter != _hotValueMap.end()) {
        second = floor(secondLogicId * iter->second);
    }
    return (first + second) % _hashSize;
}

uint32_t RoutingHashFunction::getHashId(const string& str1, const string& str2, float ratio) const {
    uint32_t first = getLogicId(str1);
    uint32_t second = floor(getLogicId(str2) * ratio);
    return (first + second) % _hashSize;
}

vector<pair<uint32_t, uint32_t> > RoutingHashFunction::getHashRange(uint32_t partId) const {
    if (partId >= _hashSize) {
        return {};
    }
    float ratio = _hotRangeVec[partId];
    return getInnerHashRange(partId, ratio);
}

vector<pair<uint32_t, uint32_t> > RoutingHashFunction::getHashRange(
        uint32_t partId, float ratio) const
{
    if (partId >= _hashSize || ratio < 0 || ratio > 1) {
        return {};
    }
    return getInnerHashRange(partId, ratio);
}

vector<pair<uint32_t, uint32_t> > RoutingHashFunction::getHashRange(const vector<string>& strVec) const {
    if (strVec.size() == 0) {
        return {};
    } else if (strVec.size() == 1) {
        return getInnerHashRange(strVec[0]);
    } else {
        float routingRatio = 0;
        if (!StringUtil::fromString(strVec[1], routingRatio)) {
            routingRatio = _routingRatio;
            AUTIL_LOG(WARN, "parse ratio [%s] failed", strVec[1].c_str());
            return {};
        }
        if (routingRatio < 0 || routingRatio > 1 ) {
            AUTIL_LOG(WARN, "ratio [%f] not in [0, 1].", routingRatio);
            return {};
        }
        return getInnerHashRange(strVec[0], routingRatio);
    }
}

vector<pair<uint32_t, uint32_t> > RoutingHashFunction::getInnerHashRange(const string& str) const {
    uint32_t first = getLogicId(str);
    float ratio = _hotRangeVec[first];
    auto iter = _hotValueMap.find(str);
    if (iter != _hotValueMap.end()) {
        ratio = iter->second;
    }
    return getInnerHashRange(first, ratio);
}

vector<pair<uint32_t, uint32_t> > RoutingHashFunction::getInnerHashRange(
        const string& str, float ratio) const
{
    uint32_t h1 = getLogicId(str);
    return getInnerHashRange(h1, ratio);
}

vector<pair<uint32_t, uint32_t> > RoutingHashFunction::getInnerHashRange(
        uint32_t logicId, float ratio) const
{
    uint32_t count = floor((_hashSize - 1 ) * ratio);
    vector<pair<uint32_t, uint32_t> > rangeVec;
    if (logicId + count >= _hashSize) {
        rangeVec.emplace_back(logicId, _hashSize - 1);
        rangeVec.emplace_back(0, (logicId + count) % _hashSize);
    } else {
        rangeVec.emplace_back(logicId, logicId + count);
    }
    return rangeVec;
}

uint32_t RoutingHashFunction::getLogicId(const std::string& str) const {
    return getLogicId(str.c_str(), str.length());
}

uint32_t RoutingHashFunction::getLogicId(const char *buf, size_t len) const {
    if (likely(_funcBase == nullptr)) {
        uint32_t hashVal = (uint32_t)HashAlgorithm::hashString(buf, len, 0);
        return hashVal % _hashSize;
    } else {
        return _funcBase->getHashId(buf, len);
    }
}

}

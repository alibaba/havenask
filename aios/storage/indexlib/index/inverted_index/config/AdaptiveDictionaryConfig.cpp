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
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"

#include "indexlib/config/ConfigDefine.h"
#include "indexlib/util/Exception.h"

using std::string;

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, AdaptiveDictionaryConfig);

struct AdaptiveDictionaryConfig::Impl {
    string ruleName;
    int32_t threshold = INVALID_ADAPTIVE_THRESHOLD;
    DictType dictType;
};

AdaptiveDictionaryConfig::AdaptiveDictionaryConfig() : _impl(std::make_unique<Impl>()) {}

AdaptiveDictionaryConfig::AdaptiveDictionaryConfig(const string& ruleName, const string& dictType, int32_t threshold)
    : _impl(std::make_unique<Impl>())
{
    _impl->ruleName = ruleName;
    _impl->threshold = threshold;
    _impl->dictType = FromTypeString(dictType);
    CheckThreshold();
}

AdaptiveDictionaryConfig::AdaptiveDictionaryConfig(const AdaptiveDictionaryConfig& other)
    : _impl(std::make_unique<Impl>(*other._impl))
{
}

AdaptiveDictionaryConfig::~AdaptiveDictionaryConfig() {}

AdaptiveDictionaryConfig::DictType AdaptiveDictionaryConfig::FromTypeString(const string& typeStr)
{
    if (typeStr == DOC_FREQUENCY_DICT_TYPE) {
        return AdaptiveDictionaryConfig::DictType::DF_ADAPTIVE;
    }

    if (typeStr == PERCENT_DICT_TYPE) {
        return AdaptiveDictionaryConfig::DictType::PERCENT_ADAPTIVE;
    }

    if (typeStr == INDEX_SIZE_DICT_TYPE) {
        return AdaptiveDictionaryConfig::DictType::SIZE_ADAPTIVE;
    }

    std::stringstream ss;
    ss << "unsupported dict_type [" << typeStr << "]"
       << "of adaptive_dictionary [" << _impl->ruleName << "]";
    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    return AdaptiveDictionaryConfig::DictType::UNKOWN_TYPE;
}

void AdaptiveDictionaryConfig::CheckThreshold() const
{
    if (_impl->dictType == AdaptiveDictionaryConfig::DictType::DF_ADAPTIVE) {
        if (_impl->threshold <= 0) {
            INDEXLIB_FATAL_ERROR(Schema, "adaptive threshold should"
                                         " greater than 0 when dict_type is DOC_FREQUENCY");
        }
    }

    if (_impl->dictType == AdaptiveDictionaryConfig::DictType::PERCENT_ADAPTIVE) {
        if (_impl->threshold > 100 || _impl->threshold < 0) {
            INDEXLIB_FATAL_ERROR(Schema, "adaptive threshold should"
                                         " be [0~100] when dict_type is PERCENT");
        }
    }
}

const string& AdaptiveDictionaryConfig::GetRuleName() const { return _impl->ruleName; }

const AdaptiveDictionaryConfig::DictType& AdaptiveDictionaryConfig::GetDictType() const { return _impl->dictType; }

int32_t AdaptiveDictionaryConfig::GetThreshold() const { return _impl->threshold; }

void AdaptiveDictionaryConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(ADAPTIVE_DICTIONARY_NAME, _impl->ruleName);
    int32_t defaultThreshold = INVALID_ADAPTIVE_THRESHOLD;
    json.Jsonize(ADAPTIVE_DICTIONARY_THRESHOLD, _impl->threshold, defaultThreshold);
    if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
        string typeStr = ToTypeString(_impl->dictType);
        json.Jsonize(ADAPTIVE_DICTIONARY_TYPE, typeStr);
    } else {
        string typeStr;
        json.Jsonize(ADAPTIVE_DICTIONARY_TYPE, typeStr);
        _impl->dictType = FromTypeString(typeStr);
        CheckThreshold();
    }
}

string AdaptiveDictionaryConfig::ToTypeString(DictType type)
{
    if (type == DF_ADAPTIVE) {
        return DOC_FREQUENCY_DICT_TYPE;
    }

    if (type == PERCENT_ADAPTIVE) {
        return PERCENT_DICT_TYPE;
    }

    if (type == SIZE_ADAPTIVE) {
        return INDEX_SIZE_DICT_TYPE;
    }
    std::stringstream ss;
    ss << "unknown enum dict type:" << type;
    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    return UNKOWN_DICT_TYPE;
}

Status AdaptiveDictionaryConfig::CheckEqual(const AdaptiveDictionaryConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->ruleName, other._impl->ruleName, "Adaptive dictionary name doesn't match");
    CHECK_CONFIG_EQUAL(_impl->dictType, other._impl->dictType, "Adaptive dict_type doesn't match");
    CHECK_CONFIG_EQUAL(_impl->threshold, other._impl->threshold, "Adaptive threshold doesn't match");
    return Status::OK();
}
} // namespace indexlib::config

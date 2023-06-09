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
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::config {

class AdaptiveDictionaryConfig : public autil::legacy::Jsonizable
{
public:
    enum DictType { DF_ADAPTIVE, PERCENT_ADAPTIVE, SIZE_ADAPTIVE, UNKOWN_TYPE };

public:
    AdaptiveDictionaryConfig();
    AdaptiveDictionaryConfig(const std::string& ruleName, const std::string& dictType, int32_t threshold);
    AdaptiveDictionaryConfig(const AdaptiveDictionaryConfig& other);
    ~AdaptiveDictionaryConfig();

public:
    const std::string& GetRuleName() const;

    const DictType& GetDictType() const;

    int32_t GetThreshold() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    Status CheckEqual(const AdaptiveDictionaryConfig& other) const;

private:
    AdaptiveDictionaryConfig::DictType FromTypeString(const std::string& typeStr);
    std::string ToTypeString(DictType type);
    void CheckThreshold() const;

private:
    inline static const std::string DOC_FREQUENCY_DICT_TYPE = "DOC_FREQUENCY";
    inline static const std::string PERCENT_DICT_TYPE = "PERCENT";
    inline static const std::string INDEX_SIZE_DICT_TYPE = "INDEX_SIZE";
    inline static const std::string UNKOWN_DICT_TYPE = "UNKOWN_DICT_TYPE";

    inline static const std::string ADAPTIVE_DICTIONARY_NAME = "adaptive_dictionary_name";
    inline static const std::string ADAPTIVE_DICTIONARY_THRESHOLD = "threshold";
    inline static const std::string ADAPTIVE_DICTIONARY_TYPE = "dict_type";

    constexpr static uint32_t INVALID_ADAPTIVE_THRESHOLD = -1;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    friend class AdaptiveDictionaryConfigTest;
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::config

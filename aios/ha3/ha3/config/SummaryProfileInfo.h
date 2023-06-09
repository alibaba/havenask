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

#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/isearch.h"

namespace isearch {
namespace config {

const uint32_t DEFAULT_MAX_SNIPPET = 1;
const uint32_t DEFAULT_MAX_SUMMARY_LENGTH = 273;
const std::string DEFAULT_HIGHLIGHT_PREFIX = "<font color=red>";
const std::string DEFAULT_HIGHLIGHT_SUFFIX = "</font>";
const std::string DEFAULT_SNIPPET_DELIMITER = "...";

// FieldSummaryConfig
class FieldSummaryConfig : public autil::legacy::Jsonizable 
{
public:
    FieldSummaryConfig();
    ~FieldSummaryConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    uint32_t _maxSnippet;
    uint32_t _maxSummaryLength;
    std::string _highlightPrefix;
    std::string _highlightSuffix;
    std::string _snippetDelimiter;
};
typedef std::map<std::string, FieldSummaryConfig> FieldSummaryConfigMap; 
typedef std::vector<FieldSummaryConfig*> FieldSummaryConfigVec;

// ExtractorInfo
class ExtractorInfo : public autil::legacy::Jsonizable
{
public:
    ExtractorInfo();
    ~ExtractorInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    std::string _extractorName;
    std::string _moduleName;
    KeyValueMap _parameters;
};
typedef std::vector<ExtractorInfo> ExtractorInfoVector;

// SummaryProfileInfo
class SummaryProfileInfo : public autil::legacy::Jsonizable
{
public:
    SummaryProfileInfo();
    ~SummaryProfileInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
public:
    void convertConfigMapToVector(const HitSummarySchema &hitSummarySchema);
public:
    std::string _summaryProfileName;
    ExtractorInfoVector _extractorInfos;
    FieldSummaryConfigMap _fieldSummaryConfigMap;
    FieldSummaryConfigVec _fieldSummaryConfigVec;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::vector<SummaryProfileInfo> SummaryProfileInfos;

typedef std::shared_ptr<SummaryProfileInfo> SummaryProfileInfoPtr;

} // namespace config
} // namespace isearch


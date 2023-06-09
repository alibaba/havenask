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
#include "ha3/config/SummaryProfileInfo.h"

#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "autil/legacy/jsonizable.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, SummaryProfileInfo);

// FieldSummaryConfig
FieldSummaryConfig::FieldSummaryConfig() {
    _maxSnippet = DEFAULT_MAX_SNIPPET;
    _maxSummaryLength = DEFAULT_MAX_SUMMARY_LENGTH;
    _highlightPrefix = DEFAULT_HIGHLIGHT_PREFIX;
    _highlightSuffix = DEFAULT_HIGHLIGHT_SUFFIX;
    _snippetDelimiter = DEFAULT_SNIPPET_DELIMITER;
}

FieldSummaryConfig::~FieldSummaryConfig() {
}

void FieldSummaryConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "max_snippet", _maxSnippet);
    JSONIZE(json, "max_summary_length", _maxSummaryLength);
    JSONIZE(json, "highlight_prefix", _highlightPrefix);
    JSONIZE(json, "highlight_suffix", _highlightSuffix);
    JSONIZE(json, "snippet_delimiter", _snippetDelimiter);
}

// ExtractorInfo
ExtractorInfo::ExtractorInfo() {
    _extractorName = "DefaultSummaryExtractor";
}

ExtractorInfo::~ExtractorInfo() {
}

void ExtractorInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "extractor_name", _extractorName);
    JSONIZE(json, "module_name", _moduleName);
    JSONIZE(json, "parameters", _parameters);
}

// SummaryProfileInfo
SummaryProfileInfo::SummaryProfileInfo() {
    _summaryProfileName = "DefaultProfile";
    _extractorInfos.push_back(ExtractorInfo());
}

SummaryProfileInfo::~SummaryProfileInfo() {
}

void SummaryProfileInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "summary_profile_name", _summaryProfileName);
    JSONIZE(json, "field_configs", _fieldSummaryConfigMap);
    if (json.GetMode() == FROM_JSON) {
        ExtractorInfoVector tmpExtractorInfos;
        JSONIZE(json, "extractors", tmpExtractorInfos);
        if (tmpExtractorInfos.size() == 0) {
            // compatible with old config
            ExtractorInfo extractorInfo;
            JSONIZE(json, "extractor", extractorInfo);
            tmpExtractorInfos.push_back(extractorInfo);
        }
        _extractorInfos.swap(tmpExtractorInfos);
    } else {
        JSONIZE(json, "extractors",  _extractorInfos);
    }
}

void SummaryProfileInfo::convertConfigMapToVector(
        const HitSummarySchema &hitSummarySchema)
{
    _fieldSummaryConfigVec.resize(hitSummarySchema.getFieldCount());
    size_t count = hitSummarySchema.getFieldCount();
    for (size_t i = 0; i < count; ++i)
    {
        const config::SummaryFieldInfo *summaryFieldInfo =
            hitSummarySchema.getSummaryFieldInfo(i);
        assert(summaryFieldInfo);
        FieldSummaryConfigMap::iterator mIt =
            _fieldSummaryConfigMap.find(summaryFieldInfo->fieldName);
        if (mIt == _fieldSummaryConfigMap.end()) {
            _fieldSummaryConfigVec[i] = NULL;
        } else {
            _fieldSummaryConfigVec[i] = &mIt->second;
        }
    }
}

} // namespace config
} // namespace isearch

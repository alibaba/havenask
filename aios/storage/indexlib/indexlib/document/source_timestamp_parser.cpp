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
#include "indexlib/document/source_timestamp_parser.h"

#include "autil/legacy/any.h"
#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"

namespace indexlib { namespace document {

using namespace std;

IE_LOG_SETUP(document, SourceTimestampParser);

void SourceTimestampParser::Init() {
    ExtractSouceTimestampFields(_schema->GetDefaultRegionId());
    _reportSourceCount = autil::EnvUtil::getEnv("indexlib_source_e2e_latency_report_count", MAX_SOURCE_COUNT);
}   

void SourceTimestampParser::Parse(const IndexlibExtendDocumentPtr& extendDoc, Document* docment) {
    const RawDocumentPtr& rawDoc = extendDoc->getRawDocument();
    if (nullptr == rawDoc) {
        return;
    }
    auto regionId = extendDoc->getRegionId();
    auto iter = _regionIdToPairs.find(regionId);
    if (iter == _regionIdToPairs.end()) {
        ExtractSouceTimestampFields(regionId);
        iter = _regionIdToPairs.find(regionId);
    }

    const vector<RawFieldToSource> rawFieldToSourceVec = iter->second;
    if (rawFieldToSourceVec.empty()) {
        return;
    }
    std::stringstream ss;
    for(const auto& rawFieldToSource : rawFieldToSourceVec) {
        const std::string timestampValue = rawDoc->getField(rawFieldToSource.first);
        if (!timestampValue.empty()) {
            ss << rawFieldToSource.second << SOURCE_TIMESTAMP_INNER_SEPARATOR << timestampValue << SOURCE_TIMESTAMP_OUTER_SEPARATOR;
        }
    }

    if(!ss.str().empty()) {
        docment->AddTag(SOURCE_TIMESTAMP_TAG_NAME, ss.str());
    }
}

void SourceTimestampParser::ExtractSouceTimestampFields(index::regionid_t regionId) {
    std::vector<RawFieldToSource> rawFieldToSourceVec;
    rawFieldToSourceVec.reserve(MAX_SOURCE_COUNT);
    const auto& fieldSchema = _schema->GetFieldSchema(regionId);
    if (nullptr == fieldSchema) {
        return;
    }
    auto tableName = _schema->GetSchemaName();
    for (auto fieldConfigIter = fieldSchema->Begin(); fieldConfigIter != fieldSchema->End(); ++fieldConfigIter) {
        auto& userDefineParam = (*fieldConfigIter)->GetUserDefinedParam();
        auto sourceTimestampIter = userDefineParam.find(SOURCE_TIMESTAMP_REPORT_KEY);
        auto fieldName = (*fieldConfigIter)->GetFieldName();
        auto fieldType = (*fieldConfigIter)->GetFieldType();
        if (sourceTimestampIter != userDefineParam.end()) {
            if (fieldType != ft_int64 || (*fieldConfigIter)->IsMultiValue()) {
                IE_LOG(WARN, "field [%s] in table [%s] with type [%d] and is multi [%d] can't as source timestamp.", fieldName.c_str(), tableName.c_str(), fieldType, (*fieldConfigIter)->IsMultiValue());
                rawFieldToSourceVec.clear();
                break;
            }
            std::string reportSourceName = autil::legacy::AnyCast<std::string>(sourceTimestampIter->second);
            rawFieldToSourceVec.push_back({(*fieldConfigIter)->GetFieldName(), reportSourceName});
        }
    }

    if (rawFieldToSourceVec.size() > _reportSourceCount) {
        IE_LOG(WARN, "table [%s] source timestamp field count is bigger than [%d].", tableName.c_str(), _reportSourceCount);
        rawFieldToSourceVec.clear();
    }
    _regionIdToPairs[regionId] = rawFieldToSourceVec;
}

} // namespace document
} // namespace indexlib
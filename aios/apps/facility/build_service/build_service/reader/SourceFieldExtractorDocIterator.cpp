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
#include "build_service/reader/SourceFieldExtractorDocIterator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/normal_table/NormalTabletDocIterator.h"
#include "indexlib/util/KeyValueMap.h"

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, SourceFieldExtractorDocIterator);

SourceFieldExtractorDocIterator::SourceFieldExtractorDocIterator(
    const std::shared_ptr<indexlibv2::config::TabletSchema>& schema)
    : _docIter(std::make_shared<indexlibv2::table::NormalTabletDocIterator>())
    , _targetSchema(schema)
{
}

SourceFieldExtractorDocIterator::~SourceFieldExtractorDocIterator() {}

indexlib::Status SourceFieldExtractorDocIterator::Seek(const std::string& checkpoint)
{
    return _docIter->Seek(checkpoint);
}

bool SourceFieldExtractorDocIterator::HasNext() const { return _docIter->HasNext(); }

indexlib::Status
SourceFieldExtractorDocIterator::Init(const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
                                      std::pair<uint32_t, uint32_t> rangeInRatio,
                                      const std::shared_ptr<indexlibv2::framework::MetricsManager>& metricsManager,
                                      const std::map<std::string, std::string>& params)
{
    std::string requiredFieldsStr = indexlib::util::GetValueFromKeyValueMap(params, "read_index_required_fields");
    assert(!requiredFieldsStr.empty());
    std::vector<std::string> fieldNames;
    try {
        autil::legacy::FromJsonString(fieldNames, requiredFieldsStr);
        AUTIL_LOG(INFO, "required [%lu] fields [%s]", fieldNames.size(), requiredFieldsStr.c_str());
    } catch (const autil::legacy::ExceptionBase& e) {
        RETURN_STATUS_ERROR(InvalidArgs, "parse required fields [%s] failed", requiredFieldsStr.c_str());
    }
    _requiredFields = std::set<std::string>(fieldNames.begin(), fieldNames.end());
    auto readSchema = tabletData->GetOnDiskVersionReadSchema();
    std::vector<std::string> sourceFieldNames;
    auto summaryConfig = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(
        readSchema->GetIndexConfig(indexlibv2::index::SUMMARY_INDEX_TYPE_STR, indexlibv2::index::SUMMARY_INDEX_NAME));

    for (const auto& fieldName : fieldNames) {
        auto fieldConfig = _targetSchema->GetFieldConfig(fieldName);
        assert(fieldConfig);
        if (readSchema->GetIndexConfig(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR, fieldName)) {
            sourceFieldNames.push_back(fieldName);
        } else if (summaryConfig && summaryConfig->GetSummaryConfig(fieldName)) {
            sourceFieldNames.push_back(fieldName);
        } else if (util::SourceFieldExtractorUtil::needExtractSourceField(fieldConfig)) {
            auto param = getSourceFieldParam(fieldConfig);
            if (param == nullptr) {
                RETURN_STATUS_ERROR(Corruption, "get source field param failed for [%s]", fieldName.c_str());
            }
            sourceFieldNames.push_back(param->fieldName);
            _fieldInfos[fieldName] = FieldInfo {param, fieldConfig->GetDefaultValue()};
        } else {
            assert(!fieldConfig->IsEnableNullField());
            _defaultValues[fieldName] = fieldConfig->GetDefaultValue();
        }
    }

    auto cloneParams = params;
    cloneParams["read_index_required_fields"] = autil::legacy::ToJsonString(sourceFieldNames);
    BS_LOG(INFO, "source required fields [%s], after extractor required fields [%s]", requiredFieldsStr.c_str(),
           cloneParams["read_index_required_fields"].c_str());
    return _docIter->Init(tabletData, rangeInRatio, metricsManager, cloneParams);
}

indexlib::Status SourceFieldExtractorDocIterator::Next(indexlibv2::document::RawDocument* rawDocument,
                                                       std::string* checkpoint,
                                                       indexlibv2::document::IDocument::DocInfo* docInfo)
{
    auto status = _docIter->Next(rawDocument, checkpoint, docInfo);
    RETURN_IF_STATUS_ERROR(status, "get next doc failed");
    for (const auto& [fieldName, fieldInfo] : _fieldInfos) {
        const auto& sourceFieldParam = fieldInfo.param;
        assert(sourceFieldParam);
        auto sourceFieldValue = rawDocument->getField(sourceFieldParam->fieldName);
        std::string fieldValue;
        if (!extractSourceFieldValue(sourceFieldValue, *sourceFieldParam, fieldInfo.defaultValue, fieldValue)) {
            RETURN_STATUS_ERROR(Corruption, "extract source field failed, fieldName[%s]", fieldName.c_str());
        }
        rawDocument->setField(fieldName, fieldValue);
    }
    for (const auto& [_, fieldInfo] : _fieldInfos) {
        auto sourceFieldName = fieldInfo.param->fieldName;
        if (!_requiredFields.count(sourceFieldName)) {
            if (rawDocument->exist(sourceFieldName)) {
                rawDocument->eraseField(sourceFieldName);
            }
        }
    }
    for (const auto& [fieldName, defaultValue] : _defaultValues) {
        rawDocument->setField(fieldName, defaultValue);
    }
    _sourceField2ParsedInfo.clear();
    return indexlib::Status::OK();
}

bool SourceFieldExtractorDocIterator::extractSourceFieldValue(
    const std::string& rawStr, const util::SourceFieldExtractorUtil::SourceFieldParam& param,
    const std::string& defaultValue, std::string& fieldValue)
{
    const std::string& sourceFieldName = param.fieldName;
    const std::string& fieldName = param.key;

    auto it = _sourceField2ParsedInfo.find(sourceFieldName);
    if (it != _sourceField2ParsedInfo.end()) {
        const auto& parsedInfo = it->second;
        const auto& kvMap = parsedInfo.second;
        auto iter = kvMap.find(fieldName);
        if (iter == kvMap.end()) {
            fieldValue = defaultValue;
        } else {
            fieldValue = iter->second;
        }
        return true;
    }

    util::SourceFieldParsedInfo parsedInfo;
    parsedInfo.first = rawStr;
    if (!util::SourceFieldExtractorUtil::parseRawString(param, parsedInfo)) {
        BS_LOG(ERROR, "parse raw string failed, source field[%s]", sourceFieldName.c_str());
        return false;
    }

    auto iter = parsedInfo.second.find(fieldName);
    if (iter == parsedInfo.second.end()) {
        fieldValue = defaultValue;
    } else {
        fieldValue = iter->second;
    }
    _sourceField2ParsedInfo.insert(it, std::make_pair(sourceFieldName, std::move(parsedInfo)));
    return true;
}

std::shared_ptr<util::SourceFieldExtractorUtil::SourceFieldParam> SourceFieldExtractorDocIterator::getSourceFieldParam(
    const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig)
{
    std::map<std::string, std::pair<std::string, std::string>> sourceField2Separators;
    auto param = std::make_shared<util::SourceFieldExtractorUtil::SourceFieldParam>();

    if (!util::SourceFieldExtractorUtil::getSourceFieldParam(fieldConfig, param)) {
        BS_LOG(ERROR, "get source field param failed.");
        return nullptr;
    }

    if (!util::SourceFieldExtractorUtil::checkUnifiedSeparator(*param, sourceField2Separators)) {
        BS_LOG(ERROR, "check failed.");
        return nullptr;
    }
    return param;
}

}} // namespace build_service::reader

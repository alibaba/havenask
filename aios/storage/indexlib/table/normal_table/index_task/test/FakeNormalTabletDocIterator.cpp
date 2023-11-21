#include "indexlib/table/normal_table/index_task/test/FakeNormalTabletDocIterator.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, FakeNormalTabletDocIterator);

FakeNormalTabletDocIterator::FakeNormalTabletDocIterator(const std::shared_ptr<config::ITabletSchema>& targetSchema)
    : ITabletDocIterator()
    , _docIter(std::make_shared<indexlibv2::table::NormalTabletDocIterator>())
    , _schema(targetSchema)
{
}

FakeNormalTabletDocIterator::~FakeNormalTabletDocIterator() {}

Status FakeNormalTabletDocIterator::Init(const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
                                         std::pair<uint32_t /*0-99*/, uint32_t /*0-99*/> rangeInRatio,
                                         const std::shared_ptr<indexlibv2::framework::MetricsManager>& metricsManager,
                                         const std::optional<std::vector<std::string>>& fieldNames,
                                         const std::map<std::string, std::string>& params)
{
    assert(fieldNames);
    std::vector<std::string> sourceFieldNames;
    auto readSchema = tabletData->GetOnDiskVersionReadSchema();
    auto summaryConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        readSchema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME));

    for (const auto& fieldName : fieldNames.value()) {
        std::string sourceFieldName = fieldName;
        auto fieldConfig = _schema->GetFieldConfig(fieldName);
        auto attributeConfig = readSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);
        if (attributeConfig) {
            sourceFieldNames.push_back(fieldName);
        } else if (summaryConfig && summaryConfig->GetSummaryConfig(fieldName)) {
            sourceFieldNames.push_back(fieldName);
        } else if (GetSourceField(fieldConfig, sourceFieldName)) {
            sourceFieldNames.push_back(sourceFieldName);
            _fieldMapper[fieldName] = sourceFieldName;
        } else if (UseDefaultValue(fieldConfig)) {
            _defaultValues[fieldName] = fieldConfig->GetDefaultValue();
        } else {
            AUTIL_LOG(ERROR, "get field [%s] source [%s] in attributes failed", fieldName.c_str(),
                      sourceFieldName.c_str());
            return Status::Corruption();
        }
    }
    return _docIter->Init(tabletData, rangeInRatio, metricsManager, sourceFieldNames, params);
}

Status FakeNormalTabletDocIterator::Next(indexlibv2::document::RawDocument* rawDocument, std::string* checkpoint,
                                         indexlibv2::framework::Locator::DocInfo* docInfo)
{
    auto status = _docIter->Next(rawDocument, checkpoint, docInfo);
    RETURN_IF_STATUS_ERROR(status, "get next doc failed");
    for (const auto& [fieldName, sourceFieldName] : _fieldMapper) {
        auto sourceFieldValue = rawDocument->getField(sourceFieldName);
        rawDocument->setField(fieldName, sourceFieldValue);
    }
    for (const auto& [fieldName, sourceFieldName] : _fieldMapper) {
        if (rawDocument->exist(sourceFieldName)) {
            rawDocument->eraseField(sourceFieldName);
        }
    }
    for (const auto& [fieldName, defaultValue] : _defaultValues) {
        rawDocument->setField(fieldName, defaultValue);
    }
    return Status::OK();
}

bool FakeNormalTabletDocIterator::HasNext() const { return _docIter->HasNext(); }

Status FakeNormalTabletDocIterator::Seek(const std::string& checkpoint) { return _docIter->Seek(checkpoint); }

bool FakeNormalTabletDocIterator::UseDefaultValue(std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig)
{
    auto userDefineParam = fieldConfig->GetUserDefinedParam();
    if (userDefineParam.empty()) {
        return true;
    }
    return false;
}

bool FakeNormalTabletDocIterator::GetSourceField(std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig,
                                                 std::string& fieldName)
{
    auto userDefinedParam = fieldConfig->GetUserDefinedParam();
    auto iter = userDefinedParam.find("source_field");
    if (iter == userDefinedParam.end()) {
        return false;
    }
    auto sourceField = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(iter->second);

    auto iter2 = sourceField.find("field_name");
    if (iter2 == sourceField.end()) {
        return false;
    }
    fieldName = autil::legacy::AnyCast<std::string>(iter2->second);
    return true;
}

} // namespace indexlibv2::table

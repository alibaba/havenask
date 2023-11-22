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
#include "indexlib/document/normal/TokenizeDocumentConvertor.h"

#include "indexlib/analyzer/Analyzer.h"
#include "indexlib/analyzer/AnalyzerDefine.h"
#include "indexlib/analyzer/AnalyzerInfo.h"
#include "indexlib/analyzer/IAnalyzerFactory.h"
#include "indexlib/analyzer/SimpleTokenizer.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/normal/tokenize/TokenizeDocument.h"
#include "indexlib/document/normal/tokenize/TokenizeSection.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/util/Algorithm.h"

using namespace indexlibv2::analyzer;

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, TokenizeDocumentConvertor);

const std::string TokenizeDocumentConvertor::LAST_VALUE_PREFIX = "__last_value__";
static const char SECTION_WEIGHT_SEPARATOR = '\x1C';

Status TokenizeDocumentConvertor::Init(const std::shared_ptr<config::ITabletSchema>& schema,
                                       analyzer::IAnalyzerFactory* analyzerFactory)
{
    std::vector<std::string> fieldNames;
    for (const auto& indexType : {indexlib::index::INVERTED_INDEX_TYPE_STR, index::ANN_INDEX_TYPE_STR,
                                  index::PRIMARY_KEY_INDEX_TYPE_STR, index::SUMMARY_INDEX_TYPE_STR}) {
        for (const auto& fieldConfig : schema->GetIndexFieldConfigs(indexType)) {
            fieldNames.push_back(fieldConfig->GetFieldName());
        }
    }
    indexlib::util::Algorithm::SortUniqueAndErase(fieldNames);
    std::vector<std::shared_ptr<config::FieldConfig>> fieldConfigs;
    for (const auto& fieldName : fieldNames) {
        fieldConfigs.push_back(schema->GetFieldConfig(fieldName));
    }
    auto allFieldConfigs = schema->GetFieldConfigs();
    for (const auto& fieldConfig : allFieldConfigs) {
        if (fieldConfig->GetFieldType() != ft_raw &&
            std::find(fieldNames.begin(), fieldNames.end(), fieldConfig->GetFieldName()) == fieldNames.end()) {
            _emptyFieldConfigs.push_back(fieldConfig);
        }
    }
    return Init(fieldConfigs, analyzerFactory);
}

Status TokenizeDocumentConvertor::Init(const std::vector<std::shared_ptr<config::FieldConfig>>& fieldConfigs,
                                       analyzer::IAnalyzerFactory* analyzerFactory)
{
    _fieldConfigs = fieldConfigs;
    _maxFieldId = INVALID_FIELDID;
    for (const auto& fieldConfig : fieldConfigs) {
        _maxFieldId = std::max(_maxFieldId, fieldConfig->GetFieldId());
    }
    _analyzerFactory = analyzerFactory;
    return CheckAnalyzers();
}

Status TokenizeDocumentConvertor::CheckAnalyzers() const
{
    for (const auto& fieldConfig : _fieldConfigs) {
        if (fieldConfig->GetFieldType() == ft_text) {
            const std::string& analyzerName = fieldConfig->GetAnalyzerName();
            if (analyzerName.empty()) {
                std::string errorMsg = "Get analyzer FAIL, fieldName = [" + fieldConfig->GetFieldName() + "]";
                AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
                return Status::InvalidArgs();
            }
            if (analyzerName == SIMPLE_ANALYZER) {
                // built-in simple analyzer can be created without analyzer factory
                continue;
            }
            if (_analyzerFactory == nullptr || !_analyzerFactory->hasAnalyzer(analyzerName)) {
                std::string errorMsg = "create Analyzer failed: [" + analyzerName + "]";
                AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
                return Status::InternalError();
            }
        }
    }
    return Status::OK();
}

Status
TokenizeDocumentConvertor::Convert(RawDocument* rawDoc, const std::map<fieldid_t, std::string>& fieldAnalyzerNameMap,
                                   const std::shared_ptr<indexlib::document::TokenizeDocument>& tokenizeDocument,
                                   const std::shared_ptr<indexlib::document::TokenizeDocument>& lastTokenizeDocument)
{
    auto fieldCount = _maxFieldId + 1;
    tokenizeDocument->reserve(fieldCount);
    lastTokenizeDocument->reserve(fieldCount);
    for (const auto& fieldConfig : _fieldConfigs) {
        const std::string& fieldName = fieldConfig->GetFieldName();
        std::string specifyAnalyzerName = GetAnalyzerName(fieldConfig->GetFieldId(), fieldAnalyzerNameMap);
        if (!ProcessField(rawDoc, fieldConfig, fieldName, specifyAnalyzerName, tokenizeDocument)) {
            return Status::InternalError();
        }
        if (!ProcessLastField(rawDoc, fieldConfig, LAST_VALUE_PREFIX + fieldName, specifyAnalyzerName,
                              lastTokenizeDocument)) {
            return Status::InternalError();
        }
    }
    for (const auto& fieldConfig : _emptyFieldConfigs) {
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (tokenizeDocument->getField(fieldId) != nullptr) {
            AUTIL_LOG(DEBUG, "fieldName:[%s] has already been tokenized", fieldConfig->GetFieldName().c_str());
            continue;
        }
        const indexlib::document::TokenizeFieldPtr& field = tokenizeDocument->createField(fieldId);
        if (field.get() == nullptr) {
            std::stringstream ss;
            ss << "create TokenizeField failed: [" << fieldId << "]";
            std::string errorMsg = ss.str();
            AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
            return Status::Corruption();
        }
    }
    return Status::OK();
}

std::string TokenizeDocumentConvertor::GetAnalyzerName(fieldid_t fieldId,
                                                       const std::map<fieldid_t, std::string>& fieldAnalyzerNameMap)
{
    auto it = fieldAnalyzerNameMap.find(fieldId);
    if (it != fieldAnalyzerNameMap.end()) {
        return it->second;
    } else {
        return "";
    }
}

bool TokenizeDocumentConvertor::ProcessLastField(const RawDocument* rawDocument,
                                                 const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                                                 const std::string& fieldName, const std::string& specifyAnalyzerName,
                                                 const indexlib::document::TokenizeDocumentPtr& tokenizeDocument)
{
    if (!rawDocument->exist(fieldName)) {
        return true;
    }
    return ProcessField(rawDocument, fieldConfig, fieldName, specifyAnalyzerName, tokenizeDocument);
}

bool TokenizeDocumentConvertor::ProcessField(const RawDocument* rawDocument,
                                             const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                                             const std::string& fieldName, const std::string& specifyAnalyzerName,
                                             const indexlib::document::TokenizeDocumentPtr& tokenizeDocument)
{
    FieldType fieldType = fieldConfig->GetFieldType();
    if (fieldType == ft_raw) {
        return true;
    }

    const autil::StringView& fieldValue = rawDocument->getField(autil::StringView(fieldName));
    AUTIL_LOG(DEBUG, "fieldtype:[%d], fieldname:[%s], fieldvalue:[%s]", fieldType, fieldName.c_str(),
              std::string(fieldValue.data(), fieldValue.size()).c_str());

    fieldid_t fieldId = fieldConfig->GetFieldId();
    if (tokenizeDocument->getField(fieldId) != nullptr) {
        AUTIL_LOG(DEBUG, "fieldName:[%s] has already been tokenized", fieldName.c_str());
        return true;
    }
    const indexlib::document::TokenizeFieldPtr& field = tokenizeDocument->createField(fieldId);
    if (field.get() == nullptr) {
        std::stringstream ss;
        ss << "create TokenizeField failed: [" << fieldId << "]";
        std::string errorMsg = ss.str();
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    bool setNull = fieldConfig->IsEnableNullField() and ((fieldValue.empty() && !rawDocument->exist(fieldName)) ||
                                                         fieldValue == fieldConfig->GetNullFieldLiteralString());
    return TokenizeField(fieldConfig, fieldValue, specifyAnalyzerName, setNull, field);
}

Analyzer* TokenizeDocumentConvertor::GetAnalyzer(const std::shared_ptr<config::FieldConfig>& fieldConfig,
                                                 const std::string& fieldAnalyzerName) const
{
    std::string analyzerName;
    if (!fieldAnalyzerName.empty()) {
        analyzerName = fieldAnalyzerName;
    } else {
        if (!fieldConfig) {
            return nullptr;
        }
        analyzerName = fieldConfig->GetAnalyzerName();
    }
    Analyzer* analyzer = nullptr;
    if (_analyzerFactory) {
        analyzer = _analyzerFactory->createAnalyzer(analyzerName);
    }
    if (analyzer == nullptr && analyzerName == SIMPLE_ANALYZER) {
        if (!fieldConfig) {
            return nullptr;
        }
        analyzer = CreateSimpleAnalyzer(GetSeparator(fieldConfig));
    }
    return analyzer;
}

Analyzer* TokenizeDocumentConvertor::CreateSimpleAnalyzer(const std::string& delimiter) const
{
    ITokenizer* tokenizer = new SimpleTokenizer(delimiter);
    Analyzer* analyzer = new Analyzer(tokenizer);
    analyzer::AnalyzerInfo analyzerInfo;
    analyzer->init(&analyzerInfo, /*TraditionalTable=*/nullptr);
    return analyzer;
}

bool TokenizeDocumentConvertor::TokenizeTextField(const std::shared_ptr<config::FieldConfig>& fieldConfig,
                                                  const indexlib::document::TokenizeFieldPtr& field,
                                                  const autil::StringView& fieldValue,
                                                  const std::string& fieldAnalyzerName)
{
    if (fieldValue.empty()) {
        return true;
    }
    Analyzer* analyzer = GetAnalyzer(fieldConfig, fieldAnalyzerName);
    if (!analyzer) {
        std::stringstream ss;
        ss << "Get analyzer FAIL, fieldName = [" << fieldConfig->GetFieldName() << "]";
        std::string errorMsg = ss.str();
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    bool ret = DoTokenizeTextField(field, fieldValue, analyzer);
    delete analyzer;
    return ret;
}

bool TokenizeDocumentConvertor::DoTokenizeTextField(const indexlib::document::TokenizeFieldPtr& field,
                                                    const autil::StringView& fieldValue, Analyzer* analyzer)
{
    indexlib::document::TokenizeSection* section = nullptr;
    indexlib::document::TokenizeSection::Iterator iterator;
    analyzer->tokenize(fieldValue.data(), fieldValue.size());

    indexlib::document::AnalyzerToken token;

    while (analyzer->next(token)) {
        const std::string& text = token.getText();
        assert(text.length() > 0);
        if (text[0] == SECTION_WEIGHT_SEPARATOR) {
            if (ExtractSectionWeight(section, *analyzer)) {
                section = nullptr;
                continue;
            } else {
                break;
            }
        }
        if (text[0] == MULTI_VALUE_SEPARATOR && !token.isDelimiter()) {
            section = nullptr;
            continue;
        }

        if (section == nullptr) {
            section = field->getNewSection();
            if (nullptr == section) {
                std::stringstream ss;
                ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
                std::string errorMsg = ss.str();
                AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            iterator = section->createIterator();
        }

        if (token.isBasicRetrieve()) {
            section->insertBasicToken(token, iterator);
        } else {
            section->insertExtendToken(token, iterator);
        }
    }
    return true;
}

bool TokenizeDocumentConvertor::FindFirstNonDelimiterToken(const Analyzer& analyzer,
                                                           indexlib::document::AnalyzerToken& token) const
{
    bool found = false;
    while (analyzer.next(token)) {
        if (!token.isDelimiter()) {
            found = true;
            break;
        }
    }
    return found;
}

bool TokenizeDocumentConvertor::ExtractSectionWeight(indexlib::document::TokenizeSection* section,
                                                     const analyzer::Analyzer& analyzer)
{
    indexlib::document::AnalyzerToken token;
    if (nullptr == section) {
        analyzer.next(token);
        return true;
    }
    if (!FindFirstNonDelimiterToken(analyzer, token)) {
        return false;
    } else {
        std::string& text = token.getText();
        assert(text.length() > 0);
        int32_t sectionWeight;
        if (text[0] == MULTI_VALUE_SEPARATOR) {
            sectionWeight = 0;
        } else {
            if (!autil::StringUtil::strToInt32(text.c_str(), sectionWeight)) {
                std::stringstream ss;
                ss << "cast section weight failed, [" << text << "](" << text.c_str()[0] << ")";
                std::string errorMsg = ss.str();
                AUTIL_LOG(WARN, "%s", errorMsg.c_str());
                return false;
            }
            if (!FindFirstNonDelimiterToken(analyzer, token)) {
                return false;
            } else {
                text = token.getText();
                assert(text.length() > 0);
                if (text[0] != MULTI_VALUE_SEPARATOR) {
                    return false;
                }
            }
        }
        section->setSectionWeight((section_weight_t)sectionWeight);
        return true;
    }
}

bool TokenizeDocumentConvertor::TokenizeMultiValueField(const indexlib::document::TokenizeFieldPtr& field,
                                                        const autil::StringView& fieldValue,
                                                        const std::string& seperator)
{
    if (fieldValue.empty()) {
        return true;
    }
    SimpleTokenizer tokenizer(seperator);
    tokenizer.tokenize(fieldValue.data(), fieldValue.size());

    indexlib::document::TokenizeSection* section = field->getNewSection();
    if (nullptr == section) {
        std::stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        std::string errorMsg = ss.str();
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    indexlib::document::TokenizeSection::Iterator iterator = section->createIterator();
    indexlib::document::AnalyzerToken token;
    while (tokenizer.next(token)) {
        token.setText(token.getNormalizedText());
        section->insertBasicToken(token, iterator);
    }

    return true;
}

bool TokenizeDocumentConvertor::TokenizeSingleValueField(const indexlib::document::TokenizeFieldPtr& field,
                                                         const autil::StringView& fieldValue)
{
    if (fieldValue.empty()) {
        return true;
    }

    indexlib::document::TokenizeSection* section = field->getNewSection();
    if (nullptr == section) {
        std::stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        std::string errorMsg = ss.str();
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    indexlib::document::TokenizeSection::Iterator iterator = section->createIterator();

    indexlib::document::AnalyzerToken token;
    std::string tokenStr(fieldValue.data(), fieldValue.size());
    token.setText(tokenStr);
    token.setNormalizedText(tokenStr);
    section->insertBasicToken(token, iterator);
    return true;
}

const std::string&
TokenizeDocumentConvertor::GetSeparator(const std::shared_ptr<config::FieldConfig>& fieldConfig) const
{
    return fieldConfig->GetSeparator();
}

bool TokenizeDocumentConvertor::TokenizeField(const std::shared_ptr<config::FieldConfig>& fieldConfig,
                                              autil::StringView fieldValue, const std::string& analyzerName,
                                              bool isNull,
                                              const std::shared_ptr<indexlib::document::TokenizeField>& field)
{
    if (isNull) {
        field->setNull(true);
        return true;
    }
    bool ret = true;
    auto fieldType = fieldConfig->GetFieldType();
    if (ft_text == fieldType) {
        ret = TokenizeTextField(fieldConfig, field, fieldValue, analyzerName);
    } else if (ft_location == fieldType || ft_line == fieldType || ft_polygon == fieldType) {
        ret = TokenizeSingleValueField(field, fieldValue);
    } else if (fieldConfig->IsMultiValue()) {
        ret = TokenizeMultiValueField(field, fieldValue, GetSeparator(fieldConfig));
    } else {
        ret = TokenizeSingleValueField(field, fieldValue);
    }

    if (!ret) {
        std::string errorMsg = "Failed to Tokenize field:[" + fieldConfig->GetFieldName() + "]";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

} // namespace indexlibv2::document

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
#include "autil/NoCopyable.h"
#include "autil/Span.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"

namespace indexlibv2::analyzer {
class IAnalyzerFactory;
class Analyzer;
} // namespace indexlibv2::analyzer
namespace indexlib::config {
class IndexSchema;
class IndexPartitionSchema;
}; // namespace indexlib::config
namespace indexlibv2::config {
class FieldConfig;
class ITabletSchema;
} // namespace indexlibv2::config
namespace indexlib::document {
class TokenizeField;
typedef std::shared_ptr<TokenizeField> TokenizeFieldPtr;
class TokenizeDocument;
typedef std::shared_ptr<TokenizeDocument> TokenizeDocumentPtr;
class AnalyzerToken;
class TokenizeSection;
}; // namespace indexlib::document

namespace indexlibv2::document {
class RawDocument;

class TokenizeDocumentConvertor : private autil::NoCopyable
{
public:
    TokenizeDocumentConvertor() = default;
    virtual ~TokenizeDocumentConvertor() = default;

public:
    static const std::string LAST_VALUE_PREFIX;

    Status Init(const std::shared_ptr<config::ITabletSchema>& schema, analyzer::IAnalyzerFactory* analyzerFactory);

    Status Init(const std::vector<std::shared_ptr<config::FieldConfig>>& fieldConfigs,
                analyzer::IAnalyzerFactory* analyzerFactory);
    Status Convert(RawDocument* rawDoc, const std::map<fieldid_t, std::string>& fieldAnalyzerNameMap,
                   const std::shared_ptr<indexlib::document::TokenizeDocument>& tokenizeDocument,
                   const std::shared_ptr<indexlib::document::TokenizeDocument>& lastTokenizeDocument);

protected:
    virtual const std::string& GetSeparator(const std::shared_ptr<config::FieldConfig>& fieldConfig) const;
    virtual Status CheckAnalyzers() const;

protected:
    bool TokenizeField(const std::shared_ptr<config::FieldConfig>& fieldConfig, autil::StringView fieldValue,
                       const std::string& analyzerName, bool isNull,
                       const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField);

private:
    std::string GetAnalyzerName(fieldid_t fieldId, const std::map<fieldid_t, std::string>& fieldAnalyzerNameMap);

    analyzer::Analyzer* CreateSimpleAnalyzer(const std::string& delimiter) const;
    bool ProcessLastField(const RawDocument* rawDocument,
                          const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
                          const std::string& fieldName, const std::string& specifyAnalyzerName,
                          const indexlib::document::TokenizeDocumentPtr& tokenizeDocument);
    analyzer::Analyzer* GetAnalyzer(const std::shared_ptr<config::FieldConfig>& fieldConfig,
                                    const std::string& fieldAnalyzerName) const;
    bool ProcessField(const RawDocument* rawDocument,
                      const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig, const std::string& fieldName,
                      const std::string& specifyAnalyzerName,
                      const indexlib::document::TokenizeDocumentPtr& tokenizeDocument);
    bool TokenizeTextField(const std::shared_ptr<config::FieldConfig>& fieldConfig,
                           const indexlib::document::TokenizeFieldPtr& field, const autil::StringView& fieldValue,
                           const std::string& fieldAnalyzerName);

    bool TokenizeMultiValueField(const indexlib::document::TokenizeFieldPtr& field, const autil::StringView& fieldValue,
                                 const std::string& seperator);
    bool TokenizeSingleValueField(const indexlib::document::TokenizeFieldPtr& field,
                                  const autil::StringView& fieldValue);

    bool DoTokenizeTextField(const indexlib::document::TokenizeFieldPtr& field, const autil::StringView& fieldValue,
                             analyzer::Analyzer* analyzer);

    bool FindFirstNonDelimiterToken(const analyzer::Analyzer& analyzer, indexlib::document::AnalyzerToken& token) const;

    bool ExtractSectionWeight(indexlib::document::TokenizeSection* section, const analyzer::Analyzer& analyzer);

private:
    fieldid_t _maxFieldId = INVALID_FIELDID;
    std::vector<std::shared_ptr<config::FieldConfig>> _fieldConfigs;
    //兼容老行为，非倒排字段也会create field
    std::vector<std::shared_ptr<config::FieldConfig>> _emptyFieldConfigs;
    analyzer::IAnalyzerFactory* _analyzerFactory = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document

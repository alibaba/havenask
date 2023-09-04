#pragma once

#include "indexlib/document/normal/NormalExtendDocument.h"
#include "indexlib/document/normal/tokenize/AnalyzerToken.h"
#include "indexlib/document/normal/tokenize/TokenizeDocument.h"
#include "indexlib/document/normal/tokenize/TokenizeField.h"

namespace indexlibv2::config {
class ITabletSchema;
class FieldConfig;
} // namespace indexlibv2::config

namespace indexlibv2 { namespace document {

// This class copies code from TokenizeDocumentProcessor.cpp in build_service. Refer to documentation there and also
// sync here accordingly if anything is updated in build_service.
class TokenizeHelper
{
public:
    static const std::string PROCESSOR_NAME;
    static const std::string LAST_VALUE_PREFIX;

public:
    TokenizeHelper();
    TokenizeHelper(const TokenizeHelper& other);
    ~TokenizeHelper();

public:
    bool process(const std::shared_ptr<NormalExtendDocument>& document);
    void destroy();
    bool init(const std::shared_ptr<config::ITabletSchema>& schemaPtr);

private:
    bool tokenizeTextField(const std::shared_ptr<indexlib::document::TokenizeField>& field,
                           const autil::StringView& fieldValue);
    bool doTokenizeTextField(const std::shared_ptr<indexlib::document::TokenizeField>& field,
                             const autil::StringView& fieldValue);
    bool tokenizeMultiValueField(const std::shared_ptr<indexlib::document::TokenizeField>& field,
                                 const autil::StringView& fieldValue);
    bool tokenizeSingleValueField(const std::shared_ptr<indexlib::document::TokenizeField>& field,
                                  const autil::StringView& fieldValue);
    void addToken(std::vector<indexlib::document::AnalyzerToken>& tokens, std::string text, int pos,
                  std::string normText, indexlib::document::AnalyzerToken::TokenProperty& property);
    bool processField(const std::shared_ptr<NormalExtendDocument>& document,
                      const std::shared_ptr<config::FieldConfig>& fieldConfig, const std::string& fieldName,
                      const std::shared_ptr<indexlib::document::TokenizeDocument>& tokenizeDocument);

private:
    std::shared_ptr<config::ITabletSchema> _schema;
    std::set<fieldid_t> _invertedFieldIds;
    indexlib::document::AnalyzerToken::TokenProperty _property;
    indexlib::document::AnalyzerToken::TokenProperty _emptyProperty;
    indexlib::document::AnalyzerToken::TokenProperty _stopwordProperty;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document

#ifndef __INDEXLIB_TEST_TOKENHELPER_H
#define __INDEXLIB_TEST_TOKENHELPER_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/extend_document/tokenize/analyzer_token.h"
#include "indexlib/document/extend_document/tokenize/tokenize_field.h"
#include "indexlib/document/extend_document/tokenize_document.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

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
    bool process(const IndexlibExtendDocumentPtr& document);
    void destroy();
    bool init(const config::IndexPartitionSchemaPtr& schemaPtr);

private:
    bool tokenizeTextField(const TokenizeFieldPtr& field, const autil::StringView& fieldValue);
    bool doTokenizeTextField(const TokenizeFieldPtr& field, const autil::StringView& fieldValue);
    bool tokenizeMultiValueField(const TokenizeFieldPtr& field, const autil::StringView& fieldValue);
    bool tokenizeSingleValueField(const TokenizeFieldPtr& field, const autil::StringView& fieldValue);
    void addToken(std::vector<AnalyzerToken>& tokens, std::string text, int pos, std::string normText,
                  AnalyzerToken::TokenProperty& property);
    bool processField(const IndexlibExtendDocumentPtr& document, const config::FieldConfigPtr& fieldConfig,
                      const std::string& fieldName, const TokenizeDocumentPtr& tokenizeDocument);

private:
    config::IndexSchemaPtr _indexSchema;
    config::FieldSchemaPtr _fieldSchema;

    AnalyzerToken::TokenProperty _property;
    AnalyzerToken::TokenProperty _emptyProperty;
    AnalyzerToken::TokenProperty _stopwordProperty;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TokenizeHelper);
}} // namespace indexlib::document

#endif // INDEXLIB_TEST_TOKENHELPER_H

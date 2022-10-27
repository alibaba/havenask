#ifndef __INDEXLIB_TEST_TOKENHELPER_H
#define __INDEXLIB_TEST_TOKENHELPER_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/extend_document/tokenize/analyzer_token.h"
#include "indexlib/document/extend_document/tokenize/tokenize_field.h"
#include "indexlib/document/extend_document/tokenize_document.h"
#include "indexlib/document/extend_document/tokenize/analyzer_token.h"

IE_NAMESPACE_BEGIN(document);

class TokenizeHelper
{
public:
    static const std::string PROCESSOR_NAME;
public:
    TokenizeHelper();
    TokenizeHelper(const TokenizeHelper &other);
    ~TokenizeHelper();
public:
    bool process(const IndexlibExtendDocumentPtr& document);
    void destroy();

    bool init(const config::IndexPartitionSchemaPtr& schemaPtr);

    std::string getDocumentProcessorName() const
    { return PROCESSOR_NAME; }

private:
    bool tokenizeTextField(const TokenizeFieldPtr &field,
                           const autil::ConstString &fieldValue);
    bool doTokenizeTextField(const TokenizeFieldPtr &field,
                             const autil::ConstString &fieldValue);
    bool tokenizeMultiValueField(const TokenizeFieldPtr &field,
                                 const autil::ConstString &fieldValue);
    bool tokenizeSingleValueField(const TokenizeFieldPtr &field,
                                  const autil::ConstString &fieldValue);
    void addToken(std::vector<AnalyzerToken>& tokens,
                  std::string text,
                  int pos, std::string normText,
                  AnalyzerToken::TokenProperty& property);

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

IE_NAMESPACE_END(document);

#endif //INDEXLIB_TEST_TOKENHELPER_H

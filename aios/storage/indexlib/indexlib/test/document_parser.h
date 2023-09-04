#ifndef __INDEXLIB_TEST_DOCUMENT_PARSER_H
#define __INDEXLIB_TEST_DOCUMENT_PARSER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/raw_document.h"
#include "indexlib/test/result.h"
#include "indexlib/test/tokenize_section.h"

namespace indexlib { namespace test {

class DocumentParser
{
public:
    DocumentParser();
    ~DocumentParser();

public:
    static RawDocumentPtr Parse(const std::string& docStr);
    static ResultPtr ParseResult(const std::string& docStr);
    static TokenizeSectionPtr ParseSection(const std::string& sectionStr, const std::string& sep = DP_TOKEN_SEPARATOR);
    static std::string ParseMultiValueField(const std::string& fieldValue, bool replaceSep = true);
    static std::string ParseMultiValueResult(const std::string& fieldValue);

public:
    static const std::string DP_CMD_SEPARATOR;
    static const std::string DP_TOKEN_SEPARATOR;
    static const std::string DP_TAG_HASH_ID_FIELD;

private:
    static const std::string DP_SPATIAL_KEY_VALUE_SEPARATOR;
    static const std::string DP_KEY_VALUE_SEPARATOR;
    static const std::string DP_KEY_VALUE_EQUAL_SYMBOL;
    static const char DP_MULTI_VALUE_SEPARATOR;
    static const std::string DP_MAIN_JOIN_FIELD;
    static const std::string DP_SUB_JOIN_FIELD;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentParser);
}} // namespace indexlib::test

#endif //__INDEXLIB_TEST_DOCUMENT_PARSER_H

#pragma once

#include <memory>

#include "autil/Log.h"
#include "indexlib/table/test/RawDocument.h"
#include "indexlib/table/test/Result.h"
//#include "indexlib/test/tokenize_section.h"

namespace indexlibv2 { namespace table {

class DocumentParser
{
public:
    DocumentParser();
    ~DocumentParser();

public:
    static std::shared_ptr<RawDocument> Parse(const std::string& docStr);
    static std::shared_ptr<Result> ParseResult(const std::string& docStr);
    // static TokenizeSectionPtr ParseSection(const std::string& sectionStr, const std::string& sep =
    // DP_TOKEN_SEPARATOR);
    static std::string ParseMultiValueField(const std::string& fieldValue, bool replaceSep = true);
    static std::string ParseMultiValueResult(const std::string& fieldValue);

public:
    inline static const std::string DP_TAG_HASH_ID_FIELD = "_tag_hash_id_";
    inline static const std::string DP_MAIN_JOIN_FIELD = "main_join";
    inline static const std::string DP_SUB_JOIN_FIELD = "sub_join";

public:
    inline static const std::string DP_SPATIAL_KEY_VALUE_SEPARATOR = "|";
    inline static const std::string DP_KEY_VALUE_SEPARATOR = ",";
    inline static const std::string DP_KEY_VALUE_EQUAL_SYMBOL = "=";
    inline static const std::string DP_CMD_SEPARATOR = ";";
    inline static const std::string DP_TOKEN_SEPARATOR = " ";
    inline static const char DP_MULTI_VALUE_SEPARATOR = ' ';

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table

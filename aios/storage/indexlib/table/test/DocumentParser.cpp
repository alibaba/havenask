#include "indexlib/table/test/DocumentParser.h"

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/index/common/Constant.h"
// #include "indexlib/document/index_field_convertor.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, DocumentParser);

DocumentParser::DocumentParser() {}

DocumentParser::~DocumentParser() {}

std::shared_ptr<RawDocument> DocumentParser::Parse(const std::string& docStr)
{
    std::vector<std::string> keyValues = autil::StringUtil::split(docStr, DP_SPATIAL_KEY_VALUE_SEPARATOR);
    if (keyValues.size() <= 1) {
        keyValues = autil::StringUtil::split(docStr, DP_KEY_VALUE_SEPARATOR);
    }

    std::shared_ptr<RawDocument> rawDoc;
    for (size_t i = 0; i < keyValues.size(); ++i) {
        std::vector<std::string> keyValue = autil::StringUtil::split(keyValues[i], DP_KEY_VALUE_EQUAL_SYMBOL);

        std::string key = keyValue[0];
        autil::StringUtil::trim(key);

        std::string value;
        if (keyValue.size() == 2) {
            value = keyValue[1];
            autil::StringUtil::trim(value);
        }

        if (!rawDoc) {
            rawDoc.reset(new RawDocument);
        }
        if (key == DP_MAIN_JOIN_FIELD) {
            rawDoc->SetField(indexlib::MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, value);
        } else if (key == DP_SUB_JOIN_FIELD) {
            rawDoc->SetField(indexlib::SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, value);
        } else {
            assert(keyValues[i].find(DP_KEY_VALUE_EQUAL_SYMBOL) != keyValues[i].npos);
            rawDoc->SetField(key, value);
        }
    }
    return rawDoc;
}

std::shared_ptr<Result> DocumentParser::ParseResult(const std::string& docStr)
{
    auto result = std::make_shared<Result>();

    std::vector<std::string> docStrings = autil::StringUtil::split(docStr, DP_CMD_SEPARATOR);
    for (size_t i = 0; i < docStrings.size(); ++i) {
        std::shared_ptr<RawDocument> doc = Parse(docStrings[i]);
        result->AddDoc(doc);
    }
    return result;
}

// TokenizeSectionPtr DocumentParser::ParseSection(const string& sectionStr, const std::string& sep)
// {
//     return document::IndexFieldConvertor::ParseSection(sectionStr, sep);
// }

std::string DocumentParser::ParseMultiValueField(const std::string& fieldValue, bool replaceSep)
{
    std::string result = fieldValue;
    if (replaceSep) {
        autil::StringUtil::replace(result, DP_MULTI_VALUE_SEPARATOR, MULTI_VALUE_SEPARATOR);
    }
    return result;
}

std::string DocumentParser::ParseMultiValueResult(const std::string& resultValue)
{
    std::string fieldValue = resultValue;
    autil::StringUtil::replace(fieldValue, MULTI_VALUE_SEPARATOR, DP_MULTI_VALUE_SEPARATOR);
    return fieldValue;
}
}} // namespace indexlibv2::table

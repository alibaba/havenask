#include "indexlib/test/document_parser.h"

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "indexlib/document/index_field_convertor.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, DocumentParser);

const string DocumentParser::DP_SPATIAL_KEY_VALUE_SEPARATOR = "|";
const string DocumentParser::DP_KEY_VALUE_SEPARATOR = ",";
const string DocumentParser::DP_KEY_VALUE_EQUAL_SYMBOL = "=";
const string DocumentParser::DP_CMD_SEPARATOR = ";";
const string DocumentParser::DP_TOKEN_SEPARATOR = " ";
const char DocumentParser::DP_MULTI_VALUE_SEPARATOR = ' ';

const string DocumentParser::DP_TAG_HASH_ID_FIELD = "_tag_hash_id_";
const string DocumentParser::DP_MAIN_JOIN_FIELD = "main_join";
const string DocumentParser::DP_SUB_JOIN_FIELD = "sub_join";

DocumentParser::DocumentParser() {}

DocumentParser::~DocumentParser() {}

RawDocumentPtr DocumentParser::Parse(const string& docStr)
{
    vector<string> keyValues = StringUtil::split(docStr, DP_SPATIAL_KEY_VALUE_SEPARATOR);
    if (keyValues.size() <= 1) {
        auto newStr = docStr;
        StringUtil::replaceAll(newStr, "\\,", "\020");
        keyValues = StringUtil::split(newStr, DP_KEY_VALUE_SEPARATOR);
    }

    RawDocumentPtr rawDoc;
    for (size_t i = 0; i < keyValues.size(); ++i) {
        StringUtil::replaceAll(keyValues[i], "\020", ",");
        vector<string> keyValue = StringUtil::split(keyValues[i], DP_KEY_VALUE_EQUAL_SYMBOL);

        string key = keyValue[0];
        StringUtil::trim(key);

        string value;
        if (keyValue.size() == 2) {
            value = keyValue[1];
            StringUtil::trim(value);
        }

        if (!rawDoc) {
            rawDoc.reset(new RawDocument);
        }
        if (key == DP_MAIN_JOIN_FIELD) {
            rawDoc->SetField(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, value);
        } else if (key == DP_SUB_JOIN_FIELD) {
            rawDoc->SetField(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, value);
        } else {
            assert(keyValues[i].find(DP_KEY_VALUE_EQUAL_SYMBOL) != keyValues[i].npos);
            rawDoc->SetField(key, value);
        }
    }
    return rawDoc;
}

ResultPtr DocumentParser::ParseResult(const string& docStr)
{
    ResultPtr result(new test::Result);

    vector<string> docStrings = StringUtil::split(docStr, DP_CMD_SEPARATOR);
    for (size_t i = 0; i < docStrings.size(); ++i) {
        RawDocumentPtr doc = Parse(docStrings[i]);
        result->AddDoc(doc);
    }
    return result;
}

TokenizeSectionPtr DocumentParser::ParseSection(const string& sectionStr, const std::string& sep)
{
    return document::IndexFieldConvertor::ParseSection(sectionStr, sep);
}

string DocumentParser::ParseMultiValueField(const string& fieldValue, bool replaceSep)
{
    string result = fieldValue;
    if (replaceSep) {
        StringUtil::replace(result, DP_MULTI_VALUE_SEPARATOR, MULTI_VALUE_SEPARATOR);
    }
    return result;
}

string DocumentParser::ParseMultiValueResult(const string& resultValue)
{
    string fieldValue = resultValue;
    StringUtil::replace(fieldValue, MULTI_VALUE_SEPARATOR, DP_MULTI_VALUE_SEPARATOR);
    return fieldValue;
}
}} // namespace indexlib::test

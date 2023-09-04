#include "indexlib/document/normal/test/TokenizeHelper.h"

#include "autil/StringUtil.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/normal/test/SimpleTokenizer.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/primary_key/Common.h"

using namespace std;
using namespace autil;

namespace indexlibv2 { namespace document {

AUTIL_LOG_SETUP(indexlib.document, TokenizeHelper);

const string TokenizeHelper::LAST_VALUE_PREFIX = "__last_value__";

TokenizeHelper::TokenizeHelper() {}

TokenizeHelper::TokenizeHelper(const TokenizeHelper& other)
{
    _schema = other._schema;
    _invertedFieldIds = other._invertedFieldIds;
}

TokenizeHelper::~TokenizeHelper() {}

bool TokenizeHelper::init(const std::shared_ptr<config::ITabletSchema>& schemaPtr)
{
    _schema = schemaPtr;
    auto insertFieldIds = [](const vector<shared_ptr<config::FieldConfig>>& fieldConfigs, set<fieldid_t>& s) {
        for (const auto& fieldConfig : fieldConfigs) {
            s.insert(fieldConfig->GetFieldId());
        }
    };
    insertFieldIds(_schema->GetIndexFieldConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR), _invertedFieldIds);
    insertFieldIds(_schema->GetIndexFieldConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR), _invertedFieldIds);

    _property._isStopWord = 0;
    _property._isSpace = 0;
    _property._isBasicRetrieve = 1;
    _property._isDelimiter = 0;
    _property._isRetrieve = 1;
    _property._needSetText = 1;
    _property._unused = 0;

    _emptyProperty._isStopWord = 0;
    _emptyProperty._isSpace = 1;
    _emptyProperty._isBasicRetrieve = 1;
    _emptyProperty._isDelimiter = 0;
    _emptyProperty._isRetrieve = 1;
    _emptyProperty._needSetText = 1;
    _emptyProperty._unused = 0;

    _stopwordProperty._isStopWord = 1;
    _stopwordProperty._isSpace = 0;
    _stopwordProperty._isBasicRetrieve = 1;
    _stopwordProperty._isDelimiter = 0;
    _stopwordProperty._isRetrieve = 1;
    _stopwordProperty._needSetText = 1;
    _stopwordProperty._unused = 0;
    return true;
}

bool TokenizeHelper::processField(const std::shared_ptr<NormalExtendDocument>& document,
                                  const std::shared_ptr<config::FieldConfig>& fieldConfig, const std::string& fieldName,
                                  const std::shared_ptr<indexlib::document::TokenizeDocument>& tokenizeDocument)
{
    FieldType fieldType = fieldConfig->GetFieldType();
    if (fieldType == ft_raw) {
        return true;
    }

    const std::shared_ptr<RawDocument>& rawDocument = document->getRawDocument();
    const autil::StringView& fieldValue = rawDocument->getField(autil::StringView(fieldName));
    AUTIL_LOG(DEBUG, "fieldtype:[%d], fieldname:[%s], fieldvalue:[%s]", fieldType, fieldName.c_str(),
              string(fieldValue.data(), fieldValue.size()).c_str());

    fieldid_t fieldId = fieldConfig->GetFieldId();
    if (tokenizeDocument->getField(fieldId) != NULL) {
        AUTIL_LOG(DEBUG, "fieldName:[%s] has already been tokenized", fieldName.c_str());
        return true;
    }
    const std::shared_ptr<indexlib::document::TokenizeField>& field = tokenizeDocument->createField(fieldId);
    if (field.get() == NULL) {
        stringstream ss;
        ss << "create TokenizeField failed: [" << fieldId << "]";
        string errorMsg = ss.str();
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    bool setNull = fieldConfig->IsEnableNullField() and ((fieldValue.empty() && !rawDocument->exist(fieldName)) ||
                                                         fieldValue == fieldConfig->GetNullFieldLiteralString());
    if (setNull) {
        field->setNull(true);
        return true;
    }
    bool ret = true;
    if (ft_text == fieldType) {
        ret = tokenizeTextField(field, fieldValue);
    } else if (ft_location == fieldType || ft_line == fieldType || ft_polygon == fieldType) {
        ret = tokenizeSingleValueField(field, fieldValue);
    } else if (fieldConfig->IsMultiValue() && _invertedFieldIds.find(fieldId) != _invertedFieldIds.end()) {
        ret = tokenizeMultiValueField(field, fieldValue);
    } else if (_invertedFieldIds.find(fieldId) != _invertedFieldIds.end()) {
        ret = tokenizeSingleValueField(field, fieldValue);
    } else {
        return true;
    }
    if (!ret) {
        string errorMsg = "Failed to Tokenize field:[" + fieldConfig->GetFieldName() + "]";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool TokenizeHelper::process(const std::shared_ptr<NormalExtendDocument>& document)
{
    assert(document);
    const std::shared_ptr<indexlib::document::TokenizeDocument>& tokenizeDocument = document->getTokenizeDocument();
    const std::shared_ptr<indexlib::document::TokenizeDocument>& lastTokenizeDocument =
        document->getLastTokenizeDocument();
    const auto& fieldConfigs = _schema->GetFieldConfigs();
    tokenizeDocument->reserve(fieldConfigs.size());
    lastTokenizeDocument->reserve(fieldConfigs.size());

    for (const auto& fieldConfig : fieldConfigs) {
        const string& fieldName = fieldConfig->GetFieldName();
        if (!processField(document, fieldConfig, fieldName, tokenizeDocument)) {
            return false;
        }
        std::string lastFieldName = LAST_VALUE_PREFIX + fieldName;
        const std::shared_ptr<RawDocument>& rawDocument = document->getRawDocument();
        if (!rawDocument->exist(lastFieldName)) {
            continue;
        }
        if (!processField(document, fieldConfig, lastFieldName, lastTokenizeDocument)) {
            return false;
        }
    }
    return true;
}

bool TokenizeHelper::tokenizeTextField(const std::shared_ptr<indexlib::document::TokenizeField>& field,
                                       const StringView& fieldValue)
{
    if (fieldValue.empty()) {
        return true;
    }

    bool ret = doTokenizeTextField(field, fieldValue);
    return ret;
}

bool TokenizeHelper::doTokenizeTextField(const std::shared_ptr<indexlib::document::TokenizeField>& field,
                                         const StringView& fieldValue)
{
    bool ret = true;
    indexlib::document::TokenizeSection* section = NULL;
    indexlib::document::TokenizeSection::Iterator iterator;

    vector<indexlib::document::AnalyzerToken> testTokens;
    vector<string> tokens;
    string cur;
    for (size_t i = 0; i < fieldValue.size(); i++) {
        char c = fieldValue.data()[i];
        if (c == ' ' || c == '#') {
            if (!cur.empty()) {
                tokens.push_back(cur);
                cur.clear();
            }
            tokens.push_back(string(1, c));
            continue;
        }
        cur += string(1, c);
    }
    if (!cur.empty()) {
        tokens.push_back(cur);
    }

    for (size_t i = 0; i < tokens.size(); i++) {
        if (tokens[i] == " ") {
            addToken(testTokens, " ", i, " ", _emptyProperty);
        } else if (tokens[i] == "#") {
            addToken(testTokens, "#", i, "#", _stopwordProperty);
        } else {
            string normWord = string(tokens[i].data(), tokens[i].size());
            char* iter = (char*)normWord.data();
            for (size_t j = 0; j < tokens[i].size(); j++) {
                if (iter[j] >= 'A' && iter[j] <= 'Z') {
                    iter[j] ^= 32;
                }
            }
            addToken(testTokens, tokens[i], i, normWord, _property);
        }
    }

    for (auto token : testTokens) {
        const string& text = token.getText();
        assert(text.length() > 0);
        if (text[0] == MULTI_VALUE_SEPARATOR && !token.isDelimiter()) {
            section = NULL;
            continue;
        }

        if (section == NULL) {
            section = field->getNewSection();
            if (NULL == section) {
                stringstream ss;
                ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
                string errorMsg = ss.str();
                AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
                ret = false;
                break;
            }
            iterator = section->createIterator();
        }

        if (token.isBasicRetrieve()) {
            section->insertBasicToken(token, iterator);
        } else {
            section->insertExtendToken(token, iterator);
        }
    }
    return ret;
}

bool TokenizeHelper::tokenizeMultiValueField(const std::shared_ptr<indexlib::document::TokenizeField>& field,
                                             const StringView& fieldValue)
{
    if (fieldValue.empty()) {
        return true;
    }
    string sep(1, MULTI_VALUE_SEPARATOR);
    SimpleTokenizer tokenizer(sep);
    tokenizer.tokenize(fieldValue.data(), fieldValue.size());

    indexlib::document::TokenizeSection* section = field->getNewSection();
    if (NULL == section) {
        stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        string errorMsg = ss.str();
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

bool TokenizeHelper::tokenizeSingleValueField(const std::shared_ptr<indexlib::document::TokenizeField>& field,
                                              const StringView& fieldValue)
{
    if (fieldValue.empty()) {
        return true;
    }

    indexlib::document::TokenizeSection* section = field->getNewSection();
    if (NULL == section) {
        stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        string errorMsg = ss.str();
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    indexlib::document::TokenizeSection::Iterator iterator = section->createIterator();

    indexlib::document::AnalyzerToken token;
    string tokenStr(fieldValue.data(), fieldValue.size());
    token.setText(tokenStr);
    token.setNormalizedText(tokenStr);
    section->insertBasicToken(token, iterator);
    return true;
}

void TokenizeHelper::destroy() { delete this; }

void TokenizeHelper::addToken(vector<indexlib::document::AnalyzerToken>& tokens, string text, int pos, string normText,
                              indexlib::document::AnalyzerToken::TokenProperty& property)
{
    indexlib::document::AnalyzerToken token(text, pos, normText);
    token._tokenProperty = property;
    tokens.push_back(token);
}
}} // namespace indexlibv2::document

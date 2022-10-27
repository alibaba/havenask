#include <autil/StringUtil.h>
#include "indexlib/document/document_parser/normal_parser/test/tokenize_helper.h"
#include "indexlib/document/document_parser/normal_parser/test/simple_tokenizer.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);

IE_LOG_SETUP(document, TokenizeHelper);

static const char SECTION_WEIGHT_SEPARATOR = '\x1C';

TokenizeHelper::TokenizeHelper()
{
}

TokenizeHelper::TokenizeHelper(
        const TokenizeHelper &other)
{
    _indexSchema.reset(other._indexSchema.get());
    _fieldSchema.reset(other._fieldSchema.get());
}

TokenizeHelper::~TokenizeHelper() {
}

bool TokenizeHelper::init(const IndexPartitionSchemaPtr& schemaPtr)
{
    _fieldSchema = schemaPtr->GetFieldSchema();
    _indexSchema = schemaPtr->GetIndexSchema();

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

bool TokenizeHelper::process(const IndexlibExtendDocumentPtr &document)
{
    assert(document);
    const RawDocumentPtr &rawDocument = document->getRawDocument();
    const TokenizeDocumentPtr &tokenizeDocument = document->getTokenizeDocument();
    tokenizeDocument->reserve(_fieldSchema->GetFieldCount());
    for (FieldSchema::Iterator it = _fieldSchema->Begin();
         it != _fieldSchema->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();
        const string &fieldName = (*it)->GetFieldName();
        if (tokenizeDocument->getField(fieldId) != NULL) {
            IE_LOG(DEBUG, "fieldName:[%s] has already been tokenized", fieldName.c_str());
            continue;
        }
        const autil::ConstString &fieldValue = rawDocument->getField(ConstString(fieldName));
        FieldType fieldType = (*it)->GetFieldType();
        IE_LOG(DEBUG, "fieldtype:[%d], fieldname:[%s], fieldvalue:[%s]",
               fieldType, fieldName.c_str(), string(fieldValue.data(), fieldValue.size()).c_str());

        if (fieldType == ft_raw) {
            continue;
        }
        
        const TokenizeFieldPtr &field = tokenizeDocument->createField(fieldId);
        if (field.get() == NULL) {
            stringstream ss;
            ss << "create TokenizeField failed: [" << fieldId << "]";
            string errorMsg = ss.str();
            IE_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }

        bool ret = true;
        if (ft_text == fieldType) {
            ret = tokenizeTextField(field, fieldValue);
        }else if (ft_location == fieldType || ft_line == fieldType || ft_polygon == fieldType) {
            ret = tokenizeSingleValueField(field, fieldValue);
        }else if ((*it)->IsMultiValue()
                   && _indexSchema->IsInIndex(fieldId)) {
            ret = tokenizeMultiValueField(field, fieldValue);
        } else if(_indexSchema->IsInIndex(fieldId)) {
            ret = tokenizeSingleValueField(field, fieldValue);
        } else {
            continue;
        }

        if (!ret)
        {
            string errorMsg = "Failed to Tokenize field:[" + fieldName + "]";
            IE_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

bool TokenizeHelper::tokenizeTextField(
        const TokenizeFieldPtr &field,
        const ConstString &fieldValue)
{
    if (fieldValue.empty()) {
        return true;
    }

    bool ret = doTokenizeTextField(field, fieldValue);
    return ret;
}

bool TokenizeHelper::doTokenizeTextField(const TokenizeFieldPtr &field,
        const ConstString &fieldValue)
{
    bool ret = true;
    TokenizeSection *section = NULL;
    TokenizeSection::Iterator iterator;

    vector<AnalyzerToken> testTokens;
    vector<string> tokens;
    string cur;
    for (size_t i = 0; i < fieldValue.size(); i++)
    {
        char c = fieldValue.data()[i];
        if (c == ' ' || c == '#')
        {
            if (!cur.empty())
            {
                tokens.push_back(cur);
                cur.clear();
            }
            tokens.push_back(string(1, c));
            continue;
        }
        cur += string(1, c);
    }
    if (!cur.empty())
    {
        tokens.push_back(cur);
    }

    for (size_t i = 0; i < tokens.size(); i++)
    {
        if (tokens[i] == " ")
        {
            addToken(testTokens, " ", i, " ", _emptyProperty);
        }
        else if (tokens[i] == "#")
        {
            addToken(testTokens, "#", i, "#", _stopwordProperty);
        }
        else
        {
            string normWord = string(tokens[i].data(), tokens[i].size());
            char* iter = (char*)normWord.data();
            for (size_t j = 0; j < tokens[i].size(); j++)
            {
                if (iter[j] >= 'A' && iter[j] <= 'Z')
                {
                    iter[j] ^= 32;
                }
            }
            addToken(testTokens, tokens[i], i, normWord, _property);
        }
    }
    
    
    for (auto token : testTokens) {
        const string &text = token.getText();
        assert(text.length() > 0);
        if (text[0] == MULTI_VALUE_SEPARATOR
            && !token.isDelimiter())
        {
            section = NULL;
            continue;
        }

        if (section == NULL) {
            section = field->getNewSection();
            if(NULL == section) {
                stringstream ss;
                ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
                string errorMsg = ss.str();
                IE_LOG(ERROR, "%s", errorMsg.c_str());
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

bool TokenizeHelper::tokenizeMultiValueField(const TokenizeFieldPtr &field,
        const ConstString &fieldValue)
{
    if (fieldValue.empty()) {
        return true;
    }
    string sep(1, MULTI_VALUE_SEPARATOR);
    SimpleTokenizer tokenizer(sep);
    tokenizer.tokenize(fieldValue.c_str(), fieldValue.size());

    TokenizeSection *section = field->getNewSection();
    if (NULL == section) {
        stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    TokenizeSection::Iterator iterator = section->createIterator();
    AnalyzerToken token;
    while (tokenizer.next(token)) {
        token.setText(token.getNormalizedText());
        section->insertBasicToken(token, iterator);
    }

    return true;
}

bool TokenizeHelper::tokenizeSingleValueField(
        const TokenizeFieldPtr &field, const ConstString &fieldValue)
{
    if (fieldValue.empty()) {
        return true;
    }

    TokenizeSection *section = field->getNewSection();
    if(NULL == section) {
        stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    TokenizeSection::Iterator iterator = section->createIterator();

    AnalyzerToken token;
    string tokenStr(fieldValue.data(), fieldValue.size());
    token.setText(tokenStr);
    token.setNormalizedText(tokenStr);
    section->insertBasicToken(token, iterator);
    return true;
}

void TokenizeHelper::destroy() {
    delete this;
}

void TokenizeHelper::addToken(vector<AnalyzerToken>& tokens,
                              string text,
                              int pos, string normText,
                              AnalyzerToken::TokenProperty& property)
{
    AnalyzerToken token(text, pos, normText);
    token._tokenProperty = property;
    tokens.push_back(token);
}
IE_NAMESPACE_END(document);

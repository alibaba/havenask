#include <indexlib/config/index_partition_schema.h>
#include <autil/StringUtil.h>
#include "build_service/processor/TokenizeDocumentProcessor.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/analyzer/Analyzer.h"
#include "build_service/analyzer/SimpleTokenizer.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
using namespace build_service::document;
using namespace build_service::analyzer;

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, TokenizeDocumentProcessor);

static const char SECTION_WEIGHT_SEPARATOR = '\x1C';

const string TokenizeDocumentProcessor::PROCESSOR_NAME = "TokenizeDocumentProcessor";
TokenizeDocumentProcessor::TokenizeDocumentProcessor()
{
}

TokenizeDocumentProcessor::
TokenizeDocumentProcessor(const TokenizeDocumentProcessor &other)
    : DocumentProcessor(other)
    , _indexSchema(other._indexSchema)
    , _fieldSchema(other._fieldSchema)
    , _analyzerFactoryPtr(other._analyzerFactoryPtr)
{
}

TokenizeDocumentProcessor::~TokenizeDocumentProcessor() {
}

bool TokenizeDocumentProcessor::init(const DocProcessorInitParam &param)
{
    assert(param.schemaPtr);
    _fieldSchema = param.schemaPtr->GetFieldSchema();
    _indexSchema = param.schemaPtr->GetIndexSchema();
    _analyzerFactoryPtr = param.analyzerFactoryPtr;
    return checkAnalyzers();
}

bool TokenizeDocumentProcessor::checkAnalyzers()
{
    FieldSchema::Iterator it = _fieldSchema->Begin();
    for (; it != _fieldSchema->End(); ++it)
    {
        const FieldConfigPtr &fieldConfig = *it;
        if (!fieldConfig->IsNormal()) {
            continue;
        }
        if (fieldConfig->GetFieldType() == ft_text) {
            const string &analyzerName = fieldConfig->GetAnalyzerName();
            if (analyzerName.empty()) {
                string errorMsg = "Get analyzer FAIL, fieldName = ["
                                  + fieldConfig->GetFieldName() + "]";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }

            if (!_analyzerFactoryPtr->hasAnalyzer(analyzerName)) {
                string errorMsg = "create Analyzer failed: [" + analyzerName + "]";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        }
    }
    return true;
}

void TokenizeDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool TokenizeDocumentProcessor::process(const ExtendDocumentPtr &document)
{
    assert(document);
    const RawDocumentPtr &rawDocument = document->getRawDocument();
    const TokenizeDocumentPtr &tokenizeDocument = document->getTokenizeDocument();
    tokenizeDocument->reserve(_fieldSchema->GetFieldCount());
    for (FieldSchema::Iterator it = _fieldSchema->Begin();
         it != _fieldSchema->End(); ++it)
    {
        if (!(*it)->IsNormal()) {
            continue;
        }

        fieldid_t fieldId = (*it)->GetFieldId();
        const string &fieldName = (*it)->GetFieldName();
        if (tokenizeDocument->getField(fieldId) != NULL) {
            BS_LOG(DEBUG, "fieldName:[%s] has already been tokenized", fieldName.c_str());
            continue;
        }
        const autil::ConstString &fieldValue = rawDocument->getField(ConstString(fieldName));
        FieldType fieldType = (*it)->GetFieldType();
        BS_LOG(DEBUG, "fieldtype:[%d], fieldname:[%s], fieldvalue:[%s]",
               fieldType, fieldName.c_str(), string(fieldValue.data(), fieldValue.size()).c_str());

        if (fieldType == ft_raw) {
            continue;
        }
        
        const TokenizeFieldPtr &field = tokenizeDocument->createField(fieldId);
        if (field.get() == NULL) {
            stringstream ss;
            ss << "create TokenizeField failed: [" << fieldId << "]";
            string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }

        bool ret = true;
        if (ft_text == fieldType) {
            const string &fieldAnalyzerName = document->getFieldAnalyzerName(fieldId);
            ret = tokenizeTextField(field, fieldValue, fieldAnalyzerName);
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
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

Analyzer* TokenizeDocumentProcessor::getAnalyzer(fieldid_t fieldId,
        const string &fieldAnalyzerName) const
{
    if (!fieldAnalyzerName.empty()) {
        return _analyzerFactoryPtr->createAnalyzer(fieldAnalyzerName);
    }

    const FieldConfigPtr &fieldConfig = _fieldSchema->GetFieldConfig(fieldId);
    if (!fieldConfig) {
        return NULL;
    }
    const string &analyzerName = fieldConfig->GetAnalyzerName();
    Analyzer *analyzer = _analyzerFactoryPtr->createAnalyzer(analyzerName);
    return analyzer;
}

bool TokenizeDocumentProcessor::tokenizeTextField(const TokenizeFieldPtr &field,
        const ConstString &fieldValue, const string &fieldAnalyzerName)
{
    if (fieldValue.empty()) {
        return true;
    }

    fieldid_t fieldId = field->getFieldId();
    Analyzer* analyzer = getAnalyzer(fieldId, fieldAnalyzerName);
    if (!analyzer) {
        stringstream ss;
        ss << "Get analyzer FAIL, fieldId = [" << fieldId << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    bool ret = doTokenizeTextField(field, fieldValue, analyzer);
    delete analyzer;
    return ret;
}

bool TokenizeDocumentProcessor::doTokenizeTextField(const TokenizeFieldPtr &field,
        const ConstString &fieldValue, Analyzer *analyzer)
{
    bool ret = true;
    TokenizeSection *section = NULL;
    TokenizeSection::Iterator iterator;
    analyzer->tokenize(fieldValue.c_str(), fieldValue.size());

    analyzer::Token token;

    while (analyzer->next(token)) {
        const string &text = token.getText();
        assert(text.length() > 0);
        if (text[0] == SECTION_WEIGHT_SEPARATOR) {
            if(extractSectionWeight(section, *analyzer)) {
                section = NULL;
                continue;
            } else {
                ret = false;
                break;;
            }
        }
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
                BS_LOG(ERROR, "%s", errorMsg.c_str());
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

bool TokenizeDocumentProcessor::findFirstNonDelimiterToken(
        const Analyzer &analyzer, analyzer::Token &token) const
{
    bool found = false;
    while (analyzer.next(token)) {
        if (!token.isDelimiter()) {
            found = true;
            break;
        }
    }
    return found;
}

bool TokenizeDocumentProcessor::extractSectionWeight(TokenizeSection *section,
        const analyzer::Analyzer &analyzer)
{
    analyzer::Token token;
    if (NULL == section) {
        analyzer.next(token);
        return true;
    }
    if (!findFirstNonDelimiterToken(analyzer, token)) {
        return false;
    } else {
        string &text = token.getText();
        assert(text.length() > 0);
        int32_t sectionWeight;
        if (text[0] == MULTI_VALUE_SEPARATOR) {
            sectionWeight = 0;
        } else {
            if(!StringUtil::strToInt32(text.c_str(), sectionWeight)) {
                stringstream ss;
                ss << "cast section weight failed, [" << text << "](" << text.c_str()[0] << ")";
                string errorMsg = ss.str();
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return false;
            }
            if (!findFirstNonDelimiterToken(analyzer, token)) {
                return false;
            } else {
                text = token.getText();
                assert(text.length() > 0);
                if (text[0] != MULTI_VALUE_SEPARATOR ) {
                    return false;
                }
            }
        }
        section->setSectionWeight((section_weight_t)sectionWeight);
        return true;
    }
}

bool TokenizeDocumentProcessor::tokenizeMultiValueField(const TokenizeFieldPtr &field,
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
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    TokenizeSection::Iterator iterator = section->createIterator();
    analyzer::Token token;
    while (tokenizer.next(token)) {
        token.setText(token.getNormalizedText());
        section->insertBasicToken(token, iterator);
    }

    return true;
}

bool TokenizeDocumentProcessor::tokenizeSingleValueField(
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
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    TokenizeSection::Iterator iterator = section->createIterator();

    analyzer::Token token;
    string tokenStr(fieldValue.data(), fieldValue.size());
    token.setText(tokenStr);
    token.setNormalizedText(tokenStr);
    section->insertBasicToken(token, iterator);
    return true;
}

void TokenizeDocumentProcessor::destroy() {
    delete this;
}

DocumentProcessor* TokenizeDocumentProcessor::clone() {
    return new TokenizeDocumentProcessor(*this);
}

}
}

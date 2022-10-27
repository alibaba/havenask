#include "indexlib/document/document_parser/normal_parser/extend_doc_fields_convertor.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/misc/doc_tracer.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, ExtendDocFieldsConvertor);

const uint32_t ExtendDocFieldsConvertor::MAX_TOKEN_PER_SECTION = Section::MAX_TOKEN_PER_SECTION;

ExtendDocFieldsConvertor::ExtendDocFieldsConvertor(
        const IndexPartitionSchemaPtr &schema, regionid_t regionId)
    : _schema(schema)
    , _regionId(regionId)
{
    init();
}

ExtendDocFieldsConvertor::~ExtendDocFieldsConvertor() {
    HasherVector::iterator iter = _fieldTokenHasherVec.begin();
    for (; iter != _fieldTokenHasherVec.end(); ++iter) {
        if (*iter) {
            delete *iter;
        }
    }
    _fieldTokenHasherVec.clear();
}

void ExtendDocFieldsConvertor::convertIndexField(
        const IndexlibExtendDocumentPtr &document, const FieldConfigPtr &fieldConfig)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    const ClassifiedDocumentPtr &classifiedDoc = document->getClassifiedDocument();

    FieldType fieldType = fieldConfig->GetFieldType();
    if (fieldType == ft_raw) {
        const string& fieldValue =
            document->getRawDocument()->getField(fieldConfig->GetFieldName());
        if (fieldValue.empty())
        {
            return;
        }
        auto indexRawField =
            dynamic_cast<IndexRawField*>(classifiedDoc->createIndexField(
                            fieldConfig->GetFieldId(), Field::FieldTag::RAW_FIELD));
        assert(indexRawField);
        indexRawField->SetData(ConstString(fieldValue, classifiedDoc->getPool()));
        return;
    }
    const TokenizeDocumentPtr &tokenizeDoc = document->getTokenizeDocument();
    const TokenizeFieldPtr &tokenizeField = tokenizeDoc->getField(fieldId);
    if (!tokenizeField || tokenizeField->isEmpty()) {
        return;
    }

    Field* field = classifiedDoc->createIndexField(fieldId, Field::FieldTag::TOKEN_FIELD);

    IndexTokenizeField* indexTokenField = static_cast<IndexTokenizeField*>(field);
    transTokenizeFieldToField(tokenizeField, indexTokenField, fieldId, classifiedDoc);
}

void ExtendDocFieldsConvertor::convertAttributeField(
        const IndexlibExtendDocumentPtr &document, const FieldConfigPtr &fieldConfig)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    if ((size_t)fieldId >= _attrConvertVec.size()) {
        stringstream ss;
        ss << "field config error: fieldName[" <<  fieldConfig->GetFieldName()
           << "], fieldId[" << fieldId << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    }
    const AttributeConvertorPtr &convertor = _attrConvertVec[fieldId];
    assert(convertor);
    const ClassifiedDocumentPtr &classifiedDoc = document->getClassifiedDocument();
    const AttributeDocumentPtr& attrDoc = classifiedDoc->getAttributeDoc();
    const ConstString &fieldValue = classifiedDoc->getAttributeField(fieldId);
    if (fieldValue.empty()) {
        const RawDocumentPtr &rawDoc = document->getRawDocument();
        const ConstString &rawField = rawDoc->getField(ConstString(fieldConfig->GetFieldName()));
        if (rawField.data() == NULL) {
            IE_LOG(DEBUG, "field [%s] not exist in RawDocument!",
                   fieldConfig->GetFieldName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "field [%s] not exist in RawDocument [%s]!",
                    fieldConfig->GetFieldName().c_str(), rawDoc->toString().c_str());
            IE_RAW_DOC_FORMAT_TRACE(rawDoc, "parse error: field [%s] not exist in RawDocument!", fieldConfig->GetFieldName().c_str());
            attrDoc->SetFormatError(true);
        }
        ConstString convertedValue = convertor->Encode(
                rawField, classifiedDoc->getPool(), attrDoc->GetFormatErrorLable());
        classifiedDoc->setAttributeFieldNoCopy(fieldId, convertedValue);
    } else {
        ConstString convertedValue = convertor->Encode(
                fieldValue, classifiedDoc->getPool(), attrDoc->GetFormatErrorLable());
        classifiedDoc->setAttributeFieldNoCopy(fieldId, convertedValue);
    }
}
void ExtendDocFieldsConvertor::convertSummaryField(
        const IndexlibExtendDocumentPtr &document, const FieldConfigPtr &fieldConfig)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    const ClassifiedDocumentPtr &classifiedDoc = document->getClassifiedDocument();
    const ConstString &pluginSetField = classifiedDoc->getSummaryField(fieldId);
    if (!pluginSetField.empty()) {
        return;
    }
    if (fieldConfig->GetFieldType() != ft_text) {
        const RawDocumentPtr &rawDoc = document->getRawDocument();
        const string &fieldName = fieldConfig->GetFieldName();
        const ConstString &fieldValue = rawDoc->getField(ConstString(fieldName));
        // memory is in raw document.
        // and will serialize to indexlib, so do not copy here.
        classifiedDoc->setSummaryFieldNoCopy(fieldId, fieldValue);
    } else {
        const TokenizeDocumentPtr &tokenizeDoc = document->getTokenizeDocument();
        const TokenizeFieldPtr &tokenizeField = tokenizeDoc->getField(fieldId);
        string summaryStr = transTokenizeFieldToSummaryStr(tokenizeField);
        // need copy
        classifiedDoc->setSummaryField(fieldId, summaryStr);
    }
}

string ExtendDocFieldsConvertor::transTokenizeFieldToSummaryStr(
        const TokenizeFieldPtr &tokenizeField)
{
    string summaryStr;
    if (!tokenizeField.get()) {
        return summaryStr;
    }

    TokenizeField::Iterator it = tokenizeField->createIterator();
    while (!it.isEnd()) {
        if ((*it) == NULL) {
            it.next();
            continue;
        }
        TokenizeSection::Iterator tokenIter = (*it)->createIterator();
        while (tokenIter) {
            if (!summaryStr.empty()) {
                summaryStr.append("\t");
            }
            const string &text = (*tokenIter)->getText();
            summaryStr.append(text.begin(), text.end());
            tokenIter.nextBasic();
        }
        it.next();
    }
    return summaryStr;
}

void ExtendDocFieldsConvertor::transTokenizeFieldToField(
        const TokenizeFieldPtr& tokenizeField, IndexTokenizeField *field,
        fieldid_t fieldId, const ClassifiedDocumentPtr &classifiedDoc)
{
    TokenizeField::Iterator it = tokenizeField->createIterator();
    if (_spatialFieldEncoder->IsSpatialIndexField(fieldId))
    {
        addSectionTokens(_spatialFieldEncoder, field, *it,
                         classifiedDoc->getPool(), fieldId, classifiedDoc);
        return;
    }
    if (_dateFieldEncoder->IsDateIndexField(fieldId))
    {
        addSectionTokens(_dateFieldEncoder, field, *it,
                         classifiedDoc->getPool(), fieldId, classifiedDoc);
        return;
    }
    if (_rangeFieldEncoder->IsRangeIndexField(fieldId))
    {
        addSectionTokens(_rangeFieldEncoder, field, *it,
                         classifiedDoc->getPool(), fieldId, classifiedDoc);
        return;
    }
    assert(tokenizeField);
    pos_t lastTokenPos = 0;
    pos_t curPos = -1;
    while(!it.isEnd()) {
        TokenizeSection *section = *it;
        if (!addSection(field, section, classifiedDoc->getPool(), fieldId,
                        classifiedDoc, lastTokenPos, curPos))
        {
            return;
        }
        it.next();
    }
}

bool ExtendDocFieldsConvertor::addSection(
        IndexTokenizeField *field, TokenizeSection *tokenizeSection,
        Pool *pool, fieldid_t fieldId,
        const ClassifiedDocumentPtr &classifiedDoc,
        pos_t &lastTokenPos, pos_t &curPos)
{
    //TODO: empty section
    uint32_t leftTokenCount = tokenizeSection->getTokenCount();
    Section *indexSection = classifiedDoc->createSection(
            field, leftTokenCount, tokenizeSection->getSectionWeight());
    if (indexSection == NULL) {
        IE_LOG(DEBUG, "Failed to create new section.");
        return false;
    }
    TokenizeSection::Iterator it = tokenizeSection->createIterator();
    section_len_t nowSectionLen = 0;
    section_len_t maxSectionLen = classifiedDoc->getMaxSectionLenght();
    while (*it != NULL) {
        if ((*it)->isSpace() || (*it)->isDelimiter()) {
            it.nextBasic();
            leftTokenCount--;
            continue;
        }

        if (nowSectionLen + 1 >= maxSectionLen) {
            indexSection->SetLength(nowSectionLen + 1);
            nowSectionLen = 0;
            indexSection = classifiedDoc->createSection(field,
                    leftTokenCount, tokenizeSection->getSectionWeight());
            if (indexSection == NULL) {
                IE_LOG(DEBUG, "Failed to create new section.");
                return false;
            }
            curPos++;
        }
        curPos++;
        if (!addToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos)) {
            break;
        }
        while (it.nextExtend()) {
            if (!addToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos)) {
                break;
            }
        }
        nowSectionLen++;
        leftTokenCount--;
        it.nextBasic();
    }
    indexSection->SetLength(nowSectionLen + 1);
    curPos++;
    return true;
}

bool ExtendDocFieldsConvertor::addToken(
        Section *indexSection, const AnalyzerToken *token,
        Pool *pool, fieldid_t fieldId, pos_t &lastTokenPos, pos_t &curPos)
{
    if (token->isSpace() || token->isStopWord()) {
        //do nothing
        return true;
    }

    const string& text = token->getNormalizedText();
    const KeyHasher *hasher = _fieldTokenHasherVec[fieldId];
    uint64_t hashKey;
    if (!hasher->GetHashKey(text.c_str(), hashKey)) {
        return true;
    }
    return addHashToken(indexSection, hashKey, pool, fieldId,
                        token->getPosPayLoad(), lastTokenPos, curPos);
}

bool ExtendDocFieldsConvertor::addHashToken(
        Section *indexSection, uint64_t hashKey,
        Pool *pool, fieldid_t fieldId, pospayload_t posPayload,
        pos_t &lastTokenPos, pos_t &curPos)
{
    Token *indexToken = NULL;
    indexToken = indexSection->CreateToken(hashKey);

    if (!indexToken) {
        curPos--;
        IE_LOG(INFO, "token count overflow in one section, max %u",
               MAX_TOKEN_PER_SECTION);
        ERROR_COLLECTOR_LOG(ERROR, "token count overflow in one section, max %u",
                            MAX_TOKEN_PER_SECTION);
        return false;
    }
    indexToken->SetPosPayload(posPayload);
    indexToken->SetPosIncrement(curPos - lastTokenPos);
    lastTokenPos = curPos;
    return true;
}

void ExtendDocFieldsConvertor::init() {
    initAttrConvert();
    initFieldTokenHasherVector();

    // TODO: _spatialFieldEncoder supports multi region
    _spatialFieldEncoder.reset(new SpatialFieldEncoder(_schema));
    _dateFieldEncoder.reset(new DateFieldEncoder(_schema));
    _rangeFieldEncoder.reset(new RangeFieldEncoder(_schema));
}

void ExtendDocFieldsConvertor::initFieldTokenHasherVector() {
    const FieldSchemaPtr &fieldSchemaPtr = _schema->GetFieldSchema(_regionId);
    const IndexSchemaPtr &indexSchemaPtr = _schema->GetIndexSchema(_regionId);
    if (!indexSchemaPtr) {
        return;
    }
    _fieldTokenHasherVec.resize(fieldSchemaPtr->GetFieldCount());
    for (FieldSchema::Iterator it = fieldSchemaPtr->Begin();
         it != fieldSchemaPtr->End(); ++it)
    {
        const FieldConfigPtr &fieldConfig = *it;
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (!indexSchemaPtr->IsInIndex(fieldId)) {
            continue;
        }
        FieldType fieldType = fieldConfig->GetFieldType();
        KeyHasher *hasher = KeyHasherFactory::CreateByFieldType(fieldType);
        assert(hasher);
        _fieldTokenHasherVec[fieldId] = hasher;
    }
}

void ExtendDocFieldsConvertor::initAttrConvert() {
    const FieldSchemaPtr &fieldSchemaPtr = _schema->GetFieldSchema(_regionId);
    if (!fieldSchemaPtr) {
        return;
    }
    _attrConvertVec.resize(fieldSchemaPtr->GetFieldCount());
    const AttributeSchemaPtr &attrSchemaPtr = _schema->GetAttributeSchema(_regionId);
    if (!attrSchemaPtr) {
        return;
    }

    TableType tableType = _schema->GetTableType();
    for (AttributeSchema::Iterator it = attrSchemaPtr->Begin();
         it != attrSchemaPtr->End(); ++it)
    {
        const FieldConfigPtr &fieldConfigPtr = (*it)->GetFieldConfig();
        _attrConvertVec[fieldConfigPtr->GetFieldId()].reset(
                AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(fieldConfigPtr, tableType));
    }
}

IE_NAMESPACE_END(document);

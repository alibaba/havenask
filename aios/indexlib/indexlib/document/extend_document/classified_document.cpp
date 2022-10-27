#include "indexlib/document/extend_document/classified_document.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/document/index_document/kv_document/kv_index_document.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, ClassifiedDocument);

const uint32_t ClassifiedDocument::MAX_SECTION_LENGTH = Section::MAX_SECTION_LENGTH;
const uint32_t ClassifiedDocument::MAX_TOKEN_PER_SECTION = Section::MAX_TOKEN_PER_SECTION;
const uint32_t ClassifiedDocument::MAX_SECTION_PER_FIELD = Field::MAX_SECTION_PER_FIELD;

ClassifiedDocument::ClassifiedDocument() {
    _pool.reset(new PoolType(1024));
    _indexDocument.reset(new IndexDocument(_pool.get()));
    _summaryDocument.reset(new SummaryDocument());
    _attributeDocument.reset(new AttributeDocument());
    _maxSectionLen = MAX_SECTION_LENGTH;
    _maxSectionPerField = MAX_SECTION_PER_FIELD;
}

ClassifiedDocument::~ClassifiedDocument() {
    _indexDocument.reset();
    _summaryDocument.reset();
    _attributeDocument.reset();
    _kvIndexDocument.reset();
    _pool.reset();
}

Field *ClassifiedDocument::createIndexField(fieldid_t fieldId, Field::FieldTag fieldTag) {
    assert(_indexDocument);
    return _indexDocument->CreateField(fieldId, fieldTag);
}

Section* ClassifiedDocument::createSection(
        IndexTokenizeField *field, uint32_t tokenNum, section_weight_t sectionWeight)
{
    Section *indexSection = field->CreateSection(tokenNum);
    if (nullptr == indexSection) {
        return nullptr;
    }

    indexSection->SetWeight(sectionWeight);
    if (field->GetSectionCount() >= _maxSectionPerField) {
        return nullptr;
    }
    return indexSection;
}

void ClassifiedDocument::setMaxSectionLen(section_len_t maxSectionLen)
{
    if (maxSectionLen < MAX_SECTION_LENGTH && maxSectionLen > 1) {
        _maxSectionLen = maxSectionLen;
    } else {
        _maxSectionLen = MAX_SECTION_LENGTH;
    }
}

void ClassifiedDocument::setMaxSectionPerField(uint32_t maxSectionPerField)
{
    if (maxSectionPerField < MAX_SECTION_PER_FIELD && maxSectionPerField > 0) {
        _maxSectionPerField = maxSectionPerField;
    } else {
        _maxSectionPerField = MAX_SECTION_PER_FIELD;
    }
}

void ClassifiedDocument::createKVIndexDocument(
    uint64_t pkeyHash, uint64_t skeyHash, bool hasSkey)
{
    _kvIndexDocument.reset(new KVIndexDocument(_pool.get()));
    _kvIndexDocument->SetPkeyHash(pkeyHash);        
    if (hasSkey)
    {
        _kvIndexDocument->SetSkeyHash(skeyHash);
    }
}

void ClassifiedDocument::serializeSummaryDocument(
        const SummarySchemaPtr& summarySchema)
{
    SummaryFormatter summaryFormatter(summarySchema);
    summaryFormatter.SerializeSummaryDoc(_summaryDocument, _serSummaryDoc);
}

void ClassifiedDocument::setAttributeField(fieldid_t id, const string& value)
{
    if (_attributeDocument) {
        _attributeDocument->SetField(id, ConstString(value, _pool.get()));
    }
}
    
void ClassifiedDocument::setAttributeField(fieldid_t id, const char *value, size_t len)
{
    if (_attributeDocument) {
        _attributeDocument->SetField(id, ConstString(value, len, _pool.get()));
    }
}

void ClassifiedDocument::setAttributeFieldNoCopy(fieldid_t id, const ConstString& value)
{
    if (_attributeDocument) {
        _attributeDocument->SetField(id, value);
    }
}
    
const ConstString& ClassifiedDocument::getAttributeField(fieldid_t fieldId)
{
    if (_attributeDocument) {
        return _attributeDocument->GetField(fieldId);
    } 
    return ConstString::EMPTY_STRING;
}

void ClassifiedDocument::setSummaryField(fieldid_t id, const string& value)
{
    if (_summaryDocument) {
        _summaryDocument->SetField(id, ConstString(value, _pool.get()));
    }
}
    
void ClassifiedDocument::setSummaryField(fieldid_t id, const char *value, size_t len)
{
    if (_summaryDocument) {
        _summaryDocument->SetField(id, ConstString(value, len, _pool.get()));
    }
}

void ClassifiedDocument::setSummaryFieldNoCopy(fieldid_t id, const ConstString& value)
{
    if (_summaryDocument) {
        _summaryDocument->SetField(id, value);
    }
}
    
const ConstString& ClassifiedDocument::getSummaryField(fieldid_t fieldId)
{
    if (_summaryDocument) {
        return _summaryDocument->GetField(fieldId);
    } 
    return ConstString::EMPTY_STRING;
}

termpayload_t ClassifiedDocument::getTermPayload(const string &termText) const
{
    assert(_indexDocument);
    return _indexDocument->GetTermPayload(termText);
}

termpayload_t ClassifiedDocument::getTermPayload(uint64_t intKey) const
{
    assert(_indexDocument);
    return _indexDocument->GetTermPayload(intKey);
}

void ClassifiedDocument::setTermPayload(const string &termText, termpayload_t payload)
{
    assert(_indexDocument);
    _indexDocument->SetTermPayload(termText, payload);
}

void ClassifiedDocument::setTermPayload(uint64_t intKey, termpayload_t payload)
{
    assert(_indexDocument);
    _indexDocument->SetTermPayload(intKey, payload);
}

docpayload_t ClassifiedDocument::getTermDocPayload(const string &termText) const
{
    assert(_indexDocument);
    return _indexDocument->GetDocPayload(termText);
}

docpayload_t ClassifiedDocument::getTermDocPayload(uint64_t intKey) const
{
    assert(_indexDocument);
    return _indexDocument->GetDocPayload(intKey);
}

void ClassifiedDocument::setTermDocPayload(const string &termText, docpayload_t docPayload)
{
    assert(_indexDocument);
    _indexDocument->SetDocPayload(termText, docPayload);
}

void ClassifiedDocument::setTermDocPayload(uint64_t intKey, docpayload_t docPayload)
{
    assert(_indexDocument);
    _indexDocument->SetDocPayload(intKey, docPayload);
}

void ClassifiedDocument::setPrimaryKey(const string &primaryKey)
{
    assert(_indexDocument);
    _indexDocument->SetPrimaryKey(primaryKey);
}

const string& ClassifiedDocument::getPrimaryKey()
{
    assert(_indexDocument);
    return _indexDocument->GetPrimaryKey();
}

void ClassifiedDocument::clear()
{
    _indexDocument.reset();
    _summaryDocument.reset();
    _attributeDocument.reset();
    _serSummaryDoc.reset();
    _kvIndexDocument.reset();
    _pool.reset();
}

IE_NAMESPACE_END(document);


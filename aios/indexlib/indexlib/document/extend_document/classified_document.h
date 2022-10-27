#ifndef __INDEXLIB_CLASSIFIED_DOCUMENT_H
#define __INDEXLIB_CLASSIFIED_DOCUMENT_H

#include <autil/mem_pool/Pool.h>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/index_raw_field.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"

DECLARE_REFERENCE_CLASS(config, SummarySchema);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(document, SummaryDocument);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(document, SerializedSummaryDocument);
DECLARE_REFERENCE_CLASS(document, KVIndexDocument);

IE_NAMESPACE_BEGIN(document);

class ClassifiedDocument
{
public:
    const static uint32_t MAX_SECTION_LENGTH;
    const static uint32_t MAX_TOKEN_PER_SECTION;
    const static uint32_t MAX_SECTION_PER_FIELD;

    typedef autil::mem_pool::Pool PoolType;
    typedef std::tr1::shared_ptr<PoolType> PoolTypePtr;
    
public:
    ClassifiedDocument();
    ~ClassifiedDocument();
    
public:
    void createKVIndexDocument(uint64_t pkeyHash, uint64_t skeyHash, bool hasSkey);
    
    void setAttributeField(fieldid_t id, const std::string& value);
    void setAttributeField(fieldid_t id, const char *value, size_t len);
    void setAttributeFieldNoCopy(fieldid_t id, const autil::ConstString& value);
    const autil::ConstString& getAttributeField(fieldid_t fieldId);
    
    void setSummaryField(fieldid_t id, const std::string& value);
    void setSummaryField(fieldid_t id, const char *value, size_t len);
    void setSummaryFieldNoCopy(fieldid_t id, const autil::ConstString& value);
    const autil::ConstString& getSummaryField(fieldid_t fieldId);

    termpayload_t getTermPayload(const std::string &termText) const;
    termpayload_t getTermPayload(uint64_t intKey) const;
    void setTermPayload(const std::string &termText, termpayload_t payload);
    void setTermPayload(uint64_t intKey, termpayload_t payload);

    docpayload_t getTermDocPayload(const std::string &termText) const;
    docpayload_t getTermDocPayload(uint64_t intKey) const;
    void setTermDocPayload(const std::string &termText, docpayload_t docPayload);
    void setTermDocPayload(uint64_t intKey, docpayload_t docPayload);

    void setPrimaryKey(const std::string &primaryKey);
    const std::string& getPrimaryKey();

public:
    // inner interface.
    IE_NAMESPACE(document)::Field *createIndexField(
            fieldid_t fieldId, Field::FieldTag fieldTag);
    
    IE_NAMESPACE(document)::Section* createSection(
            IE_NAMESPACE(document)::IndexTokenizeField *field,
            uint32_t tokenNum, section_weight_t sectionWeight);
    
    section_len_t getMaxSectionLenght() const { return _maxSectionLen; }

    void setMaxSectionLen(section_len_t maxSectionLen);
    void setMaxSectionPerField(uint32_t maxSectionPerField);

public:
    // for test
    const IndexDocumentPtr &getIndexDocument() const { return _indexDocument; }
    const SummaryDocumentPtr &getSummaryDoc() const { return _summaryDocument; }
    const AttributeDocumentPtr &getAttributeDoc() const { return _attributeDocument; }
    const SerializedSummaryDocumentPtr &getSerSummaryDoc() const { return _serSummaryDoc; }
    const KVIndexDocumentPtr &getKVIndexDocument() const { return _kvIndexDocument; }

    void clear();
public:
    PoolType* getPool() const { return _pool.get(); }
    const PoolTypePtr& getPoolPtr() const { return _pool; }
    void serializeSummaryDocument(const config::SummarySchemaPtr& summarySchema);
    
private:
    section_len_t _maxSectionLen;
    uint32_t _maxSectionPerField;
    IndexDocumentPtr _indexDocument;
    KVIndexDocumentPtr _kvIndexDocument;
    SummaryDocumentPtr _summaryDocument;
    AttributeDocumentPtr _attributeDocument;
    SerializedSummaryDocumentPtr _serSummaryDoc;
    PoolTypePtr _pool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ClassifiedDocument);
IE_NAMESPACE_END(document);

#endif //__INDEXLIB_CLASSIFIED_DOCUMENT_H

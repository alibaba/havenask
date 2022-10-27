#ifndef __INDEXLIB_DOCUMENT_EXTEND_DOC_FIELDS_CONVERTOR_H
#define __INDEXLIB_DOCUMENT_EXTEND_DOC_FIELDS_CONVERTOR_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/spatial/spatial_field_encoder.h"
#include "indexlib/common/field_format/date/date_field_encoder.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"

IE_NAMESPACE_BEGIN(document);

class ExtendDocFieldsConvertor
{
public:
    const static uint32_t MAX_TOKEN_PER_SECTION;
public:
    ExtendDocFieldsConvertor(
            const config::IndexPartitionSchemaPtr &schema,
            regionid_t regionId = DEFAULT_REGIONID);
    ~ExtendDocFieldsConvertor();
private:
    ExtendDocFieldsConvertor(const ExtendDocFieldsConvertor &);
    ExtendDocFieldsConvertor& operator=(const ExtendDocFieldsConvertor &);

private:
    typedef std::vector<util::KeyHasher*> HasherVector;
    typedef common::AttributeConvertorPtr AttributeConvertorPtr;
    typedef std::vector<AttributeConvertorPtr> AttributeConvertorVector;

public:
    void convertIndexField(const IndexlibExtendDocumentPtr &document,
                           const config::FieldConfigPtr &fieldConfig);
    void convertAttributeField(const IndexlibExtendDocumentPtr &document,
                               const config::FieldConfigPtr &fieldConfig);
    void convertSummaryField(const IndexlibExtendDocumentPtr &document,
                             const config::FieldConfigPtr &fieldConfig);

private:
    void transTokenizeFieldToField(const TokenizeFieldPtr& tokenizeField,
                                   IndexTokenizeField *field, fieldid_t fieldId,
                                   const ClassifiedDocumentPtr &classifiedDoc);
private:
    void init();
    void initAttrConvert();
    void initFieldTokenHasherVector();
    std::string transTokenizeFieldToSummaryStr(const TokenizeFieldPtr &tokenizeField);
    bool addSection(IndexTokenizeField *field, TokenizeSection *tokenizeSection,
                    autil::mem_pool::Pool *pool, fieldid_t fieldId,
                    const ClassifiedDocumentPtr &classifiedDoc,
                    pos_t &lastTokenPos, pos_t &curPos);
    bool addToken(Section *indexSection, const AnalyzerToken *token,
                  autil::mem_pool::Pool *pool,
                  fieldid_t fieldId, pos_t &lastTokenPos, pos_t &curPos);
    bool addHashToken(Section *indexSection,
                      uint64_t hashKey, autil::mem_pool::Pool *pool,
                      fieldid_t fieldId, pospayload_t posPayload,
                      pos_t &lastTokenPos, pos_t &curPos);

    template<typename EncoderPtr>
    void addSectionTokens(const EncoderPtr& encoder,
                          IndexTokenizeField *field,
                          TokenizeSection *tokenizeSection,
                          autil::mem_pool::Pool *pool, fieldid_t fieldId,
                          const ClassifiedDocumentPtr &classifiedDoc);

private:
    AttributeConvertorVector _attrConvertVec;
    HasherVector _fieldTokenHasherVec;
    config::IndexPartitionSchemaPtr _schema;
    regionid_t _regionId;
    common::SpatialFieldEncoderPtr _spatialFieldEncoder;
    common::DateFieldEncoderPtr _dateFieldEncoder;
    common::RangeFieldEncoderPtr _rangeFieldEncoder;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExtendDocFieldsConvertor);

template<typename EncoderPtr>
void ExtendDocFieldsConvertor::addSectionTokens(
        const EncoderPtr& encoder, IndexTokenizeField *field,
        TokenizeSection *tokenizeSection, autil::mem_pool::Pool *pool,
        fieldid_t fieldId, const ClassifiedDocumentPtr &classifiedDoc)
{
    if (tokenizeSection->getTokenCount() != 1)
    {
        ERROR_COLLECTOR_LOG(DEBUG, "field [%s] content illegal, dropped",
                            _schema->GetFieldSchema()->GetFieldConfig(fieldId)->GetFieldName().c_str());
        return;
    }

    TokenizeSection::Iterator it = tokenizeSection->createIterator();
    const std::string& tokenStr = (*it)->getNormalizedText();
    std::vector<dictkey_t> dictKeys;
    encoder->Encode(fieldId, tokenStr, dictKeys);

    uint32_t leftTokenCount = dictKeys.size();
    Section *indexSection = classifiedDoc->createSection(field, leftTokenCount, 0);
    if (indexSection == NULL) {
        IE_LOG(DEBUG, "Failed to create new section.");
        return;
    }
    section_len_t nowSectionLen = 0;
    section_len_t maxSectionLen = classifiedDoc->getMaxSectionLenght();
    for (size_t i = 0; i < dictKeys.size(); i++)
    {
        if (nowSectionLen + 1 >= maxSectionLen) {
            indexSection->SetLength(nowSectionLen + 1);
            nowSectionLen = 0;
            indexSection = classifiedDoc->createSection(
                    field, leftTokenCount, 0);
            if (indexSection == NULL) {
                IE_LOG(DEBUG, "Failed to create new section.");
                return;
            }
        }
        pos_t curPosition = 0;
        pos_t lastPosition = 0;
        addHashToken(indexSection, dictKeys[i],
                     pool, fieldId, 0, lastPosition, curPosition);
        nowSectionLen++;
        leftTokenCount--;
    }
}

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_EXTEND_DOC_FIELDS_CONVERTOR_H

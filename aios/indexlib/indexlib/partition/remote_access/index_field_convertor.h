#ifndef __INDEXLIB_PARTITION_INDEXFIELDCONVERTOR_H
#define __INDEXLIB_PARTITION_INDEXFIELDCONVERTOR_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(document, IndexTokenizeField);
DECLARE_REFERENCE_CLASS(document, Section);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(common, SpatialFieldEncoder);
DECLARE_REFERENCE_CLASS(common, DateFieldEncoder);
DECLARE_REFERENCE_CLASS(common, RangeFieldEncoder);
DECLARE_REFERENCE_CLASS(util, KeyHasher);

IE_NAMESPACE_BEGIN(partition);

class IndexFieldConvertor
{
public:
    class TokenizeSection
    {
    public:
        typedef std::vector<std::string> TokenVector;
        typedef std::vector<std::string>::iterator Iterator;

    public:
        TokenizeSection() {}
        ~TokenizeSection() {}

    public:
        Iterator Begin() { return mTokens.begin(); } 
        Iterator End() { return mTokens.end(); }

        void SetTokenVector(const TokenVector& tokenVec) { mTokens = tokenVec; }
        size_t GetTokenCount() const { return mTokens.size(); }
    private:
        TokenVector mTokens;
    };
    DEFINE_SHARED_PTR(TokenizeSection);

public:
    IndexFieldConvertor(const config::IndexPartitionSchemaPtr &schema,
                        regionid_t regionId = DEFAULT_REGIONID);
    ~IndexFieldConvertor();

public:
    void convertIndexField(const document::IndexDocumentPtr &indexDoc,
                           const config::FieldConfigPtr &fieldConfig,
                           const std::string& fieldStr, const std::string& fieldSep,
                           autil::mem_pool::Pool* pool);

    static TokenizeSectionPtr ParseSection(const std::string& sectionStr,
            const std::string& sep);

private:
    void init();
    void initFieldTokenHasherVector();
    
    bool addSection(document::IndexTokenizeField *field,
                    const std::string& fieldValue, const std::string& fieldSep, 
                    autil::mem_pool::Pool *pool, fieldid_t fieldId,
                    pos_t &lastTokenPos, pos_t &curPos);
    
    bool addToken(document::Section *indexSection, const std::string& token,
                  autil::mem_pool::Pool *pool, fieldid_t fieldId, 
                  pos_t &lastTokenPos, pos_t &curPos);
    
    void addSpatialSection(fieldid_t fieldId,
                           const std::string& fieldValue,
                           document::IndexTokenizeField* field);
    
    void addDateSection(fieldid_t fieldId,
                        const std::string& fieldValue,
                        document::IndexTokenizeField* field);
    
    bool addHashToken(document::Section *indexSection, dictkey_t hashKey,
                      pos_t &lastTokenPos, pos_t &curPos);
    
    void addSection(const std::vector<dictkey_t>& dictKeys,
                    document::IndexTokenizeField* field);
    
    void addRangeSection(fieldid_t fieldId, const std::string& fieldValue,
                         document::IndexTokenizeField* field);

private:
    typedef std::vector<IE_NAMESPACE(util)::KeyHasher*> HasherVector;

private:
    config::IndexPartitionSchemaPtr mSchema;
    common::SpatialFieldEncoderPtr mSpatialFieldEncoder;
    common::DateFieldEncoderPtr mDateFieldEncoder;
    common::RangeFieldEncoderPtr mRangeFieldEncoder;
    regionid_t mRegionId;
    HasherVector mFieldTokenHasherVec;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(IndexFieldConvertor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_INDEXFIELDCONVERTOR_H

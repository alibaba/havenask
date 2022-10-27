#ifndef __INDEXLIB_INDEX_DOCUMENT_H
#define __INDEXLIB_INDEX_DOCUMENT_H

#include <tr1/memory>
#include <autil/DataBuffer.h>
#include <autil/ConstString.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/index_raw_field.h"

IE_NAMESPACE_BEGIN(document);

class IndexDocument
{
public:
    typedef std::vector<Field* > FieldVector;
    typedef std::vector<autil::ConstString> SectionAttributeVector;
    
public:
    IndexDocument(autil::mem_pool::Pool* pool);
    ~IndexDocument();

public:
    class Iterator
    {
    public:
        Iterator(const IndexDocument& doc) : mDocument(doc)
        {
            mCursor = doc.mFields.begin();
            MoveToNext();
        }

    public:
        bool HasNext() const 
        {
            return mCursor != mDocument.mFields.end(); 
        }
        
        Field* Next()
        {
            if (mCursor != mDocument.mFields.end())
            {
                Field* field = *mCursor;
                ++mCursor;
                MoveToNext();
                return field;
            }
            return NULL;
        }

    private:
        void MoveToNext()
        {
            while(mCursor != mDocument.mFields.end() && 
                  ((*mCursor) == NULL || (*mCursor)->GetFieldId() == INVALID_FIELDID))
            {
                ++mCursor;
            }
        }

    private:
        const IndexDocument& mDocument;
        FieldVector::const_iterator mCursor;
    };

public:
    friend class Iterator;

public:
    //CreateField() and Reset() are for reusing Field object
    Field* CreateField(fieldid_t fieldId,
                       Field::FieldTag fieldTag = Field::FieldTag::TOKEN_FIELD);

    // field should be allocated by mPool
    bool AddField(Field* field);
    Field* GetField(fieldid_t fieldId);
    const Field* GetField(fieldid_t fieldId) const;
    void ClearField(fieldid_t fieldId);

    void SetField(fieldid_t fieldId, Field* field);
    docid_t GetDocId() const { return mDocId; }

    void SetDocId(docid_t docId) { mDocId = docId; }
    uint32_t GetFieldCount() const { return mFieldCount; }

    indexid_t GetMaxIndexIdInSectionAttribute() const
    {
	if (mSectionAttributeVec.empty())
	{
	    return INVALID_INDEXID;
	}
	return (indexid_t)(mSectionAttributeVec.size() - 1);
    }

    void CreateSectionAttribute(indexid_t indexId, const std::string& attrStr);
    
    // attrStr should be allocated by mPool
    void SetSectionAttribute(indexid_t indexId, const autil::ConstString& attrStr);
        
    const autil::ConstString& GetSectionAttribute(indexid_t indexId) const;

    FieldVector::const_iterator GetFieldBegin()
    {
        return mFields.begin();
    }

    FieldVector::const_iterator GetFieldEnd()
    {
        return mFields.end();
    }
    const std::string& GetPrimaryKey() const
    {
        return mPrimaryKey;
    }
    void SetPrimaryKey(const std::string& primaryKey)
    {
        mPrimaryKey = primaryKey;
    }

    void serialize(autil::DataBuffer &dataBuffer) const;
    
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool,
                     uint32_t docVersion = DOCUMENT_BINARY_VERSION);

    //the following four functions are used to get/set global idf for each term
    termpayload_t GetTermPayload(const std::string& termText) const;
    termpayload_t GetTermPayload(uint64_t intKey) const;
    void SetTermPayload(const std::string& termText, termpayload_t payload);
    void SetTermPayload(uint64_t intKey, termpayload_t payload);

    docpayload_t GetDocPayload(const std::string& termText) const;
    docpayload_t GetDocPayload(uint64_t intKey) const;
    void SetDocPayload(uint64_t intKey, docpayload_t docPayload);
    void SetDocPayload(const std::string& termText, docpayload_t docPayload);

    void Reserve(uint32_t fieldNum) { mFields.reserve(fieldNum); }

    Iterator CreateIterator() const { return Iterator(*this); }

    bool operator==(const IndexDocument& doc);
    bool operator!=(const IndexDocument& doc){return !(*this == doc);}

    void ClearData();
    autil::mem_pool::Pool* GetMemPool() { return mPool; }

private:
    static uint8_t GenerateFieldDescriptor(const Field* field)
    {
        if (field == NULL)
        {
            return 0;
        }

        int8_t fieldTagNum = static_cast<int8_t>(field->GetFieldTag());
        return Field::FIELD_DESCRIPTOR_MASK | fieldTagNum;
    }

    static Field::FieldTag GetFieldTagFromFieldDescriptor(
            uint8_t fieldDescriptor)
    {
        return static_cast<Field::FieldTag>(
                (~Field::FIELD_DESCRIPTOR_MASK) & fieldDescriptor);
    }
    
    static Field* CreateFieldByTag(autil::mem_pool::Pool* pool,
                                   Field::FieldTag fieldTag,
                                   bool fieldHasPool)
    {
        auto fieldPool = fieldHasPool ? pool : nullptr;
        if (fieldTag == Field::FieldTag::TOKEN_FIELD)
        {
            return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, fieldPool);
        }
        else if (fieldTag == Field::FieldTag::RAW_FIELD)
        {
            return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexRawField, fieldPool);
        }
        else {
            IE_LOG(ERROR, "invalid fieldTag:[%u]", static_cast<uint16_t>(fieldTag));
            ERROR_COLLECTOR_LOG(ERROR, "invalid fieldTag:[%u]", static_cast<uint16_t>(fieldTag));
            return NULL;
        }
    }
    
    template<typename K, typename V>
    static void SerializeHashMap(autil::DataBuffer &dataBuffer, const util::HashMap<K, V> &hashMap);
    template<typename K, typename V>
    static void DeserializeHashMap(autil::DataBuffer &dataBuffer, util::HashMap<K, V> &hashMap);

    static void SerializeFieldVector(autil::DataBuffer &dataBuffer, const FieldVector& fields);
    static uint32_t DeserializeFieldVector(
            autil::DataBuffer &dataBuffer,
            FieldVector& fields,
            autil::mem_pool::Pool *pool,
            bool isLegacy);

private:
    static const uint32_t HASH_MAP_CHUNK_SIZE = 4096;
    static const uint32_t HASH_MAP_INIT_ELEM_COUNT = 128;
private:
    uint32_t mFieldCount;
    docid_t mDocId;

    FieldVector mFields;
    std::string mPrimaryKey;

    autil::mem_pool::Pool* mPool;

    typedef util::HashMap<uint64_t, docpayload_t> DocPayloadMap;
    typedef util::HashMap<uint64_t, termpayload_t> TermPayloadMap;

    DocPayloadMap mPayloads;
    TermPayloadMap mTermPayloads;
    SectionAttributeVector mSectionAttributeVec;
    
private:
    friend class DocumentTest;
    friend class IndexDocumentTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexDocument);
IE_NAMESPACE_END(document);

#endif //__INDEXLIB_INDEX_DOCUMENT_H

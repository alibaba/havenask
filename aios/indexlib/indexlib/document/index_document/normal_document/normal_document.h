#ifndef __INDEXLIB_NORMAL_DOCUMENT_H
#define __INDEXLIB_NORMAL_DOCUMENT_H

#include <tr1/memory>
#include <autil/DataBuffer.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/document/index_document/kv_document/kv_document.h"
#include "indexlib/document/index_document/kv_document/kv_index_document.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(document);

class NormalDocument : public Document
{
public:
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;
public:
    NormalDocument(PoolPtr pool = PoolPtr(new autil::mem_pool::Pool(1024)));
    ~NormalDocument();

public:
    typedef std::vector<fieldid_t> FieldIdVector;
    typedef std::vector<NormalDocumentPtr> DocumentVector;

public:
    KVDocument* CloneKVDocument(autil::mem_pool::Pool* pool) {
        KVDocument* doc = IE_POOL_NEW_CLASS(pool, KVDocument, pool);
        doc->SetKVIndexDocument(*mKVIndexDocument);
        doc->SetValue(mAttributeDocument->GetPackField(0));
        doc->SetTimestamp(mTimestamp);
        doc->SetUserTimestamp(mUserTimestamp);
        doc->SetOperateType(mOpType);
        return doc;
    }

    void FillKVDocument(const KVDocument& doc)
    {
        if (unlikely(!mKVIndexDocument))
        {
            mKVIndexDocument.reset(new KVIndexDocument(mPool.get()));
        }

        if (unlikely(!mAttributeDocument))
        {
            mAttributeDocument.reset(new AttributeDocument());
        }
        *mKVIndexDocument = doc.GetKVIndexDocument();
        mAttributeDocument->SetPackField(0, doc.GetValue());
        mOriginalOpType = mOpType = doc.GetOperateType();
        mTimestamp = doc.GetTimestamp();
        mUserTimestamp = doc.GetUserTimestamp();
    }
    void ClearKVDocument()
    {
        if (mKVIndexDocument)
        {
            mKVIndexDocument->Reset();            
        }

        if (mAttributeDocument)
        {
            mAttributeDocument->Reset();            
        }
        mOriginalOpType = mOpType = UNKNOWN_OP;
        mTimestamp = INVALID_TIMESTAMP;
        mUserTimestamp = INVALID_TIMESTAMP;
    }

public:
    void SetIndexDocument(const IndexDocumentPtr& indexDocument)
    { mIndexDocument = indexDocument; }

    void SetKVIndexDocument(const KVIndexDocumentPtr& indexDocument)
    { mKVIndexDocument = indexDocument; }

    void SetAttributeDocument(const AttributeDocumentPtr& attributeDocument)
    { mAttributeDocument = attributeDocument; }

    void SetSummaryDocument(const SerializedSummaryDocumentPtr& summaryDocument)
    { mSummaryDocument = summaryDocument; }

    const IndexDocumentPtr& GetIndexDocument() const
    { return mIndexDocument; }

    const KVIndexDocumentPtr& GetKVIndexDocument() const
    { return mKVIndexDocument; }

    const AttributeDocumentPtr& GetAttributeDocument() const
    { return mAttributeDocument; }

    const SerializedSummaryDocumentPtr& GetSummaryDocument() const
    { return mSummaryDocument; }

    void AddSubDocument(NormalDocumentPtr& document)
    { mSubDocuments.push_back(document); }

    void RemoveSubDocument(size_t index)
    {
        if (index < mSubDocuments.size())
        {
            mSubDocuments.erase(mSubDocuments.begin() + index);
        }
    }

    void ClearSubDocuments()
    { mSubDocuments.clear(); }

    const DocumentVector& GetSubDocuments() const
    { return mSubDocuments; }

    docid_t GetDocId() const
    {
        if(mIndexDocument)
        {
            return mIndexDocument->GetDocId();
        }

        if(mAttributeDocument)
        {
            return mAttributeDocument->GetDocId();
        }

        if(mSummaryDocument)
        {
            return mSummaryDocument->GetDocId();
        }

        return INVALID_DOCID;
    }

    void SetDocId(docid_t docId) override
    {
        if(mIndexDocument) mIndexDocument->SetDocId(docId);
        if(mSummaryDocument) mSummaryDocument->SetDocId(docId);
        if(mAttributeDocument) mAttributeDocument->SetDocId(docId);
    }

    const std::string& GetPrimaryKey() const override;

    segmentid_t GetSegmentIdBeforeModified() const
    { return mSegmentIdBeforeModified; }

    void SetSegmentIdBeforeModified(segmentid_t segmentId)
    { mSegmentIdBeforeModified = segmentId; }

    void SetUserTimestamp(int64_t timestamp) { mUserTimestamp = timestamp; }

    int64_t GetUserTimestamp() const { return mUserTimestamp; }

    bool HasPrimaryKey() const
    {
        return mIndexDocument && !mIndexDocument->GetPrimaryKey().empty();
    }

    const FieldIdVector& GetModifiedFields() const { return mModifiedFields; }
    void AddModifiedField(fieldid_t fid) { mModifiedFields.push_back(fid); }
    void ClearModifiedFields() { mModifiedFields.clear(); }

    // store sub modified fields id from main doc modified fields
    void AddSubModifiedField(fieldid_t fid) { mSubModifiedFields.push_back(fid); }
    void SetSubModifiedFields(const FieldIdVector& subModifiedFields)
    { mSubModifiedFields = subModifiedFields; }
    const FieldIdVector& GetSubModifiedFields() const
    { return mSubModifiedFields; }
    void ClearSubModifiedFields() { mSubModifiedFields.clear(); }

    bool operator==(const NormalDocument& doc) const;
    bool operator!=(const NormalDocument& doc) const {return !(*this == doc); }

    autil::mem_pool::Pool *GetPool() const { return mPool.get(); }

    size_t GetMemoryUse() const;

    std::string SerializeToString() const;
    void DeserializeFromString(const std::string &docStr);

    void SetRegionId(regionid_t id) { mRegionId = id; }
    regionid_t GetRegionId() const { return mRegionId; }

    void SetSchemaVersionId(schemavid_t id) { mSchemaId = id; }
    schemavid_t GetSchemaVersionId() const { return mSchemaId; }

protected:
    void DoSerialize(autil::DataBuffer &dataBuffer,
                     uint32_t serializedVersion) const override;

    void DoDeserialize(autil::DataBuffer &dataBuffer,
                       uint32_t serializedVersion) override;
    
public:
    uint32_t GetDocBinaryVersion() const override
    { return DOCUMENT_BINARY_VERSION; }
    void ModifyDocOperateType(DocOperateType op) override;
    bool HasFormatError() const override;

    size_t EstimateMemory() const override;

private:
    template <typename T>
    inline T* deserializeObject(autil::DataBuffer &dataBuffer,
                                autil::mem_pool::Pool* pool,
                                uint32_t docVersion)
    {
        bool hasObj;
        dataBuffer.read(hasObj);
        if (!hasObj)
        {
            return NULL;
        }

        // deserialize may throw exception, use auto ptr
        std::unique_ptr<T> ptr;
        //T* tPtr = IE_POOL_NEW_CLASS(pool, T, pool);
        ptr.reset(new T(pool));
        ptr->deserialize(dataBuffer, pool, docVersion);
        return ptr.release();
    }

    template <typename T>
    inline T* deserializeObject(autil::DataBuffer &dataBuffer,
                                uint32_t docVersion)
    {
        bool hasObj;
        dataBuffer.read(hasObj);
        if (!hasObj)
        {
            return NULL;
        }

        // deserialize may throw exception, use auto ptr
        std::unique_ptr<T> ptr;
        ptr.reset(new T());
        ptr->deserialize(dataBuffer, docVersion);
        return ptr.release();
    }

    void deserializeVersion3(autil::DataBuffer &dataBuffer);
    void deserializeVersion4(autil::DataBuffer &dataBuffer);
    void deserializeVersion5(autil::DataBuffer &dataBuffer);
    void deserializeVersion6(autil::DataBuffer &dataBuffer);
    void deserializeVersion7(autil::DataBuffer &dataBuffer);    

    void serializeVersion3(autil::DataBuffer &dataBuffer) const;
    void serializeVersion4(autil::DataBuffer &dataBuffer) const;
    void serializeVersion5(autil::DataBuffer &dataBuffer) const;
    void serializeVersion6(autil::DataBuffer &dataBuffer) const;
    void serializeVersion7(autil::DataBuffer &dataBuffer) const;    

private:
    IndexDocumentPtr mIndexDocument;
    AttributeDocumentPtr mAttributeDocument;
    SerializedSummaryDocumentPtr mSummaryDocument;

    DocumentVector mSubDocuments;
    FieldIdVector mModifiedFields;
    FieldIdVector mSubModifiedFields;
    int64_t mUserTimestamp;
    regionid_t mRegionId;
    schemavid_t mSchemaId;    
    PoolPtr mPool;

    segmentid_t mSegmentIdBeforeModified;

    KVIndexDocumentPtr mKVIndexDocument;
private:
    friend class DocumentTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalDocument);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_NORMAL_DOCUMENT_H

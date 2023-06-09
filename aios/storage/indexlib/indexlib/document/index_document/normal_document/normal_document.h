/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_NORMAL_DOCUMENT_H
#define __INDEXLIB_NORMAL_DOCUMENT_H

#include <memory>

#include "autil/DataBuffer.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/serialized_source_document.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/document/kv_document/legacy/kv_index_document.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::document {
class NormalDocument;
}

DECLARE_REFERENCE_CLASS(document, NormalDocument);

namespace indexlib { namespace document {

class NormalDocument : public Document
{
public:
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;

public:
    NormalDocument(PoolPtr pool = PoolPtr(new autil::mem_pool::Pool(1024)));
    ~NormalDocument();

public:
    typedef std::vector<fieldid_t> FieldIdVector;
    typedef std::vector<NormalDocumentPtr> DocumentVector;

public:
    void SetIndexDocument(const IndexDocumentPtr& indexDocument) { mIndexDocument = indexDocument; }

    void SetAttributeDocument(const AttributeDocumentPtr& attributeDocument) { mAttributeDocument = attributeDocument; }

    void SetSummaryDocument(const SerializedSummaryDocumentPtr& summaryDocument) { mSummaryDocument = summaryDocument; }

    void SetSourceDocument(const SerializedSourceDocumentPtr& sourceDocument) { mSourceDocument = sourceDocument; }

    const legacy::KVIndexDocumentPtr& GetLegacyKVIndexDocument() const { return mKVIndexDocument; }

    const IndexDocumentPtr& GetIndexDocument() const { return mIndexDocument; }

    const AttributeDocumentPtr& GetAttributeDocument() const { return mAttributeDocument; }

    const SerializedSummaryDocumentPtr& GetSummaryDocument() const { return mSummaryDocument; }

    const SerializedSourceDocumentPtr& GetSourceDocument() const { return mSourceDocument; }

    void AddSubDocument(NormalDocumentPtr& document) { mSubDocuments.push_back(document); }

    void RemoveSubDocument(size_t index);
    bool RemoveSubDocument(const std::string& pk);

    void ClearSubDocuments() { mSubDocuments.clear(); }

    const DocumentVector& GetSubDocuments() const { return mSubDocuments; }

    docid_t GetDocId() const override
    {
        if (mIndexDocument) {
            return mIndexDocument->GetDocId();
        }

        if (mAttributeDocument) {
            return mAttributeDocument->GetDocId();
        }

        if (mSummaryDocument) {
            return mSummaryDocument->GetDocId();
        }

        return INVALID_DOCID;
    }

    void SetDocId(docid_t docId) override
    {
        if (mIndexDocument)
            mIndexDocument->SetDocId(docId);
        if (mSummaryDocument)
            mSummaryDocument->SetDocId(docId);
        if (mAttributeDocument)
            mAttributeDocument->SetDocId(docId);
    }

    const std::string& GetPrimaryKey() const override;

    segmentid_t GetSegmentIdBeforeModified() const { return mSegmentIdBeforeModified; }

    void SetSegmentIdBeforeModified(segmentid_t segmentId) { mSegmentIdBeforeModified = segmentId; }

    void SetUserTimestamp(int64_t timestamp) { mUserTimestamp = timestamp; }

    int64_t GetUserTimestamp() const { return mUserTimestamp; }

    bool HasPrimaryKey() const { return mIndexDocument && !mIndexDocument->GetPrimaryKey().empty(); }

    bool HasIndexUpdate() const;
    const FieldIdVector& GetModifiedFields() const { return mModifiedFields; }
    void AddModifiedField(fieldid_t fid) { mModifiedFields.push_back(fid); }
    void ClearModifiedFields() { mModifiedFields.clear(); }

    const FieldIdVector& GetModifyFailedFields() const { return mModifyFailedFields; }
    void AddModifyFailedField(fieldid_t fid) { mModifyFailedFields.push_back(fid); }

    // store sub modified fields id from main doc modified fields
    void AddSubModifiedField(fieldid_t fid) { mSubModifiedFields.push_back(fid); }
    void SetSubModifiedFields(const FieldIdVector& subModifiedFields) { mSubModifiedFields = subModifiedFields; }
    const FieldIdVector& GetSubModifiedFields() const { return mSubModifiedFields; }
    void ClearSubModifiedFields() { mSubModifiedFields.clear(); }

    bool operator==(const NormalDocument& doc) const;
    bool operator!=(const NormalDocument& doc) const { return !(*this == doc); }

    autil::mem_pool::Pool* GetPool() const { return mPool.get(); }

    size_t GetMemoryUse() const;

    std::string SerializeToString() const;
    void DeserializeFromString(const std::string& docStr);

    void SetRegionId(regionid_t id) { mRegionId = id; }
    regionid_t GetRegionId() const { return mRegionId; }

    void SetSchemaVersionId(schemavid_t id) { mSchemaId = id; }
    schemavid_t GetSchemaVersionId() const { return mSchemaId; }

protected:
    void DoSerialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) const override;

    void DoDeserialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) override;

public:
    uint32_t GetDocBinaryVersion() const override { return LEGACY_DOCUMENT_BINARY_VERSION; }
    void ModifyDocOperateType(DocOperateType op) override;
    bool HasFormatError() const override;

    size_t EstimateMemory() const override;

public:
    std::unique_ptr<indexlibv2::document::NormalDocument> ConstructNormalDocumentV2() const;

private:
    // template <typename T>
    // Status deserializeObject(T* obj, autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool, uint32_t
    // docVersion);

    template <typename T>
    inline T* deserializeObject(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool, uint32_t docVersion)
    {
        bool hasObj;
        dataBuffer.read(hasObj);
        if (!hasObj) {
            return NULL;
        }

        // deserialize may throw exception, use auto ptr
        std::unique_ptr<T> ptr;
        // T* tPtr = IE_POOL_NEW_CLASS(pool, T, pool);
        ptr.reset(new T(pool));
        ptr->deserialize(dataBuffer, pool, docVersion);
        return ptr.release();
    }

    template <typename T>
    inline T* deserializeObject(autil::DataBuffer& dataBuffer, uint32_t docVersion)
    {
        bool hasObj;
        dataBuffer.read(hasObj);
        if (!hasObj) {
            return NULL;
        }

        // deserialize may throw exception, use auto ptr
        std::unique_ptr<T> ptr;
        ptr.reset(new T());
        ptr->deserialize(dataBuffer, docVersion);
        return ptr.release();
    }

    void deserializeVersion3(autil::DataBuffer& dataBuffer);
    void deserializeVersion4(autil::DataBuffer& dataBuffer);
    void deserializeVersion5(autil::DataBuffer& dataBuffer);
    void deserializeVersion6(autil::DataBuffer& dataBuffer);
    void deserializeVersion7(autil::DataBuffer& dataBuffer);
    inline void deserializeVersion8(autil::DataBuffer& dataBuffer)
    {
        assert(mAttributeDocument);
        mAttributeDocument->deserializeVersion8(dataBuffer);
    }
    inline void deserializeVersion9(autil::DataBuffer& dataBuffer)
    {
        mSourceDocument.reset(deserializeObject<SerializedSourceDocument>(dataBuffer, mPool.get(), 9));
    }
    inline void deserializeVersion10(autil::DataBuffer& dataBuffer)
    {
        if (mIndexDocument) {
            mIndexDocument->deserializeVersion10(dataBuffer);
        }
    }

    void serializeVersion3(autil::DataBuffer& dataBuffer) const;
    void serializeVersion4(autil::DataBuffer& dataBuffer) const;
    void serializeVersion5(autil::DataBuffer& dataBuffer) const;
    void serializeVersion6(autil::DataBuffer& dataBuffer) const;
    void serializeVersion7(autil::DataBuffer& dataBuffer) const;
    inline void serializeVersion8(autil::DataBuffer& dataBuffer) const
    {
        assert(mAttributeDocument);
        mAttributeDocument->serializeVersion8(dataBuffer);
    }
    inline void serializeVersion9(autil::DataBuffer& dataBuffer) const { dataBuffer.write(mSourceDocument); }
    inline void serializeVersion10(autil::DataBuffer& dataBuffer) const
    {
        if (mIndexDocument) {
            mIndexDocument->serializeVersion10(dataBuffer);
        }
    }

private:
    IndexDocumentPtr mIndexDocument;
    AttributeDocumentPtr mAttributeDocument;
    SerializedSummaryDocumentPtr mSummaryDocument;
    SerializedSourceDocumentPtr mSourceDocument;

    DocumentVector mSubDocuments;
    FieldIdVector mModifiedFields;
    FieldIdVector mSubModifiedFields;
    int64_t mUserTimestamp;
    regionid_t mRegionId;
    schemavid_t mSchemaId;
    PoolPtr mPool;

    segmentid_t mSegmentIdBeforeModified;
    legacy::KVIndexDocumentPtr mKVIndexDocument; // for serialize/deserialize compatibility, remove in future

    FieldIdVector mModifyFailedFields;

private:
    friend class DocumentTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalDocument);
}} // namespace indexlib::document

#endif //__INDEXLIB_NORMAL_DOCUMENT_H

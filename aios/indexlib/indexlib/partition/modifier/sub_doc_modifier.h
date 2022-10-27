#ifndef __INDEXLIB_SUB_DOC_MODIFIER_H
#define __INDEXLIB_SUB_DOC_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/index_partition_reader.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(index, JoinDocidAttributeReader);

IE_NAMESPACE_BEGIN(partition);

class SubDocModifier : public PartitionModifier
{
public:
    SubDocModifier(const config::IndexPartitionSchemaPtr& schema);
    ~SubDocModifier();
public:
    //for offline & patch modifier
    void Init(const index_base::PartitionDataPtr& partitionData, bool enablePackFile, bool isOffline);
    
    //for online
    void Init(const IndexPartitionReaderPtr& partitionReader);
    bool UpdateDocument(const document::DocumentPtr& doc) override;
    bool DedupDocument(const document::DocumentPtr& doc) override;
    bool RemoveDocument(const document::DocumentPtr& doc) override;

    void Dump(const file_system::DirectoryPtr& directory,
                             segmentid_t segId) override;

    bool IsDirty() const override
    { return mMainModifier->IsDirty() || mSubModifier->IsDirty(); }

    bool UpdateField(docid_t docId, fieldid_t fieldId, 
                                   const autil::ConstString& value) override
    { assert(false); return false; }

    // TODO: implement if needed
    bool TryRewriteAdd2Update(const document::DocumentPtr& doc) override
    { assert(false); return false; }

    bool UpdatePackField(
        docid_t docId, packattrid_t packAttrId, 
        const autil::ConstString& value) override
    { assert(false); return false; }        

    bool UpdateField(const index::AttrFieldValue& value) override;

    bool RemoveDocument(docid_t docId) override;

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const override
    { return mMainModifier->GetPrimaryKeyIndexReader(); }

    const index::PartitionInfoPtr& GetPartitionInfo() const override
    { return mMainModifier->GetPartitionInfo(); }

    void SetDumpThreadNum(uint32_t dumpThreadNum) override
    {
        if (mMainModifier)
        {
            mMainModifier->SetDumpThreadNum(dumpThreadNum);
        }

        if (mSubModifier)
        {
            mSubModifier->SetDumpThreadNum(dumpThreadNum);
        }
    }

public:
    const PartitionModifierPtr& GetMainModifier() const
    { return mMainModifier; }

    const PartitionModifierPtr& GetSubModifier() const
    { return mSubModifier; }

private:
    PartitionModifierPtr CreateSingleModifier(
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::PartitionDataPtr& partitionData,
            bool enablePackFile, bool isOffline);

    PartitionModifierPtr CreateSingleModifier(
            const config::IndexPartitionSchemaPtr& schema,
            const IndexPartitionReaderPtr& reader);
        
    bool NeedUpdate(const document::NormalDocumentPtr& doc,
                    fieldid_t pkIndexField) const;
    bool RemoveMainDocument(const document::NormalDocumentPtr& doc);
    bool RemoveSubDocument(const document::NormalDocumentPtr& doc);

    void RemoveDupSubDocument(const document::NormalDocumentPtr& doc) const;

    bool ValidateSubDocs(const document::NormalDocumentPtr& doc) const;
    void GetSubDocIdRange(docid_t mainDocId, docid_t& subDocIdBegin,
                          docid_t& subDocIdEnd) const;
    docid_t GetDocId(const document::NormalDocumentPtr& doc,
                     const PartitionModifierPtr& modifier) const;
protected:
    PartitionModifierPtr mMainModifier;
    PartitionModifierPtr mSubModifier;
    index::JoinDocidAttributeReaderPtr mMainJoinAttributeReader;
    fieldid_t mMainPkIndexFieldId;

private:
    friend class SubDocModifierTest;
    IE_LOG_DECLARE();
};


DEFINE_SHARED_PTR(SubDocModifier);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUB_DOC_MODIFIER_H

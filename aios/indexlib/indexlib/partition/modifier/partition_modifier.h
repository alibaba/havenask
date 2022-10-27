#ifndef __INDEXLIB_PARTITION_MODIFIER_H
#define __INDEXLIB_PARTITION_MODIFIER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, AttrFieldValue);
DECLARE_REFERENCE_CLASS(index, DefaultAttributeFieldAppender);
DECLARE_REFERENCE_CLASS(index, AttributeDocumentFieldExtractor);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, PartitionInfo);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, AttributeDocumentFieldExtractor);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);

IE_NAMESPACE_BEGIN(partition);

class PartitionModifier
{
public:
    PartitionModifier(const config::IndexPartitionSchemaPtr& schema);
    virtual ~PartitionModifier()
    {
    }

public:
    virtual bool DedupDocument(const document::DocumentPtr& doc) = 0;
    virtual bool UpdateDocument(const document::DocumentPtr& doc) = 0;
    virtual bool RemoveDocument(const document::DocumentPtr& doc) = 0;
    virtual void Dump(
            const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId) = 0;

    virtual bool IsDirty() const = 0;

    virtual bool UpdateField(docid_t docId, fieldid_t fieldId, 
                             const autil::ConstString& value) = 0;

    virtual bool UpdatePackField(docid_t docId, packattrid_t packAttrId, 
                             const autil::ConstString& value) = 0;
    
    virtual bool UpdateField(const index::AttrFieldValue& value);

    virtual bool RemoveDocument(docid_t docId) = 0;
    virtual const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyIndexReader() const = 0;
    virtual const index::PartitionInfoPtr& GetPartitionInfo() const = 0;
    virtual bool TryRewriteAdd2Update(const document::DocumentPtr& doc);
    virtual void SetDumpThreadNum(uint32_t dumpThreadNum) { mDumpThreadNum = dumpThreadNum; }

    bool SupportAutoAdd2Update() const
    {
        return mSupportAutoUpdate;
    }

    const util::BuildResourceMetricsPtr& GetBuildResourceMetrics() const;

private:
    void RewriteAttributeDocument(const document::AttributeDocumentPtr& attrDoc,
                                  autil::mem_pool::Pool* pool);
    
public: // for test
    int64_t GetTotalMemoryUse() const;
    
protected:
    config::IndexPartitionSchemaPtr mSchema;
    index::DefaultAttributeFieldAppenderPtr mDefaultValueAppender;
    bool mSupportAutoUpdate;
    util::BuildResourceMetricsPtr mBuildResourceMetrics;
    document::AttributeDocumentFieldExtractorPtr mAttrFieldExtractor;
    uint32_t mDumpThreadNum;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionModifier);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_MODIFIER_H

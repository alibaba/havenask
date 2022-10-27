#ifndef __INDEXLIB_SUB_DOC_SEGMENT_WRITER_H
#define __INDEXLIB_SUB_DOC_SEGMENT_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_writer.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(partition, SingleSegmentWriter);

IE_NAMESPACE_BEGIN(partition);

class SubDocSegmentWriter : public index_base::SegmentWriter
{
private:
    using index_base::SegmentWriter::Init;

public:
    SubDocSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                        const config::IndexPartitionOptions& options);
    
    ~SubDocSegmentWriter();
public:
    void Init(const index_base::SegmentData& segmentData,
              const index_base::SegmentMetricsPtr& metrics,
              const util::CounterMapPtr& counterMap,
              const plugin::PluginManagerPtr& pluginManager,
              const index_base::PartitionSegmentIteratorPtr& partSegIter =
              index_base::PartitionSegmentIteratorPtr());
    
    SubDocSegmentWriter* CloneWithNewSegmentData(
        index_base::BuildingSegmentData& segmentData) const override;
    void CollectSegmentMetrics() override;
    bool AddDocument(const document::DocumentPtr& doc) override;
    void EndSegment() override;
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
            std::vector<common::DumpItem*>& dumpItems) override;

    index::InMemorySegmentReaderPtr CreateInMemSegmentReader() override;

    bool IsDirty() const override;
    
    //TODO: return main sub doc segment info
    const index_base::SegmentInfoPtr& GetSegmentInfo() const override;

    InMemorySegmentModifierPtr GetInMemorySegmentModifier() override
    { return mMainModifier; }
    
    size_t EstimateInitMemUse(
            const index_base::SegmentMetricsPtr& metrics,
            const util::QuotaControlPtr& buildMemoryQuotaControler,
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr(),
            const index_base::PartitionSegmentIteratorPtr& partSegIter =
            index_base::PartitionSegmentIteratorPtr()) override;
    
    SingleSegmentWriterPtr GetMainWriter() const { return mMainWriter; }
    SingleSegmentWriterPtr GetSubWriter() const { return mSubWriter; }

private:
    SubDocSegmentWriter(const SubDocSegmentWriter& other,
                        index_base::BuildingSegmentData& segmentData);

protected:
    SingleSegmentWriterPtr CreateSingleSegmentWriter(
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options,
            const index_base::SegmentData& segmentData,
            const index_base::SegmentMetricsPtr& metrics,
            const plugin::PluginManagerPtr& pluginManager,
            const index_base::PartitionSegmentIteratorPtr& partSegIter,
            bool isSub);
    
    void AddJoinFieldToDocument(const document::NormalDocumentPtr& doc) const;
    void InitFieldIds(const config::IndexPartitionSchemaPtr& mainSchema);
    void AddJoinFieldToSubDocuments(const document::NormalDocumentPtr& doc) const;
    virtual void UpdateMainDocJoinValue();
    void DedupSubDoc(const document::NormalDocumentPtr& doc) const;
    static common::AttributeConvertorPtr CreateConvertor(
            const std::string& joinFieldName,
            const config::FieldSchemaPtr& fieldSchema);
    bool IsSubDocDuplicated(const document::NormalDocumentPtr& subDoc,
                            const std::set<std::string>& pkSet) const;
    
protected:
    SingleSegmentWriterPtr mMainWriter;
    SingleSegmentWriterPtr mSubWriter;

    fieldid_t mMainJoinFieldId;
    fieldid_t mSubJoinFieldId;
    common::AttributeConvertorPtr mMainJoinFieldConvertor;
    common::AttributeConvertorPtr mSubJoinFieldConvertor;

    InMemorySegmentModifierPtr mMainModifier;

private:
    friend class SubDocSegmentWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocSegmentWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUB_DOC_SEGMENT_WRITER_H

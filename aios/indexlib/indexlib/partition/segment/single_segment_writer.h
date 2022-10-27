#ifndef __INDEXLIB_SINGLE_SEGMENT_WRITER_H
#define __INDEXLIB_SINGLE_SEGMENT_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/segment_data.h"

DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(index, InMemoryIndexSegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index, SegmentMetricsUpdater);
DECLARE_REFERENCE_CLASS(index, InMemoryAttributeSegmentWriter);
DECLARE_REFERENCE_CLASS(index, SummaryWriter);
DECLARE_REFERENCE_CLASS(index, DeletionMapSegmentWriter);
DECLARE_REFERENCE_CLASS(index, DefaultAttributeFieldAppender);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_BEGIN(partition);

class SingleSegmentWriter : public index_base::SegmentWriter
{
public:
    SingleSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options, bool isSub = false);

    ~SingleSegmentWriter();

public:
    void Init(const index_base::SegmentData& segmentData,
              const index_base::SegmentMetricsPtr& metrics,
              const util::BuildResourceMetricsPtr& buildResourceMetrics,
              const util::CounterMapPtr& counterMap,
              const plugin::PluginManagerPtr& pluginManager,
              const index_base::PartitionSegmentIteratorPtr& partSegIter =
              index_base::PartitionSegmentIteratorPtr()) override;

    SingleSegmentWriter* CloneWithNewSegmentData(
        index_base::BuildingSegmentData& segmentData) const override;
    void CollectSegmentMetrics() override;
    size_t EstimateInitMemUse(
            const index_base::SegmentMetricsPtr& metrics,
            const util::QuotaControlPtr& buildMemoryQuotaControler,
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr(),
            const index_base::PartitionSegmentIteratorPtr& partSegIter =
            index_base::PartitionSegmentIteratorPtr()) override;
    
    bool AddDocument(const document::DocumentPtr& doc) override;
    void EndSegment() override;
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
            std::vector<common::DumpItem*>& dumpItems) override;

    index::InMemorySegmentReaderPtr CreateInMemSegmentReader() override;
    bool IsDirty() const override;

    const index_base::SegmentInfoPtr& GetSegmentInfo() const override
    { return mCurrentSegmentInfo; }
    InMemorySegmentModifierPtr GetInMemorySegmentModifier() override
    { return mModifier; }

private:
    bool IsValidDocument(const document::NormalDocumentPtr& doc) const;
    bool CheckAttributeDocument(
        const document::AttributeDocumentPtr& attrDoc) const;
    void CreateIndexDumpItems(const file_system::DirectoryPtr& directory, 
                              std::vector<common::DumpItem*>& dumpItems);
    void CreateAttributeDumpItems(const file_system::DirectoryPtr& directory, 
                                  std::vector<common::DumpItem*>& dumpItems);
    void CreateSummaryDumpItems(const file_system::DirectoryPtr& directory, 
                                std::vector<common::DumpItem*>& dumpItems);
    void CreateDeletionMapDumpItems(const file_system::DirectoryPtr& directory, 
                                    std::vector<common::DumpItem*>& dumpItems);

    InMemorySegmentModifierPtr CreateInMemSegmentModifier() const;
    index::DefaultAttributeFieldAppenderPtr CreateDefaultAttributeFieldAppender(
            const index::InMemorySegmentReaderPtr& inMemSegmentReader);

private:
    bool mIsSub;
    index_base::SegmentData mSegmentData;
    index_base::SegmentInfoPtr mCurrentSegmentInfo;

    index::InMemoryIndexSegmentWriterPtr mIndexWriters;
    index::InMemoryAttributeSegmentWriterPtr mAttributeWriters;
    index::InMemoryAttributeSegmentWriterPtr mVirtualAttributeWriters;
    index::SummaryWriterPtr mSummaryWriter;
    index::DeletionMapSegmentWriterPtr mDeletionMapSegmentWriter;

    index::DefaultAttributeFieldAppenderPtr mDefaultAttrFieldAppender;
    InMemorySegmentModifierPtr mModifier;
    index::SegmentMetricsUpdaterPtr mSegAttrUpdater;

private:
    friend class SingleSegmentWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleSegmentWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SINGLE_SEGMENT_WRITER_H

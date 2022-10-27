#ifndef __INDEXLIB_MULTI_SHARDING_INDEX_WRITER_H
#define __INDEXLIB_MULTI_SHARDING_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/format/sharding_index_hasher.h"

DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
IE_NAMESPACE_BEGIN(index);

class MultiShardingIndexWriter : public index::IndexWriter
{
public:
    MultiShardingIndexWriter(
            const index_base::SegmentMetricsPtr& segmentMetrics,
            const config::IndexPartitionOptions& options);    

    ~MultiShardingIndexWriter();

public:
    void Init(const config::IndexConfigPtr& indexConfig,
              util::BuildResourceMetrics* buildResourceMetrics,
              const index_base::PartitionSegmentIteratorPtr& segIter =
              index_base::PartitionSegmentIteratorPtr()) override;
    
    size_t EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionSegmentIteratorPtr& segIter =
                              index_base::PartitionSegmentIteratorPtr()) override;

    void AddField(const document::Field* field) override;

    void EndDocument(const document::IndexDocument& indexDocument) override;

    void EndSegment() override;

    void Dump(const file_system::DirectoryPtr& dir,
              autil::mem_pool::PoolBase* dumpPool) override;

    uint64_t GetNormalTermDfSum() const override;
    void FillDistinctTermCount(index_base::SegmentMetricsPtr mSegmentMetrics) override;
    index::IndexSegmentReaderPtr CreateInMemReader() override;

    void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                               std::priority_queue<size_t>& minMemUses) const override;

public:  // for test
    std::vector<NormalIndexWriterPtr>& GetShardingIndexWriters()
    {
        return mShardingWriters;
    }
    
    void CreateDumpItems(const file_system::DirectoryPtr& directory, 
                         std::vector<common::DumpItem*>& dumpItems);

private:
    void UpdateBuildResourceMetrics() override {};
    
    void AddToken(const document::Token* token, fieldid_t fieldId, 
                  pos_t tokenBasePos);


    index::SectionAttributeWriterPtr CreateSectionAttributeWriter(
            const config::IndexConfigPtr& indexConfig,
            util::BuildResourceMetrics* buildResourceMetrics);
    uint32_t GetDistinctTermCount() const override;

private:
    pos_t mBasePos;
    std::vector<NormalIndexWriterPtr> mShardingWriters;
    index::SectionAttributeWriterPtr mSectionAttributeWriter;
    ShardingIndexHasherPtr mShardingIndexHasher;
    index_base::SegmentMetricsPtr mSegmentMetrics;
    config::IndexPartitionOptions mOptions;

private:
    friend class IndexReaderTestBase;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiShardingIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_SHARDING_INDEX_WRITER_H

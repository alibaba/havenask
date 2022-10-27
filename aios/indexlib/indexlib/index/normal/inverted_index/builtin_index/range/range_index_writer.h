#ifndef __INDEXLIB_RANGE_INDEX_WRITER_H
#define __INDEXLIB_RANGE_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index, NormalIndexWriter);

IE_NAMESPACE_BEGIN(index);

class RangeInfo;
DEFINE_SHARED_PTR(RangeInfo);

class RangeIndexWriter : public IndexWriter
{
public:
    RangeIndexWriter(const std::string& indexName,
                     index_base::SegmentMetricsPtr segmentMetrics,
                     const config::IndexPartitionOptions& options);
    ~RangeIndexWriter();
public:
    void Init(const config::IndexConfigPtr& indexConfig,
              util::BuildResourceMetrics* buildResourceMetrics,
              const index_base::PartitionSegmentIteratorPtr& segIter =
              index_base::PartitionSegmentIteratorPtr()) override;

    size_t EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionSegmentIteratorPtr& segIter =
                              index_base::PartitionSegmentIteratorPtr()) override;

    void EndDocument(const document::IndexDocument& indexDocument) override;

    void EndSegment() override;
    
    uint64_t GetNormalTermDfSum() const override
    {
        assert(false);
        return 0;
    }
    
    void AddField(const document::Field* field) override;

    void Dump(const file_system::DirectoryPtr& dir,
              autil::mem_pool::PoolBase* dumpPool) override;

    IndexSegmentReaderPtr CreateInMemReader() override;
    void FillDistinctTermCount(index_base::SegmentMetricsPtr mSegmentMetrics) override;

private:
    uint32_t GetDistinctTermCount() const override
    {
        assert(false);
        return 0;
    }
    void UpdateBuildResourceMetrics() override
    {
        assert(false);
    }
        
    void InitMemoryPool() override
    {
    }
    
public:
    pos_t mBasePos;
    RangeInfoPtr mRangeInfo;
    NormalIndexWriterPtr mBottomLevelWriter;
    NormalIndexWriterPtr mHighLevelWriter;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RANGE_INDEX_WRITER_H

#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/util/mmap_allocator.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexWriter);

void IndexWriter::FillDistinctTermCount(SegmentMetricsPtr segmentMetrics)
{
    segmentMetrics->SetDistinctTermCount(mIndexConfig->GetIndexName(), GetDistinctTermCount());
}

void IndexWriter::Init(const IndexConfigPtr& indexConfig,
                       BuildResourceMetrics* buildResourceMetrics,
                       const PartitionSegmentIteratorPtr& segIter)
{
    mIndexConfig = indexConfig;
    InitMemoryPool();
    if (buildResourceMetrics)
    {
        mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
        assert(indexConfig);
        IE_LOG(INFO, "allocate build resource node [id:%d] for index[%s]",
               mBuildResourceMetricsNode->GetNodeId(),
               indexConfig->GetIndexName().c_str());
    }
}


void IndexWriter::InitMemoryPool()
{
    mAllocator.reset(new MMapAllocator);
    mByteSlicePool.reset(new Pool(mAllocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024));        
}


IE_NAMESPACE_END(index);


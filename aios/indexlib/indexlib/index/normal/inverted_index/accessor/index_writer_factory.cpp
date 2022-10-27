#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include <sstream>
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index/normal/primarykey/primary_key_index_writer_typed.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_writer.h"
#include "indexlib/index/normal/trie/trie_index_writer.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil::legacy;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexWriterFactory);

IndexWriterFactory::IndexWriterFactory() 
{
}

IndexWriterFactory::~IndexWriterFactory() 
{
}

size_t IndexWriterFactory::EstimateIndexWriterInitMemUse(
    const IndexConfigPtr& indexConfig,
    const IndexPartitionOptions& options,
    const plugin::PluginManagerPtr& pluginManager,
    index_base::SegmentMetricsPtr segmentMetrics,
    const index_base::PartitionSegmentIteratorPtr& segIter)
{
    size_t initSize = 0;
    IndexWriter* indexWriter = ConstructIndexWriter(
            indexConfig, segmentMetrics, options, NULL, pluginManager); 
    initSize = indexWriter->EstimateInitMemUse(indexConfig, segIter);
    delete indexWriter;
    return initSize;
}

IndexWriter* IndexWriterFactory::ConstructIndexWriter(
        const IndexConfigPtr& indexConfig,
        index_base::SegmentMetricsPtr segmentMetrics,
        const IndexPartitionOptions& options,
        BuildResourceMetrics* buildResourceMetrics,
        const plugin::PluginManagerPtr& pluginManager)
{
    assert(indexConfig);
    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        MultiShardingIndexWriter* writer = new MultiShardingIndexWriter(
                segmentMetrics, options);
        return writer;
    }

    IndexType indexType = indexConfig->GetIndexType();
    string indexName = indexConfig->GetIndexName();
    IndexWriter *indexWriter = NULL;
    size_t lastSegmentDistinctTermCount = 0;
    if (indexType != it_range)
    {
        lastSegmentDistinctTermCount = segmentMetrics->GetDistinctTermCount(indexName);
    }
    switch (indexType)
    {
    case it_string:
    case it_spatial:    
    case it_text:
    case it_pack:
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
    case it_expack:
        indexWriter = new NormalIndexWriter(
                lastSegmentDistinctTermCount, options);
        break;
    case it_primarykey64:
        indexWriter = new UInt64PrimaryKeyIndexWriterTyped();
        break;
    case it_primarykey128:
        indexWriter = new UInt128PrimaryKeyIndexWriterTyped();
        break;
    case it_trie:
        indexWriter = new TrieIndexWriter();
        break;
    case it_customized:
        indexWriter = new CustomizedIndexWriter(
                      pluginManager, lastSegmentDistinctTermCount);
        break;
    case it_date:
        indexWriter = new DateIndexWriter(lastSegmentDistinctTermCount, options);
        break;
    case it_range:
        indexWriter = new RangeIndexWriter(indexName, segmentMetrics, options);
        break;
    default:
    {
        stringstream s;
        s << "Index writer type: " << IndexConfig::IndexTypeToStr(indexType)
          <<" not implemented yet!";
        INDEXLIB_THROW(misc::UnImplementException, "%s", s.str().c_str());        
    }
    break;
    }
    return indexWriter;
}

IndexWriter* IndexWriterFactory::CreateIndexWriter(
        const IndexConfigPtr& indexConfig,
        index_base::SegmentMetricsPtr segmentMetrics,
        const IndexPartitionOptions& options,
        BuildResourceMetrics* buildResourceMetrics,
        const plugin::PluginManagerPtr& pluginManager,
        const index_base::PartitionSegmentIteratorPtr& segIter)
{
    IndexWriter *indexWriter = ConstructIndexWriter(
            indexConfig, segmentMetrics, options,
            buildResourceMetrics, pluginManager);
    indexWriter->Init(indexConfig, buildResourceMetrics, segIter);
    return indexWriter;
}

IE_NAMESPACE_END(index);


#ifndef __INDEXLIB_MULTI_FIELD_INDEX_SEGMENT_READER_H
#define __INDEXLIB_MULTI_FIELD_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_multi_sharding_index_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class MultiFieldIndexSegmentReader : public IndexSegmentReader
{
public:
    typedef std::map<std::string, IndexSegmentReaderPtr> String2IndexReaderMap;

public:
    MultiFieldIndexSegmentReader();
    ~MultiFieldIndexSegmentReader();
public:
    void AddIndexSegmentReader(const std::string& indexName,
                               const IndexSegmentReaderPtr& indexSegmentReader)
    { 
        mIndexReaders.insert(make_pair(indexName, indexSegmentReader));
    }

    void AddMultiShardingSegmentReader(const std::string& indexName,
            const InMemMultiShardingIndexSegmentReaderPtr& indexSegmentReader)
    {
        mIndexReaders.insert(make_pair(indexName, indexSegmentReader));
        typedef InMemMultiShardingIndexSegmentReader::InMemShardingSegReader SegReader;
        const std::vector<SegReader>& shardingReaders = 
            indexSegmentReader->GetShardingIndexSegReaders();
        for (size_t i = 0; i < shardingReaders.size(); ++i)
        {
            mIndexReaders.insert(shardingReaders[i]);
        }
    }

    IndexSegmentReaderPtr GetIndexSegmentReader(const std::string& indexName) const;

private:
    String2IndexReaderMap mIndexReaders;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiFieldIndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_FIELD_INDEX_SEGMENT_READER_H

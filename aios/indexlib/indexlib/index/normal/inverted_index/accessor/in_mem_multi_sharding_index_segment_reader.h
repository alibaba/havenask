#ifndef __INDEXLIB_IN_MEM_MULTI_SHARDING_INDEX_SEGMENT_READER_H
#define __INDEXLIB_IN_MEM_MULTI_SHARDING_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class InMemMultiShardingIndexSegmentReader : public IndexSegmentReader
{
public:
    typedef std::pair<std::string, IndexSegmentReaderPtr> InMemShardingSegReader;

public:
    InMemMultiShardingIndexSegmentReader(
            const std::vector<InMemShardingSegReader>& segReaders,
            const index::AttributeSegmentReaderPtr& sectionSegmentReader)
        : mShardingIndexSegReaders(segReaders)
        , mSectionSegmentReader(sectionSegmentReader)
    {}

    ~InMemMultiShardingIndexSegmentReader() {}

public:
    
    index::AttributeSegmentReaderPtr GetSectionAttributeSegmentReader() const override
    { return mSectionSegmentReader; }

    
    index::InMemBitmapIndexSegmentReaderPtr GetBitmapSegmentReader() const override
    { assert(false); return index::InMemBitmapIndexSegmentReaderPtr(); }

    
    bool GetSegmentPosting(dictkey_t key, docid_t baseDocId,
                           index::SegmentPosting &segPosting,
                           autil::mem_pool::Pool* sessionPool) const override
    {
        assert(false);
        return false;
    }

public:
    const std::vector<InMemShardingSegReader>& GetShardingIndexSegReaders() const
    {  return mShardingIndexSegReaders; }

private:
    std::vector<InMemShardingSegReader> mShardingIndexSegReaders;
    index::AttributeSegmentReaderPtr mSectionSegmentReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemMultiShardingIndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_MULTI_SHARDING_INDEX_SEGMENT_READER_H

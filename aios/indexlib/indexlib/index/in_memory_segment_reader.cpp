#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/summary/summary_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_segment_reader.h"

using namespace std;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemorySegmentReader);

InMemorySegmentReader::InMemorySegmentReader(segmentid_t segId)
    : mSegmentId(segId)
{

}

void InMemorySegmentReader::Init(const MultiFieldIndexSegmentReaderPtr& indexSegmentReader,
                                 const String2AttrReaderMap& attrReaders,
                                 const SummarySegmentReaderPtr& summaryReader,
                                 const InMemDeletionMapReaderPtr& delMap,
                                 const SegmentInfo& segmentInfo)
{
    mIndexSegmentReader = indexSegmentReader;
    mAttrReaders = attrReaders;
    mSummaryReader = summaryReader;
    mInMemDeletionMapReader = delMap;
    mSegmentInfo = segmentInfo;

}

InMemorySegmentReader::~InMemorySegmentReader()
{

}

IndexSegmentReaderPtr InMemorySegmentReader::GetSingleIndexSegmentReader(
    const std::string& name
    ) const
{
    return mIndexSegmentReader->GetIndexSegmentReader(name);

}

AttributeSegmentReaderPtr InMemorySegmentReader::GetAttributeSegmentReader(const string& name)
{
    String2AttrReaderMap::const_iterator it = mAttrReaders.find(name);
    if (it != mAttrReaders.end())
    {
        return it->second;

    }
    return AttributeSegmentReaderPtr();

}

SummarySegmentReaderPtr InMemorySegmentReader::GetSummaryReader() const
{
    return mSummaryReader;

}

InMemDeletionMapReaderPtr InMemorySegmentReader::GetInMemDeletionMapReader() const
{
    return mInMemDeletionMapReader;

}

IE_NAMESPACE_END(index);

#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_segment_reader.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiFieldIndexSegmentReader);

MultiFieldIndexSegmentReader::MultiFieldIndexSegmentReader() 
{
}

MultiFieldIndexSegmentReader::~MultiFieldIndexSegmentReader() 
{
}

IndexSegmentReaderPtr MultiFieldIndexSegmentReader::GetIndexSegmentReader(
        const string& indexName) const
{
    String2IndexReaderMap::const_iterator it = mIndexReaders.find(indexName);
    if (it != mIndexReaders.end())
    {
        return it->second;
    }
    return IndexSegmentReaderPtr();
}

IE_NAMESPACE_END(index);


#include "indexlib/index/normal/inverted_index/format/skiplist/skip_list_reader.h"

IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SkipListReader);

SkipListReader::SkipListReader()
   : mStart(0)
   , mEnd(0)
   , mSkippedItemCount(-1)
{
}

SkipListReader::SkipListReader(const SkipListReader& other)
    : mStart(other.mStart)
    , mEnd(other.mEnd)
    , mByteSliceReader(other.mByteSliceReader)
    , mSkippedItemCount(other.mSkippedItemCount)

{
}

SkipListReader::~SkipListReader() 
{
}

void SkipListReader::Load(const util::ByteSliceList* byteSliceList,
                          uint32_t start, uint32_t end)
{
    //TODO: throw exception
    assert(start <= byteSliceList->GetTotalSize());
    assert(end <= byteSliceList->GetTotalSize());
    assert(start <= end);
    mStart = start;
    mEnd = end;    
    mByteSliceReader.Open(const_cast<util::ByteSliceList*>(byteSliceList));
    mByteSliceReader.Seek(start);
}
IE_NAMESPACE_END(index);


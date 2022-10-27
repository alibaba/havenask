#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice_reader.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedByteSliceReader);

BufferedByteSliceReader::BufferedByteSliceReader() 
{
    mLocationCursor = 0;
    mShortBufferCursor = 0;
    mBufferedByteSlice = NULL;
    mMultiValue = NULL;
}

BufferedByteSliceReader::~BufferedByteSliceReader() 
{
    
}

void BufferedByteSliceReader::Open(const BufferedByteSlice* bufferedByteSlice)
{
    mLocationCursor = 0;
    mShortBufferCursor = 0;

    mByteSliceReader.Open(const_cast<util::ByteSliceList*>(
                    bufferedByteSlice->GetByteSliceList()));
    mBufferedByteSlice = bufferedByteSlice;
    mMultiValue = bufferedByteSlice->GetMultiValue();
}

IE_NAMESPACE_END(index);


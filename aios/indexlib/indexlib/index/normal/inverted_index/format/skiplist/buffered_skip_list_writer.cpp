#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"
#include "indexlib/util/byte_slice_list/byte_slice_list_iterator.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedSkipListWriter);

BufferedSkipListWriter::BufferedSkipListWriter(
        autil::mem_pool::Pool* byteSlicePool,
        autil::mem_pool::RecyclePool* bufferPool,
        CompressMode compressMode)
    : BufferedByteSlice(byteSlicePool, bufferPool)
    , mLastKey(0)
    , mLastValue1(0)
{
    if (compressMode == REFERENCE_COMPRESS_MODE)
    {
        mLastKey = INVALID_LAST_KEY;
    }
}

BufferedSkipListWriter::~BufferedSkipListWriter() 
{
}

void BufferedSkipListWriter::AddItem(uint32_t key, uint32_t value1, 
                                     uint32_t value2)
{
    assert(GetMultiValue()->GetAtomicValueSize() == 3);

    PushBack(0, key - mLastKey);
    PushBack(1, value1 - mLastValue1);
    mLastKey = key;
    mLastValue1 = value1;
    PushBack(2, value2);
    EndPushBack();

    if (NeedFlush(SKIP_LIST_BUFFER_SIZE))
    {
        Flush(PFOR_DELTA_COMPRESS_MODE);        
    }
}

void BufferedSkipListWriter::AddItem(uint32_t key, uint32_t value1)
{
    assert(GetMultiValue()->GetAtomicValueSize() == 2);

    PushBack(0, IsReferenceCompress() ? key : key - mLastKey);
    PushBack(1, value1);
    EndPushBack();

    if (!IsReferenceCompress())
    {
        mLastKey = key;
    }

    if (NeedFlush(SKIP_LIST_BUFFER_SIZE))
    {
        Flush(IsReferenceCompress() ? 
              REFERENCE_COMPRESS_MODE : PFOR_DELTA_COMPRESS_MODE);
    }
}

void BufferedSkipListWriter::AddItem(uint32_t valueDelta)
{
    mLastValue1 += valueDelta;
    PushBack(0, mLastValue1);
    EndPushBack();

    if (NeedFlush(SKIP_LIST_BUFFER_SIZE))
    {
        Flush(SHORT_LIST_COMPRESS_MODE);
    }
}

size_t BufferedSkipListWriter::FinishFlush()
{
    uint32_t skipListSize = GetTotalCount();
    uint8_t skipListCompressMode = 
        ShortListOptimizeUtil::GetSkipListCompressMode(skipListSize);

    if (IsReferenceCompress() && skipListCompressMode == PFOR_DELTA_COMPRESS_MODE)
    {
        skipListCompressMode = REFERENCE_COMPRESS_MODE;
    }
    return Flush(skipListCompressMode);
}

size_t BufferedSkipListWriter::DoFlush(uint8_t compressMode)
{
    assert(GetMultiValue()->GetAtomicValueSize() <= 3);
    assert(compressMode == PFOR_DELTA_COMPRESS_MODE
           || compressMode == SHORT_LIST_COMPRESS_MODE
           || compressMode == REFERENCE_COMPRESS_MODE);
    
    uint32_t flushSize = 0;
    uint8_t size = mBuffer.Size();
    
    const MultiValue* multiValue = GetMultiValue();
    const AtomicValueVector& atomicValues = multiValue->GetAtomicValues();
    for (size_t i = 0; i < atomicValues.size(); ++i)
    {
        AtomicValue* atomicValue = atomicValues[i];
        uint8_t* buffer = mBuffer.GetRow(atomicValue->GetLocation());
        flushSize += atomicValue->Encode(compressMode, mPostingListWriter, buffer, 
                size * atomicValue->GetSize());
    }
    return flushSize;
}

void BufferedSkipListWriter::Dump(const file_system::FileWriterPtr& file)
{
    uint32_t skipListSize = GetTotalCount();
    if (skipListSize == 0)
    {
        return;
    }
    uint8_t compressMode = ShortListOptimizeUtil::GetSkipListCompressMode(
            skipListSize);

    if (compressMode != SHORT_LIST_COMPRESS_MODE
        || GetMultiValue()->GetAtomicValueSize() != 3)
    {
        mPostingListWriter.Dump(file);
        return;
    }

    const ByteSliceList* skipList = mPostingListWriter.GetByteSliceList();
    ByteSliceListIterator iter(skipList);
    const MultiValue* multiValue = GetMultiValue();
    const AtomicValueVector& atomicValues = multiValue->GetAtomicValues();
    uint32_t start = 0;
    for (size_t i = 0; i < atomicValues.size(); ++i)
    {
        uint32_t len = atomicValues[i]->GetSize() * skipListSize;
        if (i > 0)
        {
            // not encode last value after first row
            len -= atomicValues[i]->GetSize();
        }
        if (len == 0)
        { 
            break;
        }
        bool ret = iter.SeekSlice(start);
        assert(ret);
        (void)ret;
        while (iter.HasNext(start + len))
        {
            void* data = NULL;
            size_t size = 0;
            iter.Next(data, size);
            assert(data);
            assert(size);
            file->Write(data, size);
        }
        start += atomicValues[i]->GetSize() * skipListSize;
    }
}

size_t BufferedSkipListWriter::EstimateDumpSize() const
{ 
    uint32_t skipListSize = GetTotalCount();
    if (skipListSize == 0)
    {
        return 0;
    }
    uint8_t compressMode = ShortListOptimizeUtil::GetSkipListCompressMode(
            skipListSize);

    if (compressMode != SHORT_LIST_COMPRESS_MODE
        || GetMultiValue()->GetAtomicValueSize() != 3)
    {
        return mPostingListWriter.GetSize();
    }

    const MultiValue* multiValue = GetMultiValue();
    const AtomicValueVector& atomicValues = multiValue->GetAtomicValues();
    assert(atomicValues.size() == 3);
    
    size_t optSize = atomicValues[1]->GetSize() + atomicValues[2]->GetSize();
    return mPostingListWriter.GetSize() - optSize;
}

IE_NAMESPACE_END(index);


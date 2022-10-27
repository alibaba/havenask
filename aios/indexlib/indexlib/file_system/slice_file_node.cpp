#include "indexlib/misc/exception.h"
#include "indexlib/file_system/slice_file_node.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SliceFileNode);

SliceFileNode::SliceFileNode(uint64_t sliceLen, int32_t sliceNum,
                             const BlockMemoryQuotaControllerPtr& memController)
    : mBytesAlignedSliceArray(
            new BytesAlignedSliceArray(SimpleMemoryQuotaControllerPtr(
                            new SimpleMemoryQuotaController(memController)),
                    sliceLen, sliceNum))
    , mSliceNum(sliceNum)
{
}

SliceFileNode::~SliceFileNode() 
{
    Close();
}

void SliceFileNode::DoOpen(const string& path, FSOpenType openType)
{
    assert(openType == FSOT_SLICE);
}

void SliceFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "Not support DoOpen with PackageOpenMeta");
}

void SliceFileNode::DoOpen(const string& path, const fslib::FileMeta& mountMeta,
                           FSOpenType openType)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "Not support DoOpen with FileMeta");
}

FSFileType SliceFileNode::GetType() const
{
    return FSFT_SLICE;
}

size_t SliceFileNode::GetLength() const
{
    assert(mBytesAlignedSliceArray);
    return mBytesAlignedSliceArray->GetTotalUsedBytes();
}

void* SliceFileNode::GetBaseAddress() const
{
    assert(mBytesAlignedSliceArray);
    if (mSliceNum > 1)
    {
        IE_LOG(ERROR, "Multi slice not support GetBaseAddress");
        return NULL;
    }
    return mBytesAlignedSliceArray->GetValueAddress(0);
}

size_t SliceFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    if (mSliceNum > 1)
    {
        IE_LOG(ERROR, "Multi slice not support Read");
        return 0;
    }
    if (offset + length > GetSliceFileLength())
    {
        return 0;
    }
    char* base = (char*)GetBaseAddress();
    
    memcpy(buffer, (void *)(base + offset), length);
    return length;
}

ByteSliceList* SliceFileNode::Read(size_t length, size_t offset, ReadOption option)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "Not support GetBaseAddress");
    return NULL;
}

size_t SliceFileNode::Write(const void* buffer, size_t length)
{
    assert(mBytesAlignedSliceArray);
    if (mSliceNum > 1)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, 
                             "Multi slice not support Write");
    }

    mBytesAlignedSliceArray->Insert(buffer, length);
    return length;
}

void SliceFileNode::Close()
{
}

IE_NAMESPACE_END(file_system);


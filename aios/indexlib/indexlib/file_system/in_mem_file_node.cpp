#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/mmap_pool.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/common_file_wrapper.h"
#include <fslib/fs/File.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemFileNode);

InMemFileNode::InMemFileNode(size_t reservedSize, bool readFromDisk,
                             const LoadConfig& loadConfig,
                             const util::BlockMemoryQuotaControllerPtr& memController,
                             const SessionFileCachePtr& fileCache) 
    : mData(NULL)
    , mLength(0)
    , mMemPeak(0)
    , mCapacity(reservedSize)
    , mReadFromDisk(readFromDisk)
    , mPool(new MmapPool)
    , mDataFileOffset(0)
    , mDataFileLength(-1)
    , mPopulated(false)
    , mFileCache(fileCache)
{
    mMemController.reset(new util::SimpleMemoryQuotaController(memController));
    auto mmapLoadStrategy = DYNAMIC_POINTER_CAST(MmapLoadStrategy, loadConfig.GetLoadStrategy());
    if (mmapLoadStrategy)
    {
        mLoadStrategy.reset(mmapLoadStrategy->CreateInMemLoadStrategy());
    }
}

InMemFileNode::InMemFileNode(const InMemFileNode& other)
    : FileNode(other)
    , mData(NULL)
    , mLength(other.mLength)
    , mMemPeak(0)
    , mCapacity(other.mLength)
    , mReadFromDisk(other.mReadFromDisk)
    , mPool(new MmapPool)
    , mPopulated(other.mPopulated)
{
    mMemController.reset(new util::SimpleMemoryQuotaController(
                    other.mMemController->GetBlockMemoryController()));
    if (mLength > 0 && other.mData)
    {
        mData = (uint8_t*)mPool->allocate(mLength);
        memcpy(mData, other.mData, mLength);
        AllocateMemQuota(mLength);
    }
}

InMemFileNode::~InMemFileNode() 
{
    Close();
    assert(mPool);
    delete mPool;
    mPool = NULL;
}

void InMemFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
{
    assert(mPool);
    assert(mData == NULL);

    if (mReadFromDisk)
    {
        mDataFilePath = packageOpenMeta.GetPhysicalFilePath();
        mLength = packageOpenMeta.GetLength();
        mDataFileOffset = packageOpenMeta.GetOffset();
        mDataFileLength = packageOpenMeta.GetPhysicalFileLength();
    }
    else
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "InMemFileNode of inner file only supports "
                             "read from disk.");        
    }
}

void InMemFileNode::DoOpen(const string& path, const FileMeta& fileMeta,
                           FSOpenType openType)
{
    assert(mPool);
    assert(mData == NULL);

    if (mReadFromDisk)
    {
        mDataFilePath = path;
        mLength = fileMeta.fileLength;
        mDataFileOffset = 0;
        mDataFileLength = fileMeta.fileLength;
    }
    else
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "InMemFileNode of solid file only supports "
                             "read from disk.");        
    }
}

void InMemFileNode::DoOpen(const string& path, FSOpenType openType)
{
    assert(mPool);
    assert(mData == NULL);

    if (mReadFromDisk)
    {
        mDataFilePath = path;
        mLength = FileSystemWrapper::GetFileLength(path);
        mDataFileOffset = 0;
        mDataFileLength = mLength;
    }
    else
    {
        if (mCapacity > 0)
        {
            mData = (uint8_t*)mPool->allocate(mCapacity);
        }
    }
}

void InMemFileNode::Populate()
{
    ScopedLock lock(mLock);
    if (mPopulated)
    {
        return;
    }

    if (mReadFromDisk)
    {
        DoOpenFromDisk(mDataFilePath, mDataFileOffset, mLength, mDataFileLength);
    }

    mPopulated = true;
}

void InMemFileNode::DoOpenFromDisk(
    const string& path, size_t offset, size_t length, ssize_t fileLength)
{
    assert(mPool);
    assert(mData == NULL);

    File* file = NULL;
    FileWrapperPtr fileWrapper;
    if (mInPackage && mFileCache)
    {
        file = mFileCache->Get(path, fileLength);
        fileWrapper.reset(new CommonFileWrapper(file, false, false));
    }
    else
    {
        fileWrapper.reset(FileSystemWrapper::OpenFile(path, READ, false, fileLength));
    }

    // if length <= 0, mmap will return MAP_FAILED(-1)
    // and set errno = 12, means Cannot allocate memory.
    try
    {
        if (length > 0)
        {
            LoadData(fileWrapper, offset, length);
        }
        mCapacity = length;
        if (mInPackage && mFileCache)
        {
            mFileCache->Put(file, path);
        }
        fileWrapper->Close();        
        mLength = length;
        AllocateMemQuota(mLength);
    }
    catch (...)
    {
        if (mInPackage && mFileCache)
        {
            mFileCache->Put(file, path);
        }
        fileWrapper->Close();        
        throw;
    }
}

void InMemFileNode::LoadData(const FileWrapperPtr& file, int64_t offset, int64_t length)
{
    mData = (uint8_t*) mPool->allocate(length);
    if (!mLoadStrategy || mLoadStrategy->GetInterval() == 0)
    {
        file->PRead(mData, length, offset);
    }
    else
    {
        for (int64_t offset = 0; offset < length; offset += mLoadStrategy->GetSlice())
        {
            int64_t readLen = min(length - offset, (int64_t)mLoadStrategy->GetSlice());
            file->PRead(mData + offset, readLen, offset);
            usleep(mLoadStrategy->GetInterval() * 1000);
        }
    }
 }

FSFileType InMemFileNode::GetType() const
{
    return FSFT_IN_MEM;
}

size_t InMemFileNode::GetLength() const
{
    return mLength;
}

void* InMemFileNode::GetBaseAddress() const
{
    assert(!mReadFromDisk || (mReadFromDisk && mPopulated));

    return mData;
}

size_t InMemFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    assert(buffer);
    assert(!mReadFromDisk || (mReadFromDisk && mPopulated));

    if ((offset + length) <= mLength)
    {
        assert((mLength == 0) || (mData != NULL));
        memcpy(buffer, (void*)(mData + offset), length); 
        return length;
    }

    INDEXLIB_FATAL_ERROR(OutOfRange, "read file out of range, offset: [%lu], "
                         "read length: [%lu], file length: [%lu]",
                         offset, length, mLength);

    return 0;
}

ByteSliceList* InMemFileNode::Read(size_t length, size_t offset, ReadOption option)
{
    assert(!mReadFromDisk || (mReadFromDisk && mPopulated));

    if ((offset + length) <= mLength)
    {
        assert((mLength == 0) || (mData != NULL));
        ByteSlice* slice = ByteSlice::CreateObject(0);
        slice->size = length;
        slice->data = (uint8_t*)mData + offset;
        ByteSliceList* sliceList = new ByteSliceList(slice);
        return sliceList;
    }

    INDEXLIB_FATAL_ERROR(OutOfRange, "read file out of range, offset: [%lu], "
                         "read length: [%lu], file length: [%lu]",
                         offset, length, mLength);

    return NULL;
}

size_t InMemFileNode::Write(const void* buffer, size_t length)
{
    return DoWrite(buffer, length);
}

void InMemFileNode::Close()
{
    mPopulated = false;
    if (mData != NULL)
    {
        mPool->deallocate(mData, mCapacity);
        mMemController->Free(mMemPeak);
        mMemPeak = 0;
        mCapacity = 0;
        mLength = 0;
        mData = NULL;
    }
}

size_t InMemFileNode::CalculateSize(size_t needLength)
{
    size_t size = mCapacity ? mCapacity : needLength;
    while (size < needLength)
    {
        size *= 2;
    }
    return size;
}

void InMemFileNode::Resize(size_t size)
{
    if (size > mCapacity)
    {
        Extend(size);
    }
    mLength = size;
    AllocateMemQuota(mLength);
}

void InMemFileNode::Extend(size_t extendSize)
{
    assert(extendSize > mCapacity);
    assert(mPool);
    if (!mData)
    {
        mData = (uint8_t*) mPool->allocate(extendSize);
        mCapacity = extendSize;
        return;
    }
    uint8_t* dstBuffer = (uint8_t*) mPool->allocate(extendSize);
    memcpy(dstBuffer, mData, mLength);
    mPool->deallocate(mData, mCapacity);
    mData = dstBuffer;
    mCapacity = extendSize;
}

InMemFileNode* InMemFileNode::Clone() const
{
    return new InMemFileNode(*this);
}

IE_NAMESPACE_END(file_system);


#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/swap_mmap_file_node.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace fslib;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SwapMmapFileNode);

SwapMmapFileNode::SwapMmapFileNode(
        size_t fileSize, const BlockMemoryQuotaControllerPtr& memController)
    : MmapFileNode(LoadConfig(), memController, true)
    , mCursor(0)
    , mRemainFile(true)
    , mReadOnly(true)
{
    mLength = fileSize;
}

SwapMmapFileNode::~SwapMmapFileNode() 
{
    if (mRemainFile)
    {
        Sync();
    }

    try
    {
        Close();
    }
    catch(...)
    {}

    if (!mRemainFile && !GetPath().empty())
    {
        FileSystemWrapper::DeleteIfExist(GetPath());
    }
}

void SwapMmapFileNode::WarmUp()
{
    LoadData();
}

void SwapMmapFileNode::Lock(size_t offset, size_t length)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "SwapMmapFileNode not support lock in memory!");
}

void SwapMmapFileNode::OpenForRead(const std::string& path, FSOpenType openType)
{
    mPath = PathUtil::NormalizePath(path);
    mOpenType = openType;
    mReadOnly = true;

    MmapFileNode::DoOpen(path, openType);
    mCursor = mLength;
    mRemainFile = true;
}

void SwapMmapFileNode::OpenForWrite(const std::string& path, FSOpenType openType)
{
    mPath = PathUtil::NormalizePath(path);
    mOpenType = openType;
    mReadOnly = false;    

    int mapFlag = MAP_SHARED;
    int prot = PROT_READ | PROT_WRITE;
    mType = FSFT_MMAP;

    IE_LOG(DEBUG, "MMap file[%s], loadStrategy[%s], openType[%d], "
           "type[%d], prot [%d], mapFlag [%d]", 
           path.c_str(), ToJsonString(*mLoadStrategy).c_str(), openType, 
           mType, prot, mapFlag);
    FileSystemWrapper::MkDirIfNotExist(PathUtil::GetParentDirPath(path));
    mFile.reset(FileSystemWrapper::MmapFile(path, fslib::WRITE, NULL, 
                                            mLength, prot, mapFlag, 0, -1));
    mData = mFile->getBaseAddress();
    mWarmup = false;
    mRemainFile = false;
}

void SwapMmapFileNode::DoOpen(const string& path, FSOpenType openType)
{
    assert(false);
}

void SwapMmapFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "SwapMmapFileNode not support open in package file!");
}

void SwapMmapFileNode::DoOpen(const string& path, const FileMeta& fileMeta, 
                              FSOpenType openType)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "SwapMmapFileNode not support open in solid mode!");
}

void SwapMmapFileNode::Resize(size_t size)
{
    if (size > mLength)
    {
        IE_LOG(ERROR, "not support resize to length[%lu] over file capacity[%lu]",
               size, mLength);
        return;
    }
    mCursor = size;
}

size_t SwapMmapFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    if (offset + length > mCursor)
    {
        INDEXLIB_FATAL_ERROR(OutOfRange, "read file [%s] out of range, "
                             "offset [%lu], read length: [%lu], file length: [%lu]",
                             GetPath().c_str(), offset, length, mCursor);
    }
    return MmapFileNode::Read(buffer, length, offset, option);
}

util::ByteSliceList* SwapMmapFileNode::Read(size_t length, size_t offset, ReadOption option)
{
    if (offset + length > mCursor)
    {
        INDEXLIB_FATAL_ERROR(OutOfRange, "read file [%s] out of range, "
                             "offset [%lu], read length: [%lu], file length: [%lu]",
                             GetPath().c_str(), offset, length, mCursor);
    }
    return MmapFileNode::Read(length, offset, option);
}

size_t SwapMmapFileNode::Write(const void* buffer, size_t length)
{
    if (GetOpenType() != FSOT_MMAP)
    {
        return 0;
    }
    
    assert(mFile);
    int64_t toWriteLen = length;
    if (mCursor + length > mLength)
    {
        toWriteLen = mLength > mCursor ? mLength - mCursor : 0;
    }

    assert(mData);
    memcpy((char*)mData + mCursor, buffer, toWriteLen);
    mCursor += toWriteLen;
    return toWriteLen;
}

void SwapMmapFileNode::Sync()
{
    if (GetOpenType() != FSOT_MMAP)
    {
        return;
    }
    
    if (mFile)
    {
        mFile->flush();
    }
}

IE_NAMESPACE_END(file_system);


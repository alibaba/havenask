#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/mmap_file_node.h"
#include "indexlib/file_system/file_carrier.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MmapFileNode);

MmapFileNode::MmapFileNode(const LoadConfig& loadConfig,
                           const util::BlockMemoryQuotaControllerPtr& memController,
                           bool readOnly)
    : mLoadStrategy(DYNAMIC_POINTER_CAST(MmapLoadStrategy, loadConfig.GetLoadStrategy()))
    , mData(NULL)
    , mLength(0)
    , mType(FSFT_UNKNOWN)
    , mWarmup(loadConfig.GetWarmupStrategy().GetWarmupType() != WarmupStrategy::WARMUP_NONE)
    , mPopulated(false)
    , mReadOnly(readOnly)
{
    assert(mLoadStrategy);
    assert(mLoadStrategy->GetSlice() != 0);
    mMemController.reset(new util::SimpleMemoryQuotaController(memController));
}

MmapFileNode::~MmapFileNode() 
{
    try
    {
        Close();
    }
    catch(...)
    {}
}

void MmapFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
{
    const string& packageDataFilePath = packageOpenMeta.GetPhysicalFilePath();
    size_t length = packageOpenMeta.GetLength();
    size_t offset = packageOpenMeta.GetOffset();

    IE_LOG(DEBUG, "mmap innerfilePath:%s, packageDataFilePath:%s,"
           "fileLength:%lu, offset:%lu", 
           GetPath().c_str(), packageDataFilePath.c_str(), length, offset);

    if (packageOpenMeta.physicalFileCarrier)
    {
        const MmapFileNodePtr& mmapFileNode = packageOpenMeta.physicalFileCarrier->GetMmapLockFileNode();
        if (mmapFileNode)
        {
            IE_LOG(DEBUG, "file[%s] in package[%s] shared, useCount[%ld]",
                   GetPath().c_str(), packageDataFilePath.c_str(), mmapFileNode.use_count());
            mDependFileNode = mmapFileNode;
            return DoOpenInSharedFile(packageDataFilePath, offset, length, mmapFileNode->mFile, openType);
        }
    }
    assert(IsInPackage());
    DoOpenMmapFile(packageDataFilePath, offset, length, openType,
                   packageOpenMeta.GetPhysicalFileLength());
}

void MmapFileNode::DoOpen(const string& path, const fslib::FileMeta& fileMeta,
                          FSOpenType openType)
{
    DoOpenMmapFile(path, 0, fileMeta.fileLength, openType, fileMeta.fileLength);
}

void MmapFileNode::DoOpen(const string& path, FSOpenType openType)
{
    DoOpenMmapFile(path, 0, -1, openType, -1);
}

void MmapFileNode::DoOpenMmapFile(const string& path, size_t offset, 
                                  size_t length, FSOpenType openType, ssize_t fileLength)
{
    assert(!mFile);
    int mapFlag = 0;
    int prot = 0;
    if (mReadOnly)
    {
        mapFlag = MAP_SHARED;
        prot = PROT_READ;
    }
    else
    {
        mapFlag = MAP_PRIVATE;
        prot = PROT_READ | PROT_WRITE;        
    }

    mType = mLoadStrategy->IsLock() ? FSFT_MMAP_LOCK : FSFT_MMAP;

    IE_LOG(DEBUG, "MMap file[%s], loadStrategy[%s], openType[%d], "
           "type[%d], prot [%d], mapFlag [%d]", 
           path.c_str(), ToJsonString(*mLoadStrategy).c_str(), openType, 
           mType, prot, mapFlag);

    if (length == 0)
    {
        // the known empty file, may be in package file or file meta cached
        mFile.reset(new MMapFile(path, -1, NULL, 0, 0, EC_OK));
    }
    else
    {
        mFile.reset(FileSystemWrapper::MmapFile(path, READ, NULL,
                                                length, prot, mapFlag, offset, fileLength));
    }
    mData = mFile->getBaseAddress();
    assert(mFile->getLength() >= 0);
    mLength = mFile->getLength();

    if (mLoadStrategy->IsLock() && !mWarmup)
    {
        IE_LOG(DEBUG, "lock enforce warmup, set warmp is true.");
        mWarmup = true;
    }
    if (mLoadStrategy->IsLock())
    {
        mMemController->Allocate(mLength);
    }
}

// multiple file share one mmaped shared package file
void MmapFileNode::DoOpenInSharedFile(
        const string& path, size_t offset, size_t length,
        const std::shared_ptr<fslib::fs::MMapFile>& sharedFile,
        FSOpenType openType)
{
    assert(!mFile);

    IE_LOG(DEBUG, "MMap file[%s], loadStrategy[%s], openType[%d], type[%d], ", 
           path.c_str(), ToJsonString(*mLoadStrategy).c_str(), openType, mType);

    mFile = sharedFile;
    mData = mFile->getBaseAddress() + offset;
    mLength = length;
    mType = mLoadStrategy->IsLock() ? FSFT_MMAP_LOCK : FSFT_MMAP;

    if (mLoadStrategy->IsLock() && !mWarmup)
    {
        IE_LOG(DEBUG, "lock enforce warmup, set warmp is true.");
        mWarmup = true;
    }

    // dcache mmapFile has load all data in memory
    if (-1 == mFile->getFd())
    {
        if (!mLoadStrategy->IsLock())
        {
            IE_LOG(ERROR, "file[%s] in shared package [%s] is anonymous mmap, should must be mmap lock",
                   GetPath().c_str(), sharedFile->getFileName());
            mType = FSFT_MMAP_LOCK;
        }
    }
}

void MmapFileNode::Populate()
{
    ScopedLock lock(mLock);
    if (mPopulated)
    {
        return;
    }
    if (mDependFileNode)
    {
        mDependFileNode->Populate();
        mPopulated = true;
        return;
    }
    
    ErrorCode ec = mFile->populate(mLoadStrategy->IsLock(), (int64_t)mLoadStrategy->GetSlice(),
                                   (int64_t)mLoadStrategy->GetInterval());
    if (ec == EC_OK)
    {
        mPopulated = true;
        return;
    }
    else if (ec != EC_NOTSUP)
    {
        INDEXLIB_FATAL_IO_ERROR(ec, "populate file [%s] failed, lock[%d], slice[%uB], interval[%ums]",
                                GetPath().c_str(), mLoadStrategy->IsLock(),
                                mLoadStrategy->GetSlice(), mLoadStrategy->GetInterval());
    }
    else if (-1 == mFile->getFd())
    {
        // for old dcache version without populate
        mPopulated = true;
        return;
    }

    if (mData && mLoadStrategy->IsAdviseRandom())
    {
        IE_LOG(TRACE1, "madvse random in MmapFileNode.");
        if(madvise(mData, mLength, MADV_RANDOM) < 0)
        {
            IE_LOG(WARN, "madvice failed! errno:%d", errno);
        }
    }

    if (mWarmup)
    {
        LoadData();
    }
    mPopulated = true;
}

FSFileType MmapFileNode::GetType() const
{
    return mType;
}

size_t MmapFileNode::GetLength() const
{
    return mLength;
}

void* MmapFileNode::GetBaseAddress() const
{
    assert(!mWarmup || (mWarmup && mPopulated));
    return mData;
}

size_t MmapFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    assert(mFile);
    assert(!mWarmup || (mWarmup && mPopulated));

    if ((offset + length) <= mLength)
    {
        assert((mLength == 0) || (mData != NULL));
        memcpy(buffer, (void*)((uint8_t*)mData + offset), length); 
        return length;
    }

    INDEXLIB_FATAL_ERROR(OutOfRange, "read file [%s] out of range, "
                         "offset [%lu], read length: [%lu], file length: [%lu]",
                         GetPath().c_str(), offset, length, mLength);
    return 0;
}

ByteSliceList* MmapFileNode::Read(size_t length, size_t offset, ReadOption option)
{
    assert(mFile);
    assert(!mWarmup || (mWarmup && mPopulated));

    if ((offset + length) <= mLength)
    {
        assert((mLength == 0) || (mData != NULL));
        ByteSlice* slice = ByteSlice::CreateObject(0);
        slice->size = length;
        slice->data = (uint8_t*)mData + offset;
        ByteSliceList* sliceList = new ByteSliceList(slice);
        return sliceList;
    }

    INDEXLIB_FATAL_ERROR(OutOfRange, "read file [%s] out of range, "
                         "offset [%lu], read length: [%lu], file length: [%lu]",
                         GetPath().c_str(), offset, length, mLength);

    return NULL;
}

size_t MmapFileNode::Write(const void* buffer, size_t length)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not supported Write");
    return 0;
}

void MmapFileNode::Close()
{
    if (mDependFileNode)
    {
        IE_LOG(DEBUG, "close file[%s] in package[%s] shared, useCount[%ld]",
               GetPath().c_str(), mDependFileNode->GetPath().c_str(), mDependFileNode.use_count());
    }
    mPopulated = false;
    mFile.reset();
    mDependFileNode.reset();
}

void MmapFileNode::LoadData()
{
    const char *base = (const char *)mData;
    IE_LOG(DEBUG, "Begin load file [%s], length [%ldB], "
           "slice [%uB], interval [%ums].",
           GetPath().c_str(), mLength, mLoadStrategy->GetSlice(),
           mLoadStrategy->GetInterval());

    for (int64_t offset = 0; offset < (int64_t)mLength; offset += mLoadStrategy->GetSlice())
    {
        int64_t lockLen = min((int64_t)mLength - offset, (int64_t)mLoadStrategy->GetSlice());
        (void)WarmUp(base + offset, lockLen);
        if (mLoadStrategy->IsLock())
        {
            if (mlock(base + offset, lockLen) < 0)
            {
                INDEXLIB_FATAL_ERROR(FileIO, "lock file: [%s] FAILED"
                        ", errno: %d, errmsg: %s",
                        GetPath().c_str(), errno, strerror(errno));
            }
        }
        usleep(mLoadStrategy->GetInterval() * 1000);
    }

    IE_LOG(DEBUG, "End load.");
}

uint8_t MmapFileNode::WarmUp(const char *addr, int64_t len)
{
    static int WARM_UP_PAGE_SIZE = getpagesize();

    uint8_t *pStart = (uint8_t*)addr;
    uint8_t *pEnd = pStart + len;

    uint8_t noUse = 0;
    while (pStart < pEnd) 
    {
        noUse += *pStart;
        pStart += WARM_UP_PAGE_SIZE;
    }
    return noUse;
}

void MmapFileNode::Lock(size_t offset, size_t length)
{
    IE_LOG(DEBUG, "partial lock file[%s], offset[%lu], length[%lu], "
           "lock[%d], partialLock[%d]",
           GetPath().c_str(), offset, length,
           mLoadStrategy->IsLock(), mLoadStrategy->IsPartialLock());
    if (!mLoadStrategy->IsPartialLock() || mLoadStrategy->IsLock())
    {
        return;
    }
    size_t endOffset = offset + length;
    if (endOffset > GetLength())
    {
        endOffset = GetLength();
    }
    if (offset > GetLength() || endOffset < offset)
    {
        IE_LOG(ERROR, "file[%s] has bad param, offset[%lu], length[%lu], "
               "fileLength[%lu]",
               GetPath().c_str(), offset, length, GetLength());
        return;
    }
    const char *base = (const char *)mData;
    for (; offset < endOffset; offset += mLoadStrategy->GetSlice())
    {
        int64_t lockLen = min(endOffset - offset, (size_t)mLoadStrategy->GetSlice());
        (void)WarmUp(base + offset, lockLen);
        if (mlock(base + offset, lockLen) < 0)
        {
            INDEXLIB_FATAL_ERROR(FileIO, "lock file: [%s] FAILED"
                    ", errno: %d, errmsg: %s",
                    GetPath().c_str(), errno, strerror(errno));
        }
        usleep(mLoadStrategy->GetInterval() * 1000);
    }
}

future_lite::Future<uint32_t> MmapFileNode::ReadVUInt32Async(size_t offset, ReadOption option)
{
    uint8_t* byte = static_cast<uint8_t*>(mData) + offset;
    uint32_t value = (*byte) & 0x7f;
    int shift = 7;
    while ((*byte) & 0x80)
    {
        ++byte;
        value |= ((*byte & 0x7F) << shift);
        shift += 7;
    }
    return future_lite::makeReadyFuture<uint32_t>(value);
}

IE_NAMESPACE_END(file_system);

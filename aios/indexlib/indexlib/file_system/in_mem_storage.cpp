#include "indexlib/misc/exception.h"
#include "indexlib/file_system/load_config/load_config.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/async_dump_scheduler.h"
#include "indexlib/file_system/simple_dump_scheduler.h"
#include "indexlib/file_system/in_mem_storage.h"
#include "indexlib/file_system/single_file_flush_operation.h"
#include "indexlib/file_system/mkdir_flush_operation.h"
#include "indexlib/file_system/in_mem_file_writer.h"
#include "indexlib/file_system/normal_file_reader.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/file_system/directory_file_node_creator.h"
#include "indexlib/file_system/in_mem_package_file_writer.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemStorage);

InMemStorage::InMemStorage(const string& rootPath,
                           const BlockMemoryQuotaControllerPtr& memController,
                           bool needFlush, bool asyncFlush)
    : Storage(rootPath, FSST_IN_MEM, memController)
    , mRootPathTable(new FileNodeCache(NULL))
{
    if (needFlush)
    {
        if (asyncFlush)
        {
            mFlushScheduler.reset(new AsyncDumpScheduler());
        }
        else
        {
            mFlushScheduler.reset(new SimpleDumpScheduler());
        }
    }
    mOperationQueue = CreateNewFlushOperationQueue();
}

InMemStorage::~InMemStorage() 
{
}

void InMemStorage::Init(const FileSystemOptions& options)
{
    mOptions = options;
    if (NeedFlush() && options.enablePathMetaContainer)
    {
        IE_LOG(INFO, "Init in-memory flush path meta cache!");
        mPathMetaContainer.reset(new PathMetaContainer);
    }

    mInMemFileNodeCreator.reset(new InMemFileNodeCreator());
    mInMemFileNodeCreator->Init(LoadConfig(), mMemController);

    mDirectoryFileNodeCreator.reset(new DirectoryFileNodeCreator());
    mDirectoryFileNodeCreator->Init(LoadConfig(), mMemController);
    mOperationQueue = CreateNewFlushOperationQueue();
}

FileWriterPtr InMemStorage::CreateFileWriter(
    const string& filePath,
    const FSWriterParam& param)
{
    IE_LOG(DEBUG, "FilePath:%s", filePath.c_str());
    if (param.prohibitInMemDump)
    {
        return Storage::CreateFileWriter(filePath, param);
    }

    // delay check exist to store & flush phase
    FileWriterPtr fileWriter(new InMemFileWriter(mMemController, this, param));
    fileWriter->Open(filePath);
    return fileWriter;
}

FileReaderPtr InMemStorage::CreateFileReaderWithoutCache(const std::string& filePath,
        FSOpenType type)
{
    return CreateFileReader(filePath, type);
}


FileReaderPtr InMemStorage::CreateFileReader(const string& filePath,
        FSOpenType type)
{
    if (type != FSOT_IN_MEM)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "In mem storage not supported "
                             "type[%d], file[%s]", type, filePath.c_str());
    }

    FileNodePtr fileNode = mFileNodeCache->Find(filePath);
    if (!fileNode)
    {
        if (!IsExistInPackageFile(filePath) && !IsExistOnDisk(filePath))
        {
            INDEXLIB_THROW_WARN(NonExist, "File [%s] not exist", filePath.c_str());
        }
        IE_LOG(DEBUG, "File [%s] cache miss, Open type [%d]",
               filePath.c_str(), type);
        assert(mInMemFileNodeCreator);
        fileNode = mInMemFileNodeCreator->CreateFileNode(filePath, type, false);
        OpenFileNode(fileNode, filePath, type);
        mFileNodeCache->Insert(fileNode);
        mStorageMetrics.IncreaseFileCacheMiss();
    }
    else
    {
        if (fileNode->GetOpenType() != type)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "File [%s] open type[%d] "
                    "not match with cache type[%d]",
                    filePath.c_str(), type, fileNode->GetOpenType());
        }
        IE_LOG(DEBUG, "File [%s] cache hit, Open type [%d]",
               filePath.c_str(), type);
        mStorageMetrics.IncreaseFileCacheHit();
    }

    return FileReaderPtr(new NormalFileReader(fileNode));
}

FileReaderPtr InMemStorage::CreateWritableFileReader(const string& filePath,
        FSOpenType type)
{
    return CreateFileReader(filePath, type);
}

void InMemStorage::MakeDirectory(const string& dirPath, bool recursive)
{
    IE_LOG(DEBUG, "Make dir[%s], recursive[%d]", dirPath.c_str(), recursive);

    FileNodePtr fileNode = mFileNodeCache->Find(dirPath);
    if (fileNode)
    {
        if (fileNode->GetType() != FSFT_DIRECTORY)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "path[%s] already exist and is not dir!", 
                    fileNode->GetPath().c_str());
        }

        if (!recursive && !fileNode->IsInPackage())
        {
            INDEXLIB_FATAL_ERROR(Exist, "Dir [%s] has exist.", dirPath.c_str());
        }
        fileNode->SetInPackage(false);
        IE_LOG(DEBUG, "dir[%s] is exist, recursive[%d]",
               dirPath.c_str(), recursive);
        return;
    }

    if (recursive)
    {
        MakeDirectoryRecursive(dirPath, true);
    }
    else
    {
        string parentPath = PathUtil::GetParentDirPath(dirPath);
        if (!IsExistInCache(parentPath))
        {
            INDEXLIB_FATAL_ERROR(NonExist, "Parent dir [%s] does not exist.",
                    dirPath.c_str());
        }
        DoMakeDirectory(dirPath, true);
    }
}

void InMemStorage::MakeDirectoryRecursive(const string& dirPath, 
        bool physicalPath)
{
    if (!dirPath.empty())
    {
        string parentPath = PathUtil::GetParentDirPath(dirPath);
        FileNodePtr fileNode = mFileNodeCache->Find(parentPath);
        if (!fileNode)
        {
            MakeDirectoryRecursive(parentPath, physicalPath);
        }
        else
        {
            if (fileNode->GetType() != FSFT_DIRECTORY)
            {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                        "path[%s] already exist and is not dir!", 
                        fileNode->GetPath().c_str());
            }
            if (physicalPath)
            {
                fileNode->SetInPackage(false);
            }
        }
    }

    IE_LOG(DEBUG, "Make dir[%s]", dirPath.c_str());
    DoMakeDirectory(dirPath, physicalPath);
}

void InMemStorage::DoMakeDirectory(const string& dirPath, bool physicalPath)
{
    assert(mDirectoryFileNodeCreator);
    FileNodePtr dirNode =
        mDirectoryFileNodeCreator->CreateFileNode(dirPath, FSOT_UNKNOWN, true);
    dirNode->SetDirty(true);
    dirNode->SetInPackage(!physicalPath);
    mFileNodeCache->Insert(dirNode);
    if (NeedFlush() && physicalPath)
    {
        // delay check exist on disk to dump phase
        MkdirFlushOperationPtr flushOperatiron(new MkdirFlushOperation(dirNode));
        assert(mOperationQueue);
        mOperationQueue->PushBack(flushOperatiron);
    }
}

void InMemStorage::RemoveDirectory(const string& dirPath, bool mayNonExist)
{
    DoRemoveDirectory(dirPath, mayNonExist);
    if (mRootPathTable->IsExist(dirPath))
    {
        mRootPathTable->RemoveDirectory(dirPath);
    }
    if (mPathMetaContainer)
    {
        mPathMetaContainer->RemoveDirectory(dirPath);
    }
}

size_t InMemStorage::EstimateFileLockMemoryUse(const string& filePath, 
        FSOpenType type)
{
    if (type == FSOT_SLICE)
    {
        return SliceFileNode::EstimateFileLockMemoryUse(GetFileLength(filePath));
    }

    if (type == FSOT_IN_MEM)
    {
        return mInMemFileNodeCreator->EstimateFileLockMemoryUse(
                GetFileLength(filePath));
    }

    INDEXLIB_FATAL_ERROR(UnSupported, "In mem storage not supported "
                         "type[%d], file[%s]", type, filePath.c_str());
    return 0;
}

bool InMemStorage::IsExist(const string& path) const
{
    return IsExistInCache(path) || IsExistInPackageFile(path) || IsExistOnDisk(path);
}

bool InMemStorage::IsExistOnDisk(const string& path) const
{
    if (mPathMetaContainer)
    {
        // optimize: avoid check existance from hdfs path
        return mPathMetaContainer->IsExist(path);
    }
    return storage::FileSystemWrapper::IsExist(path);
}

void InMemStorage::GetFileMetaOnDisk(const string& filePath, FileMeta& fileMeta) const
{
    if (mPathMetaContainer)
    {
        FileInfo info;
        if (mPathMetaContainer->GetFileInfo(filePath, info))
        {
            // attention : createTime & modifyTime may not be same with dfs
            fileMeta.fileLength = info.fileLength;
            fileMeta.createTime = info.modifyTime;
            fileMeta.lastModifyTime = info.modifyTime;
            return;
        }
        // optimize: avoid check existance from hdfs path
        INDEXLIB_THROW_WARN(NonExist, "File [%s] not exist.", filePath.c_str());
    }
    FileSystemWrapper::GetFileMeta(filePath, fileMeta);
}

void InMemStorage::ListDirRecursiveOnDisk(const string& path,
        FileList& fileList, bool ignoreError, bool skipSecondaryRoot) const
{
    if (mPathMetaContainer)
    {
        // optimize: avoid check existance from hdfs path
        mPathMetaContainer->ListFile(path, fileList, true);
    }
    else
    {
        FileSystemWrapper::ListDirRecursive(path, fileList, ignoreError);
    }
}
    
void InMemStorage::ListDirOnDisk(const string& path, FileList& fileList,
                                 bool ignoreError, bool skipSecondaryRoot) const
{
    if (mPathMetaContainer)
    {
        // optimize: avoid check existance from hdfs path
        mPathMetaContainer->ListFile(path, fileList, false);
    }
    else
    {
        FileSystemWrapper::ListDir(path, fileList, ignoreError);
    }
}

void InMemStorage::ListFile(const string& dirPath, FileList& fileList, 
                            bool recursive, bool physicalPath, bool skipSecondaryRoot) const
{
    fileList.clear();
    
    FileNodePtr fileNode = mFileNodeCache->Find(dirPath);
    if (fileNode && fileNode->GetType() != FSFT_DIRECTORY)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "Path [%s] is not a dir.",
                             dirPath.c_str());
    }

    if (!IsExistInCache(dirPath))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Directory[%s] not exist in InMemStorage",
                             dirPath.c_str());
    }

    if (physicalPath)
    {
        DoListPackageFilePysicalPath(dirPath, fileList, recursive);
    }
    else
    {
        DoListFileInPackageFile(dirPath, fileList, recursive);
    }
    DoListFileWithOnDisk(dirPath, fileList, recursive, physicalPath, true, skipSecondaryRoot);
    SortAndUniqFileList(fileList);
}

std::future<bool> InMemStorage::Sync(bool waitFinish)
{
    auto flushPromise = unique_ptr<promise<bool>>(new promise<bool>());
    auto flushFuture = flushPromise->get_future();
    if (!NeedFlush())
    {
        IE_LOG(INFO, "Sync No Need. WaitFinish: %d.", waitFinish);
        flushPromise->set_value(false);
    }
    else
    {
        IE_LOG(INFO, "Sync Start. WaitFinish: %d.", waitFinish);
        assert(mOperationQueue);
        mFlushScheduler->WaitDumpQueueEmpty();
        if (mOperationQueue->Size() == 0)
        {
            flushPromise->set_value(true);
        }
        else
        {
            mOperationQueue->SetPromise(std::move(flushPromise));
            {
                ScopedLock lock(mLock);
                mFlushScheduler->Push(mOperationQueue);
#ifdef __INDEXLIB_INMEMSTORAGETEST_TESTMULTITHREADSTOREFILE
                usleep(20);
#endif

                mOperationQueue = CreateNewFlushOperationQueue();
            }
        }
        if (waitFinish)
        {
            mFlushScheduler->WaitDumpQueueEmpty();
        }
        IE_LOG(INFO, "Sync Finish.");
    }
    return flushFuture;
}

void InMemStorage::Close()
{
    Sync(true);
}

void InMemStorage::StoreFile(const FileNodePtr& fileNode, const FSDumpParam& param)
{
    assert(fileNode);

    StoreWhenNonExist(fileNode);
    if (fileNode->GetType() == FSFT_SLICE || fileNode->GetType() == FSFT_MMAP)
    {
        return;
    }

    assert(fileNode->GetType() == FSFT_IN_MEM);
    if (NeedFlush())
    {
        FSDumpParam dumpParam = param;
        if (!dumpParam.raidConfig)
        {
            dumpParam.raidConfig = mOptions.raidConfig;
        }
        FileFlushOperationPtr flushOperation(new SingleFileFlushOperation(fileNode, dumpParam));
        AddFlushOperation(flushOperation);
    }
}

void InMemStorage::AddFlushOperation(
        const FileFlushOperationPtr& fileFlushOperation)
{
    assert(NeedFlush());
    assert(mOperationQueue);
    if (mOptions.prohibitInMemDump)
    {
        fileFlushOperation->Execute(mPathMetaContainer);
        return;
    }
    ScopedLock lock(mLock);
    mOperationQueue->PushBack(fileFlushOperation);
}

void InMemStorage::MakeRoot(const string& rootPath)
{
    IE_LOG(DEBUG, "Add root path [%s]", rootPath.c_str());

    if (IsExist(rootPath) || mRootPathTable->IsExist(rootPath))
    {
        INDEXLIB_FATAL_ERROR(Exist, "Path[%s] is exist", rootPath.c_str());
    }
    DoMakeDirectory(rootPath, true);

    assert(mDirectoryFileNodeCreator);
    FileNodePtr dirNode =
        mDirectoryFileNodeCreator->CreateFileNode(rootPath, FSOT_UNKNOWN, true);
    mRootPathTable->Insert(dirNode);
}

void InMemStorage::ListRoot(const string& supperRoot, FileList& fileList,
                            bool recursive)
{
    fileList.clear();
    mRootPathTable->ListFile(supperRoot, fileList, recursive);
}

PackageFileWriterPtr InMemStorage::CreatePackageFileWriter(
        const string& filePath)
{
    PackageFileWriterMap::const_iterator iter = 
        mPackageFileWriterMap.find(filePath);
    if (iter != mPackageFileWriterMap.end())
    {
        INDEXLIB_FATAL_ERROR(Exist, "PackageFileWriter for Path [%s] is exist!",
                             filePath.c_str());
        return PackageFileWriterPtr();
    }

    PackageFileWriterPtr packageFileWriter(
            new InMemPackageFileWriter(filePath, this, mMemController));

    mPackageFileWriterMap.insert(make_pair(filePath, packageFileWriter));
    return packageFileWriter;
}

PackageFileWriterPtr InMemStorage::GetPackageFileWriter(
        const string& filePath)
{
    PackageFileWriterMap::const_iterator iter = 
        mPackageFileWriterMap.find(filePath);
    if (iter == mPackageFileWriterMap.end())
    {
        return PackageFileWriterPtr();
    }

    PackageFileWriterPtr packageFileWriter = iter->second;
    if (packageFileWriter->IsClosed())
    {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "PackageFileWriter for Path [%s] is already closed!",
                             filePath.c_str());
        return PackageFileWriterPtr();
    }
    return packageFileWriter;
}

FlushOperationQueuePtr InMemStorage::CreateNewFlushOperationQueue()
{
    if (!NeedFlush())
    {
        return FlushOperationQueuePtr();
    }
    
    return FlushOperationQueuePtr(new FlushOperationQueue(
                    mFileNodeCache.get(), &mStorageMetrics, mPathMetaContainer));
}

IE_NAMESPACE_END(file_system);


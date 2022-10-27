#include <autil/TimeUtility.h>
#include "indexlib/file_system/storage.h"
#include "indexlib/file_system/slice_file_node.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/directory_map_iterator.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, Storage);

Storage::Storage(const std::string& rootPath, FSStorageType storageType,
                 const util::BlockMemoryQuotaControllerPtr& memController) 
    : mFileNodeCache(new FileNodeCache(&mStorageMetrics))
    , mStorageType(storageType)
    , mMemController(memController)
    , mRootPath(rootPath)
{
}

Storage::~Storage() 
{
}

SliceFilePtr Storage::CreateSliceFile(const string& path, uint64_t sliceLen, 
                                      int32_t sliceNum)
{
    IE_LOG(DEBUG, "Create slice file[%s], sliceLen[%lu], sliceNum[%d]", 
           path.c_str(), sliceLen, sliceNum);

    SliceFilePtr sliceFile = GetSliceFile(path);
    if (sliceFile)
    {
        return sliceFile;
    }

    SliceFileNodePtr sliceFileNode(new SliceFileNode(sliceLen, sliceNum, mMemController));
    sliceFileNode->Open(path, FSOT_SLICE);
    // slice file will be clean by CleanCache
    sliceFileNode->SetDirty(false);
    sliceFileNode->SetStorageMetrics(&mStorageMetrics);
    // diff from other writer(buffered & inMem),
    // appears when open for read & write in same time
    StoreFile(sliceFileNode);

    return SliceFilePtr(new SliceFile(sliceFileNode));
}

SliceFilePtr Storage::GetSliceFile(const string& path)
{
    FileNodePtr fileNode = mFileNodeCache->Find(path);
    if (!fileNode)
    {
        return SliceFilePtr();
    }

    SliceFileNodePtr sliceFileNode = 
        DYNAMIC_POINTER_CAST(SliceFileNode, fileNode);
    if (!sliceFileNode)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, 
                             "File[%s] is not slice file, actual type is [%d]", 
                             path.c_str(), fileNode->GetType());
    }

    return SliceFilePtr(new SliceFile(sliceFileNode));
}

SwapMmapFileReaderPtr Storage::CreateSwapMmapFileReader(const string& path)
{
    SwapMmapFileNodePtr swapMmapFileNode;
    FileNodePtr fileNode = mFileNodeCache->Find(path);
    if (fileNode)
    {
        swapMmapFileNode = DYNAMIC_POINTER_CAST(SwapMmapFileNode, fileNode);
    }

    if (!swapMmapFileNode)
    {
        size_t fileLen = GetFileLength(path);
        swapMmapFileNode.reset(new SwapMmapFileNode(fileLen, mMemController));
        // TODO: check : why OpenForRead?
        swapMmapFileNode->OpenForRead(path, FSOT_MMAP);
        swapMmapFileNode->SetDirty(false);
        mFileNodeCache->Insert(swapMmapFileNode);
        mStorageMetrics.IncreaseFileCacheMiss();
    }
    else
    {
        mStorageMetrics.IncreaseFileCacheHit();
    }
    return SwapMmapFileReaderPtr(new SwapMmapFileReader(swapMmapFileNode));
}
    
SwapMmapFileWriterPtr Storage::CreateSwapMmapFileWriter(const string& path, size_t fileSize)
{
    IE_LOG(DEBUG, "Create swap mmap file[%s], fileSize[%lu]", path.c_str(), fileSize);

    bool isExistOnDisk = IsExistOnDisk(path);
    if (isExistOnDisk || IsExistInCache(path))
    {
        INDEXLIB_FATAL_ERROR(Exist, "File[%s] already exist %s", 
                             path.c_str(), isExistOnDisk ? "on disk":"in mem");
    }

    SwapMmapFileNodePtr swapMmapFileNode(new SwapMmapFileNode(
                    fileSize, mMemController));
    swapMmapFileNode->OpenForWrite(path, FSOT_MMAP);
    // swap mmap file will be clean by CleanCache
    swapMmapFileNode->SetDirty(false);
    StoreFile(swapMmapFileNode);
    return SwapMmapFileWriterPtr(new SwapMmapFileWriter(swapMmapFileNode, this));
}
 
InMemoryFilePtr Storage::CreateInMemoryFile(const std::string& path, uint64_t fileLength)
{
    IE_LOG(DEBUG, "Create in memory file[%s], file length[%lu]", 
           path.c_str(), fileLength);

    return InMemoryFilePtr(new InMemoryFile(path, fileLength, this, mMemController));
}

size_t Storage::GetFileLength(const std::string& filePath) const
{
    const FileNodePtr& fileNode = mFileNodeCache->Find(filePath);
    if (fileNode)
    {
        return fileNode->GetLength();
    }

    PackageFileMeta::InnerFileMeta innerFileMeta;
    if (GetPackageFileMountMeta(filePath, innerFileMeta))
    {
        return innerFileMeta.GetLength();
    }

    size_t fileLen = 0;
    if (mPackageFileMountTable.GetPackageFileLength(filePath, fileLen))
    {
        return fileLen;
    }
    
    FileMeta fileMeta;
    GetFileMetaOnDisk(filePath, fileMeta);
    return fileMeta.fileLength;
}

FSFileType Storage::GetFileType(const std::string& filePath) const
{
    const FileNodePtr& fileNode = mFileNodeCache->Find(filePath);
    if (fileNode)
    {
        return fileNode->GetType();
    }
    return FSFT_UNKNOWN;
}

void Storage::GetFileStat(const string& filePath, FileStat& fileStat) const
{
    // if filePath belongs to packagefile, fileStat.OnDisk always be false
    // todo: check innerfile's package file if on disk or not.
    fileStat.storageType = mStorageType;
    fileStat.fileType = FSFT_UNKNOWN;
    fileStat.openType = FSOT_UNKNOWN;
    fileStat.fileLength = 0;
    fileStat.inCache = IsExistInCache(filePath);
    fileStat.onDisk = IsExistOnDisk(filePath);
    fileStat.inPackage = IsExistInPackageFile(filePath);
    fileStat.isDirectory = false;
    if (fileStat.inCache)
    {
        const FileNodePtr& fileNode = mFileNodeCache->Find(filePath);
        assert(fileNode);
        fileStat.fileType = fileNode->GetType();
        fileStat.openType = fileNode->GetOpenType();
        fileStat.fileLength = fileNode->GetLength();        
        fileStat.isDirectory = (fileStat.fileType == FSFT_DIRECTORY);
    }
    else if (fileStat.onDisk)
    {
        fileStat.isDirectory = IsDirOnDisk(filePath);
        FileMeta fileMeta;
        GetFileMetaOnDisk(filePath, fileMeta);
        fileStat.fileLength = fileMeta.fileLength;
    }
    else if (fileStat.inPackage)
    {
        PackageFileMeta::InnerFileMeta innerMeta;
        GetPackageFileMountMeta(filePath, innerMeta);
        fileStat.isDirectory = innerMeta.IsDir();
        fileStat.fileLength = innerMeta.GetLength();
    }
    else
    {
        INDEXLIB_FATAL_ERROR(NonExist, "File[%s] is not exist", filePath.c_str());
    }
}

void Storage::GetFileMeta(const std::string& filePath, fslib::FileMeta& fileMeta) const
{
    bool filePathExistOnDisk = true;
    try
    {
        GetFileMetaOnDisk(filePath, fileMeta);
    }
    catch (const NonExistException& e)
    {
        filePathExistOnDisk = false;
    }
    catch(...)
    {
        throw;
    }

    if (filePathExistOnDisk)
    {
        return;
    }

    fileMeta.createTime = (uint64_t)-1;
    fileMeta.lastModifyTime = (uint64_t)-1;
    size_t fileLen = 0;
    if (IsExistInCache(filePath))
    {
        fileMeta.fileLength = mFileNodeCache->Find(filePath)->GetLength();
    }
    else if (IsExistInPackageFile(filePath))
    {
        PackageFileMeta::InnerFileMeta innerMeta;
        GetPackageFileMountMeta(filePath, innerMeta);
        fileMeta.fileLength = innerMeta.GetLength();
    }
    else if (mPackageFileMountTable.GetPackageFileLength(filePath, fileLen))
    {
        fileMeta.fileLength = fileLen;   
    }
    else
    {
        INDEXLIB_THROW_WARN(NonExist, "File[%s] is not exist", filePath.c_str());
    }
}

bool Storage::IsDir(const string& path) const
{
    if (IsExistInCache(path))
    {
        const FileNodePtr& fileNode = mFileNodeCache->Find(path);
        assert(fileNode);
        return fileNode->GetType() == FSFT_DIRECTORY;
    }

    if (IsExistInPackageFile(path))
    {
        PackageFileMeta::InnerFileMeta innerMeta;
        GetPackageFileMountMeta(path, innerMeta);
        return innerMeta.IsDir();
    }

    if (IsExistInPackageMountTable(path))
    {
        return false;
    }
    return IsDirOnDisk(path);
}

void Storage::StoreWhenNonExist(const FileNodePtr& fileNode)
{
    const string& filePath = fileNode->GetPath();
    if (IsExistInCache(filePath))
    {
        FileNodePtr nodeInCache = mFileNodeCache->Find(filePath);
        if (nodeInCache.get() == fileNode.get())
        {
            return;
        }
        INDEXLIB_FATAL_ERROR(Exist, "File[%s] is exist", filePath.c_str());
    }
    mFileNodeCache->Insert(fileNode);
}

void Storage::Truncate(const string& filePath, size_t newLength)
{
    if (IsExistInCache(filePath))
    {
        mFileNodeCache->Truncate(filePath, newLength);
        return;
    }
    assert(false && "only support file in cache now");
}

void Storage::DoListPackageFilePysicalPath(const string& dirPath, 
        FileList& fileList, bool recursive) const
{
    string normalizeDirPath = FileSystemWrapper::NormalizeDir(dirPath);
    vector<PackageFileWriterPtr> matchPackageFileWriters;
    GetMatchedPackageFileWriter(dirPath, recursive, matchPackageFileWriters);
    for (size_t i = 0; i < matchPackageFileWriters.size(); i++)
    {
        if (!matchPackageFileWriters[i]->IsClosed())
        {
            continue;
        }

        vector<string> absPaths;
        matchPackageFileWriters[i]->GetPhysicalFiles(absPaths);
        for (size_t j = 0; j < absPaths.size(); j++)
        {
            fileList.push_back(absPaths[j].substr(normalizeDirPath.size()));
        }
    }
}

void Storage::DoListFileInPackageFile(const string& dirPath, 
        FileList& fileList, bool recursive) const
{
    mPackageFileMountTable.ListFile(dirPath, fileList, recursive);
}

bool Storage::DoRemoveFileInPackageFile(const string& filePath)
{
    return mPackageFileMountTable.RemoveFile(filePath);
}

bool Storage::DoRemoveDirectoryInPackageFile(const string& dirPath)
{
    return mPackageFileMountTable.RemoveDirectory(dirPath);
}

void Storage::DoListFileWithOnDisk(const string& dirPath, FileList& fileList, 
                                   bool recursive, bool physicalPath, bool ignoreError,
                                   bool skipSecondaryRoot) const
{
    mFileNodeCache->ListFile(dirPath, fileList, recursive, physicalPath);

    if (!IsExistOnDisk(dirPath))
    {
        if (!ignoreError)
        {
            INDEXLIB_FATAL_ERROR(NonExist, "%s not exist!", dirPath.c_str());
        }
        return;
    }

    FileList onDiskFileList;
    if (recursive)
    {
        ListDirRecursiveOnDisk(dirPath, onDiskFileList, false, skipSecondaryRoot);
    }
    else
    {
        ListDirOnDisk(dirPath, onDiskFileList, false, skipSecondaryRoot);
    }
    fileList.insert(fileList.end(), 
                    onDiskFileList.begin(), onDiskFileList.end());
}

void Storage::SortAndUniqFileList(FileList& fileList) const
{
    sort(fileList.begin(), fileList.end());
    FileList::iterator fileListRepeat =
        unique(fileList.begin(), fileList.end());
    fileList.erase(fileListRepeat, fileList.end());
}

void Storage::RemoveFile(const string& filePath, bool mayNonExist)
{
    IE_LOG(DEBUG, "Remove file[%s]", filePath.c_str());

    bool isExistInCache = IsExistInCache(filePath);
    if (isExistInCache && mFileNodeCache->GetUseCount(filePath) > 1)
    {
        DoSync(true);
        isExistInCache = IsExistInCache(filePath);
    }

    bool isExistOnDisk = IsExistOnDisk(filePath);
    bool isInPackage = IsExistInPackageFile(filePath);

    if (!isExistInCache && !isExistOnDisk && !isInPackage)
    {
        if (mayNonExist)
        {
            return;
        }
        INDEXLIB_FATAL_ERROR(NonExist, "File[%s] not exist", filePath.c_str());
    }

    if (isExistInCache)
    {
        if (!mFileNodeCache->RemoveFile(filePath))
        {
            INDEXLIB_FATAL_ERROR(MemFileIO, "File[%s] remove failed",
                    filePath.c_str());
        }
    }

    if (isInPackage)
    {
        if (!DoRemoveFileInPackageFile(filePath))
        {
            INDEXLIB_FATAL_ERROR(MemFileIO, "File[%s] remove failed",
                    filePath.c_str());
        }
    }

    if (isExistOnDisk)
    {
        if (IsDirOnDisk(filePath))
        {
            INDEXLIB_FATAL_ERROR(MemFileIO, "Path[%s] is dir, remove failed",
                    filePath.c_str());
        }
        if (mPathMetaContainer)
        {
            mPathMetaContainer->RemoveFile(filePath);
        }
        DoRemoveFileInLifecycleTable(filePath);
        FileSystemWrapper::Delete(filePath, mayNonExist);
    }
}

void Storage::DoRemoveDirectory(const string& dirPath, bool mayNonExist)
{
    IE_LOG(DEBUG, "Remove dir[%s]", dirPath.c_str());

    bool isExistInCache = IsExistInCache(dirPath);
    bool isExistOnDisk = IsExistOnDisk(dirPath);
    bool isInPackage = IsExistInPackageFile(dirPath);

    if (!isExistInCache && !isExistOnDisk && !isInPackage)
    {
        if (mayNonExist)
        {
            return;
        }
        INDEXLIB_FATAL_ERROR(NonExist, "Directory[%s] not exist",
                             dirPath.c_str());
    }

    if (!mFileNodeCache->RemoveDirectory(dirPath))
    {
        INDEXLIB_FATAL_ERROR(MemFileIO, "Directory[%s] remove failed",
                             dirPath.c_str());
    }

    if (isInPackage) 
    {
        if (!DoRemoveDirectoryInPackageFile(dirPath))
        {
            INDEXLIB_FATAL_ERROR(MemFileIO, "Directory[%s] remove failed",
                    dirPath.c_str());
        }
    }

    if (isExistOnDisk)
    {
        if (!IsDirOnDisk(dirPath))
        {
            INDEXLIB_FATAL_ERROR(MemFileIO, "Path[%s] is file, remove failed", 
                    dirPath.c_str());            
        }
        if (mPathMetaContainer)
        {
            mPathMetaContainer->RemoveDirectory(dirPath);
        }
        DoRemoveDirectoryInLifecycleTable(dirPath);
        FileSystemWrapper::Delete(dirPath, mayNonExist);
    }
    DoRemovePackageFileWriter(dirPath);
}

void Storage::CleanCache()
{
    IE_LOG(DEBUG, "Clean Cache.");
    mFileNodeCache->Clean();
}

bool Storage::MountPackageFile(const string& filePath)
{
    return mPackageFileMountTable.MountPackageFile(filePath, this);
}

void Storage::GetMatchedPackageFileWriter(
        const string& dirPath, bool recursive,
        vector<PackageFileWriterPtr>& matchedPackageWriters) const
{
    string matchDirPrefix = FileSystemWrapper::NormalizeDir(dirPath);
    PackageFileWriterMap::const_iterator iter = mPackageFileWriterMap.begin();
    for ( ; iter != mPackageFileWriterMap.end(); iter++)
    {
        string parentDirPath = FileSystemWrapper::NormalizeDir(
                PathUtil::GetParentDirPath(iter->first));
        if (parentDirPath.length() >= matchDirPrefix.length()
            && parentDirPath.find(matchDirPrefix) == 0)
        {
            if (!recursive && 
                (parentDirPath.length() > matchDirPrefix.length()))
            {
                continue;
            }
            matchedPackageWriters.push_back(iter->second);
        }
    }
}

void Storage::DoRemovePackageFileWriter(const string& dirPath)
{
    DirectoryMapIterator<PackageFileWriterPtr> dirIter(
            mPackageFileWriterMap, dirPath);
    while (dirIter.HasNext())
    {
        DirectoryMapIterator<PackageFileWriterPtr>::Iterator iter = 
            dirIter.Next();
        if (!iter->second->IsClosed())
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, 
                    "package file writer in [%s] is not closed!", 
                    dirPath.c_str());
        }
        dirIter.Erase(iter);
    }
}

FileWriterPtr Storage::CreateFileWriter(
    const string& filePath, const FSWriterParam& param)
{
    IE_LOG(DEBUG, "FilePath:%s", filePath.c_str());

    assert(param.prohibitInMemDump || IsExistOnDisk(PathUtil::GetParentDirPath(filePath)));
    // string parentDirPath = PathUtil::GetParentDirPath(filePath);
    // if (!param.prohibitInMemDump && !IsExistOnDisk(parentDirPath))
    // {
    //     INDEXLIB_FATAL_ERROR(NonExist, "Parent Dir[%s] does not exist on disk", 
    //                          parentDirPath.c_str());
    // }

    bool isExistOnDisk = IsExistOnDisk(filePath);
    if (isExistOnDisk || IsExistInCache(filePath))
    {
        INDEXLIB_FATAL_ERROR(Exist, "File[%s] already exist %s", 
                             filePath.c_str(), isExistOnDisk?"on disk":"in mem");
    }

    BufferedFileWriterPtr fileWriter(new BufferedFileWriter(param));
    // no need to check exist again
    fileWriter->OpenWithoutCheck(filePath);
    return fileWriter;
}

void Storage::OpenFileNode(FileNodePtr fileNode, const string& filePath, FSOpenType type)
{
    PackageOpenMeta packageOpenMeta;
    FileInfo info;
    if (GetPackageOpenMeta(filePath, packageOpenMeta))
    {
        // if file is in package
        fileNode->Open(packageOpenMeta, type);
    }
    else if (mPathMetaContainer && mPathMetaContainer->GetFileInfo(filePath, info))
    {
        // if file meta is already loaded
        FileMeta fileMeta;
        fileMeta.fileLength = info.fileLength;
        fileMeta.createTime = info.modifyTime;
        fileMeta.lastModifyTime = info.modifyTime;
        fileNode->Open(filePath, fileMeta, type);
    }
    else
    {
        fileNode->Open(filePath, type);
        if (mPathMetaContainer)
        {
            // -1 means invalid ts
            mPathMetaContainer->AddFileInfo(filePath, fileNode->GetLength(), -1, false);            
        }
    }
}

void Storage::GetFileMetaOnDisk(const string& filePath, fslib::FileMeta& fileMeta) const
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
    }
    FileSystemWrapper::GetFileMeta(filePath, fileMeta);
}

bool Storage::IsDirOnDisk(const string& path) const
{
    if (mPathMetaContainer)
    {
        // optimize: avoid check existance from hdfs path
        FileInfo info;
        if (mPathMetaContainer->GetFileInfo(path, info))
        {
            return info.isDirectory();
        }
        if (mPathMetaContainer->MatchSolidPath(path))
        {
            return false;
        }
    }
    return FileSystemWrapper::IsDir(path);
}

void Storage::ListDirRecursiveOnDisk(const string& path,
                                     fslib::FileList& fileList, bool ignoreError,
                                     bool skipSecondaryRoot) const
{
    if (mPathMetaContainer && mPathMetaContainer->MatchSolidPath(path))
    {
        mPathMetaContainer->ListFile(path, fileList, true);
    }
    else
    {
        FileSystemWrapper::ListDirRecursive(path, fileList, ignoreError);
    }
}

void Storage::ListDirOnDisk(const string& path, fslib::FileList& fileList,
                            bool ignoreError, bool skipSecondaryRoot) const
{
    if (mPathMetaContainer && mPathMetaContainer->MatchSolidPath(path))
    {
        mPathMetaContainer->ListFile(path, fileList, false);
    }
    else
    {
        FileSystemWrapper::ListDir(path, fileList, ignoreError);
    }
}

bool Storage::GetPackageFileMountMeta(const string& filePath,
                                      PackageFileMeta::InnerFileMeta& innerMeta) const
{
    return mPackageFileMountTable.GetMountMeta(filePath, innerMeta);
}

bool Storage::GetPackageOpenMeta(const string& filePath, PackageOpenMeta& packageOpenMeta)
{
    if (mPackageFileMountTable.GetMountMeta(filePath, packageOpenMeta.innerFileMeta))
    {
        size_t physicalFileLength = -1;
        bool ret = mPackageFileMountTable.GetPackageFileLength(
            packageOpenMeta.GetPhysicalFilePath(), physicalFileLength);
        assert(ret);(void)ret;
        packageOpenMeta.physicalFileLength = (ssize_t)physicalFileLength;
        packageOpenMeta.SetLogicalFilePath(filePath);
        return true;
    }
    return false;
}

bool Storage::IsExistOnDisk(const string& path) const
{
    if (!mPathMetaContainer)
    {
        return storage::FileSystemWrapper::IsExist(path);
    }
    
    // optimize: avoid check existance from hdfs path
    if (mPathMetaContainer->IsExist(path))
    {
        return true;
    }
    if (mPathMetaContainer->MatchSolidPath(path))
    {
        return false;
    }
    if (!mOptions.isOffline)
    {
        // do not update metaContainer when it is a OnlineFileSystem
        return storage::FileSystemWrapper::IsExist(path);
    }
    
    fslib::PathMeta pathMeta;
    ErrorCode ec = fslib::fs::FileSystem::getPathMeta(path, pathMeta);
    if (ec == EC_NOENT)
    {
        return false;
    }
    if (mPathMetaContainer)
    {
        mPathMetaContainer->AddFileInfo(
            path, pathMeta.isFile?pathMeta.length:-1,
            pathMeta.lastModifyTime, !pathMeta.isFile);
    }
    return true;
}

string Storage::GetFileLifecycle(const string& path) const
{
    static const string EMPTY_STRING;
    string physicalPath = path;
    PackageFileMeta::InnerFileMeta meta;
    if (GetPackageFileMountMeta(path, meta))
    {
        physicalPath = meta.GetFilePath();
    }
    return mLifecycleTable.GetLifecycle(physicalPath);
}

void Storage::Validate(const std::string& path, int64_t expectLength) const
{
    if (expectLength >= 0)
    {
        size_t actualLength = GetFileLength(path);
        if (actualLength != (size_t)expectLength)
        {
            INDEXLIB_FATAL_ERROR(FileIO, "file [%s] length not equal, expect[%lu], actual[%ld]",
                    path.c_str(), expectLength, actualLength);
        }
    }
    else if (!IsExist(path))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "path [%s] not existed.", path.c_str());
    }
}

IE_NAMESPACE_END(file_system);


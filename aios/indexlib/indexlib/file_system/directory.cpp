#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/pack_directory.h"
#include "indexlib/file_system/link_directory.h"
#include "indexlib/file_system/compress_file_writer.h"
#include "indexlib/file_system/compress_file_reader.h"
#include "indexlib/file_system/compress_file_info.h"
#include "indexlib/file_system/compress_file_reader_creator.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/in_memory_file.h"
#include "indexlib/file_system/swap_mmap_file_reader.h"

using namespace std;
using namespace fslib;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, Directory);

Directory::Directory(const string& path, 
                     const IndexlibFileSystemPtr& indexFileSystem)
    : mRootDir(PathUtil::NormalizePath(path))
    , mFileSystem(indexFileSystem)
{}

FileReaderPtr Directory::CreateIntegratedFileReader(const string& filePath)
{
    FSOpenType openType = GetIntegratedOpenType(filePath);
    FileReaderPtr fileReader = CreateFileReader(filePath, openType);
    // empty file opened with MMAP, NULL should be returned by GetBaseAddress 
    // assert(fileReader->GetBaseAddress());
    return fileReader;
}

size_t Directory::EstimateIntegratedFileMemoryUse(const string& filePath)
{
    if (!IsExist(filePath))
    {
        return 0;
    }
    FSOpenType type = GetIntegratedOpenType(filePath);
    return EstimateFileMemoryUse(filePath, type);
}

void Directory::ListFile(const string& path, FileList& fileList,
                         bool recursive, bool physicalPath, bool skipSecondaryRoot) const
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    mFileSystem->ListFile(absPath, fileList, recursive, physicalPath, skipSecondaryRoot);
}

bool Directory::IsExist(const string& path)
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    return mFileSystem->IsExist(absPath);
}

bool Directory::IsExistInCache(const std::string& path)
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    return mFileSystem->IsExistInCache(absPath);
}

bool Directory::IsExistInPackageMountTable(const string& path) const
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    return mFileSystem->IsExistInPackageMountTable(absPath);
}

bool Directory::IsDir(const std::string& path)
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    return mFileSystem->IsDir(absPath);
}

void Directory::RemoveDirectory(const string& dirPath, bool mayNonExist)
{
    string absPath = PathUtil::JoinPath(mRootDir, dirPath);
    return mFileSystem->RemoveDirectory(absPath, mayNonExist);
}

void Directory::RemoveFile(const string& filePath, bool mayNonExist)
{
    string absPath = PathUtil::JoinPath(mRootDir, filePath);
    return mFileSystem->RemoveFile(absPath, mayNonExist);
}

size_t Directory::GetFileLength(const string& filePath) const
{
    string absPath = PathUtil::JoinPath(mRootDir, filePath);
    return mFileSystem->GetFileLength(absPath);
}

FSFileType Directory::GetFileType(const string& filePath) const
{
    string absPath = PathUtil::JoinPath(mRootDir, filePath);
    return mFileSystem->GetFileType(absPath);
}

void Directory::Load(const string& fileName, string& fileContent,
                     bool insertToCacheIfNotExist)
{
    FileReaderPtr fileReader = CreateFileReader(fileName, FSOT_IN_MEM);
    if (insertToCacheIfNotExist)
    {
        mFileSystem->InsertToFileNodeCacheIfNotExist(fileReader);
    }
    size_t fileLength = fileReader->GetLength();
    char* data = (char*)fileReader->GetBaseAddress();
    fileContent = string(data, fileLength);
    fileReader->Close();
}

bool Directory::LoadMayNonExist(const string& fileName, string& fileContent,
                                bool cacheFileNode)
{
    try
    {
        Load(fileName, fileContent, cacheFileNode);
    }
    catch (const NonExistException& e)
    {
        return false;
    }
    catch (...)
    {
        throw;
    }
    return true; 
}

void Directory::Store(const string& fileName, const string& fileContent, bool atomic)
{
    FSWriterParam param;
    param.atomicDump = atomic;
    FileWriterPtr fileWriter = CreateFileWriter(fileName, param);
    fileWriter->Write(fileContent.c_str(), fileContent.size());
    fileWriter->Close();
}

void Directory::GetFileMeta(const std::string& filePath, FileMeta& fileMeta) const
{
    string absPath = PathUtil::JoinPath(mRootDir, filePath);
    mFileSystem->GetFileMeta(absPath, fileMeta);
}

void Directory::GetFileStat(const string& filePath, FileStat& fileStat)
{
    string absPath = PathUtil::JoinPath(mRootDir, filePath);
    mFileSystem->GetFileStat(absPath, fileStat);
}

SliceFilePtr Directory::CreateSliceFile(const string& path,
                                        uint64_t sliceLen, int32_t sliceNum)
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    return mFileSystem->CreateSliceFile(absPath, sliceLen, sliceNum);
}

SliceFilePtr Directory::GetSliceFile(const string& path)
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    return mFileSystem->GetSliceFile(absPath);
}

InMemoryFilePtr Directory::CreateInMemoryFile(const string& path, uint64_t fileLen)
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    if (mFileSystem->IsExistInCache(absPath))
    {
        // only in cache file (no one use it, remove will success)
        mFileSystem->RemoveFile(absPath);
        IE_LOG(INFO, "File [%s] already exist, remove it before createInMemoryFile!",
               absPath.c_str());
    }
    return mFileSystem->CreateInMemoryFile(absPath, fileLen);
}

bool Directory::MountPackageFile(const string& filePath)
{
    string absPath = PathUtil::JoinPath(mRootDir, filePath);
    assert(mFileSystem);
    return mFileSystem->MountPackageFile(absPath);
}

LinkDirectoryPtr Directory::CreateLinkDirectory() const
{
    if (!mFileSystem->UseRootLink())
    {
        return LinkDirectoryPtr();
    }

    const string& rootPath = mFileSystem->GetRootPath();
    string relativePath;
    if (!PathUtil::GetRelativePath(rootPath, mRootDir, relativePath))
    {
        IE_LOG(ERROR, "dir path [%s] not in root path[%s]",
               mRootDir.c_str(), rootPath.c_str());
        return LinkDirectoryPtr();
    }

    string linkPath = PathUtil::JoinPath(mFileSystem->GetRootLinkPath(), relativePath);
    if (!FileSystemWrapper::IsExist(linkPath))
    {
        return LinkDirectoryPtr();
    }
    return LinkDirectoryPtr(new LinkDirectory(linkPath, mFileSystem));
}

PackDirectoryPtr Directory::CreatePackDirectory(const string& packageFileName)
{
    PackDirectoryPtr packDirectory(
            new PackDirectory(mRootDir, mFileSystem));
    PackageFileWriterPtr packFileWriter = 
        GetPackageFileWriter(packageFileName);
    if (!packFileWriter)
    {
        packFileWriter = CreatePackageFileWriter(packageFileName);
    }

    packDirectory->Init(packFileWriter);
    return packDirectory;
}

// for index_printer
CompressFileReaderPtr Directory::CreateCompressFileReaderWhenNotExistInCache(
        const string& filePath, FSOpenType type)
{
    CompressFileInfo compressInfo;
    LoadCompressFileInfo(filePath, compressInfo);
    
    FileReaderPtr dataReader = CreateFileReaderWhenNotExistInCache(filePath, type);
    return CompressFileReaderCreator::Create(dataReader, compressInfo, this);
}

// for index_printer
FileReaderPtr Directory::CreateFileReaderWhenNotExistInCache(
        const string& filePath, FSOpenType type)
{
    if (IsExistInCache(filePath))
    {
        FileStat fileStat;
        GetFileStat(filePath, fileStat);
        return CreateFileReader(filePath, fileStat.openType);
    }
    return CreateFileReader(filePath, type);
}
// for kv segment writer

CompressFileReaderPtr Directory::CreateCompressFileReader(
        const string& filePath, FSOpenType type)
{
    CompressFileInfo compressInfo;
    LoadCompressFileInfo(filePath, compressInfo);
    
    FileReaderPtr dataReader = CreateFileReader(filePath, type);
    return CompressFileReaderCreator::Create(dataReader, compressInfo, this);
}

CompressFileWriterPtr Directory::CreateCompressFileWriter(
        const string& filePath,
        const string& compressorName,
        size_t compressBufferSize,
        const FSWriterParam& param)
{
    string compressInfoFilePath = filePath + COMPRESS_FILE_INFO_SUFFIX;
    FileWriterPtr compressInfoFileWriter = CreateFileWriter(compressInfoFilePath, param);
    assert(compressInfoFileWriter);
    
    FileWriterPtr fileWriter = CreateFileWriter(filePath, param);
    assert(fileWriter);

    CompressFileWriterPtr compressWriter(new CompressFileWriter);
    compressWriter->Init(fileWriter, compressInfoFileWriter,
                         compressorName, compressBufferSize);
    return compressWriter;
}

void Directory::LoadCompressFileInfo(const string& filePath,
                                     CompressFileInfo &compressInfo)
{
    string compressInfoPath = filePath + COMPRESS_FILE_INFO_SUFFIX;
    string compressInfoStr;
    Load(compressInfoPath, compressInfoStr);
    compressInfo.FromString(compressInfoStr);
}

CompressFileInfoPtr Directory::GetCompressFileInfo(const string& filePath)
{
    string compressInfoPath = filePath + COMPRESS_FILE_INFO_SUFFIX;
    if (!IsExist(compressInfoPath))
    {
        return CompressFileInfoPtr();
    }

    CompressFileInfoPtr info(new CompressFileInfo);
    string compressInfoStr;
    Load(compressInfoPath, compressInfoStr);
    info->FromString(compressInfoStr);
    return info;
}

SwapMmapFileReaderPtr Directory::CreateSwapMmapFileReader(const string& filePath)
{
    string absPath = PathUtil::JoinPath(mRootDir, filePath);
    return mFileSystem->CreateSwapMmapFileReader(absPath);
}

CompressFileReaderPtr Directory::CreateCompressSwapMmapFileReader(const string& filePath)
{
    CompressFileInfo compressInfo;
    LoadCompressFileInfo(filePath, compressInfo);

    SwapMmapFileReaderPtr dataReader = CreateSwapMmapFileReader(filePath);
    return CompressFileReaderCreator::Create(dataReader, compressInfo, this);
}

SwapMmapFileWriterPtr Directory::CreateSwapMmapFileWriter(
        const string& filePath, size_t fileSize)
{
    string absPath = PathUtil::JoinPath(mRootDir, filePath);
    if (mFileSystem->IsExistInCache(absPath))
    {
        // only in cache file (no one use it, remove will success)
        mFileSystem->RemoveFile(absPath);
        IE_LOG(INFO, "File [%s] already exist, remove it before createSwapMmapFileWriter!",
               absPath.c_str());
    }
    return mFileSystem->CreateSwapMmapFileWriter(absPath, fileSize);
}

string Directory::GetRootLinkName() const
{
    string rootLinkName;
    if (!mFileSystem->UseRootLink())
    {
        return string("");
    }
    
    PathUtil::GetRelativePath(mFileSystem->GetRootPath(),
                              mFileSystem->GetRootLinkPath(),
                              rootLinkName);
    return rootLinkName;
}

std::future<bool> Directory::Sync(bool waitFinish)
{
    return mFileSystem->Sync(waitFinish);
}

FSStorageType Directory::GetStorageType() const
{
    return mFileSystem->GetStorageType(mRootDir, false);
}

void Directory::AddPathFileInfo(const FileInfo& fileInfo)
{
    mFileSystem->AddPathFileInfo(mRootDir, fileInfo);
}

void Directory::AddSolidPathFileInfos(const vector<FileInfo>::const_iterator& firstFileInfo,
                                      const vector<FileInfo>::const_iterator& lastFileInfo)
{
    if (mFileSystem->MatchSolidPath(mRootDir))
    {
        IE_LOG(DEBUG, "path [%s] already mark as solid", mRootDir.c_str());
        return;
    }
    mFileSystem->AddSolidPathFileInfos(mRootDir, firstFileInfo, lastFileInfo);
}

bool Directory::SetLifecycle(const string& lifecycle)
{
    return mFileSystem->SetDirLifecycle(mRootDir, lifecycle);
}

ArchiveFolderPtr Directory::GetArchiveFolder()
{
    ArchiveFolderPtr folder(new ArchiveFolder(false));
    try
    {
        folder->Open(mRootDir);
    }
    catch (const NonExistException& e)
    {
        string relativePath;
        if (!PathUtil::GetRelativePath(mFileSystem->GetRootPath(), mRootDir, relativePath))
        {
            assert(false);
            IE_LOG(ERROR, "dir path [%s] not in root path[%s]",
                   mRootDir.c_str(), mFileSystem->GetRootPath().c_str());
            return ArchiveFolderPtr();
        }

        folder.reset(new ArchiveFolder(false));
        folder->Open(PathUtil::JoinPath(mFileSystem->GetSecondaryRootPath(), relativePath));
    }
    return folder;
}

void Directory::Validate(const std::string& path, int64_t expectLength) const
{
    string absPath = PathUtil::JoinPath(mRootDir, path);
    return mFileSystem->Validate(absPath, expectLength);
}

IE_NAMESPACE_END(file_system);


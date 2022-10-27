#include <autil/StringUtil.h>
#include "indexlib/file_system/package_file_mount_table.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/storage.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackageFileMountTable);

PackageFileMountTable::PackageFileMountTable(StorageMetrics* storageMetrics) 
    : mStorageMetrics(storageMetrics)
{
}

PackageFileMountTable::~PackageFileMountTable() 
{
}

bool PackageFileMountTable::MountPackageFile(
        const string& primaryPackageFilePath, Storage* storage)
{
    ScopedLock lock(mLock);

    if (IsMounted(primaryPackageFilePath))
    {
        IE_LOG(DEBUG, "package file [%s] already be mounted!", 
               primaryPackageFilePath.c_str());
        return true;
    }

    const string& packageFileMetaPath = storage ?
        PackageFileMeta::GetPackageFileMetaPath(primaryPackageFilePath) :
        GetPackageFileMetaPath(primaryPackageFilePath);
    PackageFileMetaPtr fileMeta = LoadPackageFileMeta(packageFileMetaPath, storage);
    if (!fileMeta)
    {
        CleanPhysicalPathCache();
        return false;
    }

    ValidatePackageDataFiles(primaryPackageFilePath, fileMeta, storage);
    MountPackageFile(primaryPackageFilePath, fileMeta);
    return true;
}

void PackageFileMountTable::ValidatePackageDataFiles(
    const string& primaryPackageFilePath,
    const PackageFileMetaPtr& fileMeta, Storage* storage) const
{
    assert(fileMeta);
    const std::vector<size_t>& physicalFileLengths = fileMeta->GetPhysicalFileLengths();
    const std::vector<std::string>& physicalFileNames = fileMeta->GetPhysicalFileNames();
    const std::vector<std::string>& physicalFileTags = fileMeta->GetPhysicalFileTags();
    if (physicalFileNames.size() != physicalFileLengths.size() ||
        physicalFileNames.size() != physicalFileTags.size())
    {
        INDEXLIB_FATAL_ERROR(FileIO, "package file is broken, path[%s], [%lu] != [%lu] != [%lu]",
                             primaryPackageFilePath.c_str(),
                             physicalFileNames.size(), physicalFileLengths.size(),
                             physicalFileTags.size());
    }
    for (size_t i = 0; i < physicalFileNames.size(); ++i)
    {
        string dataFilePath = GetPackageFileDataPath(
                primaryPackageFilePath, physicalFileTags[i], i, false);
        string dataFilePrimaryPath = PackageFileMeta::GetPackageFileDataPath(primaryPackageFilePath, physicalFileTags[i], i);
        assert(dataFilePath.find(physicalFileNames[i]) != string::npos);
        size_t actualFileLength = storage ?
                                  storage->GetFileLength(dataFilePrimaryPath) :
                                  FileSystemWrapper::GetFileLength(dataFilePath);
        size_t expectFileLength = physicalFileLengths[i];
        if (expectFileLength != actualFileLength)
        {
            INDEXLIB_FATAL_ERROR(FileIO, "package file [%s] is broken, [%lu] != [%lu]",
                    dataFilePath.c_str(), expectFileLength, actualFileLength);
        }
    }
}

void PackageFileMountTable::MountPackageFile(const string& primaryPackageFilePath,
        const PackageFileMetaPtr& fileMeta, bool forDump)
{
    ScopedLock lock(mLock);

    assert(fileMeta);
    assert(!fileMeta->GetPhysicalFileNames().empty());

    string primaryParentDir = PathUtil::GetParentDirPath(primaryPackageFilePath);
    vector<uint32_t> refCount;
    refCount.resize(fileMeta->GetPhysicalFileNames().size(), 0u);
    for (auto iter = fileMeta->Begin(); iter != fileMeta->End(); iter++)
    {
        const PackageFileMeta::InnerFileMeta& innerFileMeta = *iter;
        uint32_t dataFileIdx = innerFileMeta.GetDataFileIdx();
        string actualDataPath = GetPackageFileDataPath(primaryPackageFilePath,
                fileMeta->GetPhysicalFileTag(dataFileIdx), dataFileIdx, forDump);
        PackageFileMeta::InnerFileMeta mountMeta(actualDataPath, innerFileMeta.IsDir());
        mountMeta.Init(innerFileMeta.GetOffset(), innerFileMeta.GetLength(),
                       innerFileMeta.GetDataFileIdx());
        string innerFilePath = PathUtil::JoinPath(primaryParentDir, innerFileMeta.GetFilePath());
        InsertMountItem(innerFilePath, mountMeta);
        ++(refCount[dataFileIdx]);
    }
    mMountedMap.insert(make_pair(primaryPackageFilePath, fileMeta->GetInnerFileCount()));

    string primaryMetaPath = PackageFileMeta::GetPackageFileMetaPath(primaryPackageFilePath);
    const string& actualMetaPath = GetPackageFilePath(primaryMetaPath, forDump);
    for (size_t i = 0; i < fileMeta->GetPhysicalFileNames().size(); ++i)
    {
        string primaryDataPath = PackageFileMeta::GetPackageFileDataPath(
                primaryPackageFilePath, fileMeta->GetPhysicalFileTag(i), i);
        auto packageFileIter = mPhysicalFileSize.find(primaryDataPath);
        if (packageFileIter == mPhysicalFileSize.end())
        {
            // store package data file len for deploy index
            mPhysicalFileSize.insert(make_pair(primaryDataPath,
                            fileMeta->GetPhysicalFileLength(i)));
        }
        const string& dataPath = GetPackageFilePath(primaryDataPath, forDump);
        mPhysicalFileInfoMap[dataPath] =
            PhysicalFileInfo{primaryDataPath, actualMetaPath, refCount[i]};
    }
    // store package meta file len for deploy index
    mPhysicalFileSize.insert(make_pair(primaryMetaPath, fileMeta->GetMetaStrLength()));
    mPhysicalFileInfoMap[actualMetaPath] =
        PhysicalFileInfo{primaryMetaPath, "", (uint32_t)fileMeta->GetPhysicalFileNames().size()};
    
    if (mStorageMetrics)
    {
        mStorageMetrics->IncreasePackageFileCount();
    }
    
    CleanPhysicalPathCache();
}

void PackageFileMountTable::InsertMountItem(
        const string& itemPath, const PackageFileMeta::InnerFileMeta& mountMeta)
{
    MountMetaMap::iterator iter = mTable.find(itemPath);
    if (iter == mTable.end())
    {
        mTable.insert(make_pair(itemPath, mountMeta));
        IncreaseInnerNodeMetrics(mountMeta.IsDir());
        return;
    }
    if (!mountMeta.IsDir())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "file %s already mounted!",
                             itemPath.c_str());
    }

    const string& oldPath = iter->second.GetFilePath();
    string newPath = oldPath + ";" + mountMeta.GetFilePath();

    PackageFileMeta::InnerFileMeta newDirMountMeta(newPath, true);
    newDirMountMeta.Init(0, 0, 0);
    mTable[itemPath] = newDirMountMeta;
}

bool PackageFileMountTable::GetMountMeta(
    const string& innerFilePath,
    PackageFileMeta::InnerFileMeta& meta) const
{
    ScopedLock lock(mLock);

    MountMetaMap::const_iterator iter = mTable.find(innerFilePath);
    if (iter == mTable.end())
    {
        return false;
    }
    meta = iter->second;
    return true;
}

bool PackageFileMountTable::IsExist(const string& absPath) const
{
    ScopedLock lock(mLock);
    return mTable.find(absPath) != mTable.end();
}

void PackageFileMountTable::ListFile(
    const string& dirPath, FileList& fileList, 
    bool recursive) const
{
    string normalPath = PathUtil::NormalizePath(dirPath);

    ScopedLock lock(mLock);
    DirectoryIterator iter(mTable, normalPath);
    while (iter.HasNext())
    {
        DirectoryIterator::Iterator it = iter.Next();
        const string& path = it->first;
        if (!recursive && path.find("/", normalPath.size() + 1) != string::npos)
        {
            continue;
        }

        string relativePath = path.substr(normalPath.size() + 1);
        if (recursive && it->second.IsDir())
        {
            relativePath += "/";
        }
        fileList.push_back(relativePath);
    }
}

bool PackageFileMountTable::RemoveFile(const string& filePath, bool readOnlySecondaryRoot)
{
    string normalPath = PathUtil::NormalizePath(filePath);

    ScopedLock lock(mLock);
    MountMetaMap::iterator iter = mTable.find(normalPath);
    if (iter == mTable.end())
    {
        IE_LOG(DEBUG, "inner filePath [%s] not exist!", 
               filePath.c_str());
        return false;
    }

    if (iter->second.IsDir())
    {
        IE_LOG(DEBUG, "inner filePath [%s] should not be dir!", 
               filePath.c_str());
        return false;
    }

    DecreaseNodeCount(iter->second.GetFilePath(), readOnlySecondaryRoot);
    DecreaseInnerNodeMetrics(false);
    mTable.erase(iter);
    return true;
}

void PackageFileMountTable::DecreaseMultiNodeCount(const string& packageFileDataPaths, bool readOnlySecondaryRoot)
{
    vector<string> dataPathVec;
    StringUtil::fromString(packageFileDataPaths, dataPathVec, ";");
    for (size_t i = 0; i < dataPathVec.size(); i++)
    {
        DecreaseNodeCount(dataPathVec[i], readOnlySecondaryRoot);
    }
}

void PackageFileMountTable::DecreaseNodeCount(
        const string& packageFileDataPath, bool readOnlySecondaryRoot)
{
    auto dataIt = mPhysicalFileInfoMap.find(packageFileDataPath);
    assert(dataIt != mPhysicalFileInfoMap.end());

    PhysicalFileInfo& dataInfo = dataIt->second;
    if (--(dataInfo.refCount) == 0)
    {
        if (!readOnlySecondaryRoot || packageFileDataPath == dataInfo.primaryPath)
        {
            FileSystemWrapper::DeleteIfExist(packageFileDataPath);
        }
        string metaPath = dataInfo.packageFileMetaPath;
        mPhysicalFileSize.erase(dataInfo.primaryPath);
        mPhysicalFileInfoMap.erase(dataIt);
        auto metaIt = mPhysicalFileInfoMap.find(metaPath);
        assert(metaIt != mPhysicalFileInfoMap.end());
        PhysicalFileInfo& metaInfo = metaIt->second;
        if (--(metaInfo.refCount) == 0)
        {
            if (!readOnlySecondaryRoot || metaPath == metaInfo.primaryPath)
            {
                FileSystemWrapper::DeleteIfExist(metaPath);
            }
            mPhysicalFileSize.erase(metaInfo.primaryPath);
            mPhysicalFileInfoMap.erase(metaIt);
        }
    }
}

bool PackageFileMountTable::RemoveDirectory(const string& dirPath, bool readOnlySecondaryRoot)
{
    string normalPath = PathUtil::NormalizePath(dirPath);
    ScopedLock lock(mLock);

    MountMetaMap::iterator iter = mTable.find(normalPath);
    if (iter != mTable.end() && !iter->second.IsDir())
    {
        IE_LOG(DEBUG, "inner dirPath [%s] should not be file!", 
               dirPath.c_str());
        return false;
    }

    DirectoryIterator dirIter(mTable, normalPath);
    while (dirIter.HasNext())
    {
        DirectoryIterator::Iterator it = dirIter.Next();
        if (it->second.IsDir())
        {
            DecreaseMultiNodeCount(it->second.GetFilePath(), readOnlySecondaryRoot);
        }
        else
        {
            DecreaseNodeCount(it->second.GetFilePath(), readOnlySecondaryRoot);
        }
        DecreaseInnerNodeMetrics(it->second.IsDir());
        dirIter.Erase(it);
    }

    if (iter != mTable.end())
    {
        DecreaseMultiNodeCount(iter->second.GetFilePath(), readOnlySecondaryRoot);
        DecreaseInnerNodeMetrics(true);
        mTable.erase(iter);
    }
    else
    {
        IE_LOG(DEBUG, "Directory [%s] does not exist", normalPath.c_str());
    }
    RemoveMountedPackageFile(normalPath);
    return true;
}

void PackageFileMountTable::RemoveMountedPackageFile(const string& dirPath)
{
    DirectoryMapIterator<size_t> dirIter(mMountedMap, dirPath);
    while (dirIter.HasNext())
    {
        DirectoryMapIterator<size_t>::Iterator it = dirIter.Next();
        // if (it->second > 0)
        // {
        //     INDEXLIB_FATAL_ERROR(InconsistentState, "package file [%s] not empty!",
        //                          it->first.c_str());
        // }
        dirIter.Erase(it);
        if (mStorageMetrics)
        {
            mStorageMetrics->DecreasePackageFileCount();
        }
    }
}

bool PackageFileMountTable::IsMounted(const string& primaryPackageFilePath) const
{
    return mMountedMap.find(primaryPackageFilePath) != mMountedMap.end();
}

PackageFileMetaPtr PackageFileMountTable::LoadPackageFileMeta(
        const string& packageFileMetaPath, Storage* storage)
{
    string content;
    if (storage)
    {
        try {
            FileReaderPtr fileReader = storage->CreateFileReader(packageFileMetaPath, FSOT_IN_MEM);
            assert(fileReader);
            fileReader->Open();
            content.resize(fileReader->GetLength());
            size_t readLen = fileReader->Read((void*)content.data(), fileReader->GetLength(), 0);
            if (readLen != fileReader->GetLength()) {
                IE_LOG(ERROR, "read length [%lu] not equal to file len [%lu] in file [%s]",
                       readLen, fileReader->GetLength(), packageFileMetaPath.c_str());
                content.clear();
            }
            else
            {
                storage->StoreWhenNonExist(fileReader->GetFileNode());
            }
        }
        catch (const NonExistException& e)
        {
            IE_LOG(DEBUG, "package file meta [%s] not exist!", packageFileMetaPath.c_str());
            return PackageFileMetaPtr();
        }
    }
    
    if (content.empty() && !FileSystemWrapper::AtomicLoad(packageFileMetaPath, content, true))
    {
        IE_LOG(DEBUG, "package file meta [%s] not exist!", packageFileMetaPath.c_str());
        return PackageFileMetaPtr();
    }
    PackageFileMetaPtr fileMeta(new PackageFileMeta);
    fileMeta->InitFromString(content);
    if (fileMeta->GetFileAlignSize() % getpagesize() != 0)
    {
        INDEXLIB_FATAL_ERROR(AssertCompatible,
                             "file align size is not the multiple of page size");
    }
    return fileMeta;
}

bool PackageFileMountTable::GetPackageFileLength(const string& filePath,
                                                 size_t& fileLen) const
{
    auto iter = mPhysicalFileSize.find(filePath);
    if (iter != mPhysicalFileSize.end())
    {
        fileLen = iter->second;
        return true;
    }
    return false;
}

const std::string& PackageFileMountTable::GetPackageFilePath(
        const std::string& primaryPath, bool forDump) const
{
    auto it = mPhysicalPathCache.find(primaryPath);
    if (it != mPhysicalPathCache.end())
    {
        return it->second;
    }

    auto ret = (forDump || !mGetPhysicalPathFunc)
               ? mPhysicalPathCache.insert(make_pair(primaryPath, primaryPath))
               : mPhysicalPathCache.insert(make_pair(primaryPath, mGetPhysicalPathFunc(primaryPath)));
    assert(ret.second);
    return ret.first->second;
}

void PackageFileMountTable::CleanPhysicalPathCache() const
{
    decltype(mPhysicalPathCache) emptyMap;
    mPhysicalPathCache.swap(emptyMap);    
}

const std::string& PackageFileMountTable::GetPackageFileDataPath(
        const std::string& primaryPackageFilePath,
        const std::string& description, uint32_t dataFileIdx,
        bool forDump) const
{
    string path = PackageFileMeta::GetPackageFileDataPath(primaryPackageFilePath, description, dataFileIdx);
    return GetPackageFilePath(path, forDump);
}

const std::string& PackageFileMountTable::GetPackageFileMetaPath(
        const std::string& primaryPackageFilePath) const
{
    string path = PackageFileMeta::GetPackageFileMetaPath(primaryPackageFilePath);
    return GetPackageFilePath(path, false);
}

IE_NAMESPACE_END(file_system);


#include "indexlib/file_system/package_storage.h"
#include "indexlib/file_system/buffered_package_file_writer.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/file_system/versioned_package_file_meta.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackageStorage);

PackageStorage::PackageStorage(const string& rootPath, const string& description,
                               const BlockMemoryQuotaControllerPtr& memController)
    : Storage(rootPath, FSST_PACKAGE, memController)
    , mDescription(description)
    , mFileAlignSize(getpagesize())
    , mIsSynced(true)
{
}

PackageStorage::~PackageStorage() 
{
    for (Unit& unit : mUnits)
    {
        if (!unit.unflushedBuffers.empty())
        {
            cerr << "*** PackageStorage has unFlushedBuffer!" << endl;
            for (auto& unflushedFile : unit.unflushedBuffers)
            {
                DELETE_AND_SET_NULL(unflushedFile.buffer);
            }
            unit.unflushedBuffers.clear();
        }
    }
}

void PackageStorage::Init(const FileSystemOptions& options) { mOptions = options; }

FileWriterPtr PackageStorage::CreateFileWriter(const string& filePath,
                                               const FSWriterParam& param)
{
    const std::string& relativePath = GetRelativePath(filePath);
    int32_t unitId = -1;
    {
        ScopedLock lock(mLock);
        unitId = MatchUnit(relativePath);
    }
    FileWriterPtr fileWriter(new BufferedPackageFileWriter(this, unitId, param));
    try
    {
        fileWriter->Open(filePath);
    }
    catch (...)
    {
        fileWriter.reset();
        throw;
    }
    return fileWriter;
}

FileReaderPtr PackageStorage::CreateFileReader(const string& filePath, FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not supported read");
    return FileReaderPtr();
}

FileReaderPtr PackageStorage::CreateWritableFileReader(
    const string& filePath, FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not supported read");
    return FileReaderPtr();
}

FileReaderPtr PackageStorage::CreateFileReaderWithoutCache(
    const string& filePath, FSOpenType type)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not supported read");
    return FileReaderPtr();
}

PackageFileWriterPtr PackageStorage::CreatePackageFileWriter(const string& filePath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "PackDirectory not support CreatePackageFileWriter!");
    return PackageFileWriterPtr();
}

PackageFileWriterPtr PackageStorage::GetPackageFileWriter(const string& filePath)
{
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "PackDirectory not support CreatePackageFileWriter!");
    return PackageFileWriterPtr();
}

void PackageStorage::MakeDirectory(const string& dirPath, bool recursive)
{
    const string& relativePath = GetRelativePath(dirPath);
    ScopedLock lock(mLock);
    int32_t unitId = MatchUnit(relativePath);
    if (unlikely(unitId < 0))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "Directory [%s] not find match root!",
                             dirPath.c_str());
    }

    auto& unit = mUnits[unitId];
    if (recursive)
    {
        return MakeDirectoryRecursive(unit, relativePath);
    }
    const string& parentDir = PathUtil::GetParentDirPath(dirPath);
    if (unlikely(!IsExist(parentDir)))
    {
        INDEXLIB_FATAL_ERROR(NonExist, "parent of dir [%s] not exist", dirPath.c_str());
    }
    if (unit.metas.find(relativePath) != unit.metas.end())
    {
        INDEXLIB_FATAL_ERROR(Exist, "dir [%s] already exist", relativePath.c_str());
    }
    DoMakeDirectory(unit, relativePath);
}

void PackageStorage::MakeDirectoryRecursive(Unit& unit, const string& relativePath)
{
    if (relativePath == unit.rootPath)
    {
        return;
    }
    
    string parentPath = PathUtil::GetParentDirPath(relativePath);
    auto iter = unit.metas.find(parentPath);
    if (iter == unit.metas.end())
    {
        MakeDirectoryRecursive(unit, parentPath);
    }
    else
    {
        if (!iter->second.IsDir())
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "path[%s] already exist and is not dir",
                                 JoinRoot(iter->second.GetFilePath()).c_str());
        }
    }
    assert(IsExist(JoinRoot(parentPath)));
    DoMakeDirectory(unit, relativePath);
}

void PackageStorage::DoMakeDirectory(Unit& unit, const string& relativePath)
{
    string path = relativePath.substr(unit.rootPath.size() + 1);
    if (unit.metas.count(path) == 0)
    {
        PackageFileMeta::InnerFileMeta meta(path, true);
        unit.metas.insert(make_pair(relativePath, meta));
    }
}

void PackageStorage::RemoveDirectory(const string& dirPath, bool mayNonExist)
{
    assert(!IsExist(dirPath));
    assert(mayNonExist);
}

size_t PackageStorage::EstimateFileLockMemoryUse(const string& filePath, FSOpenType type)
{
    assert(false);
    return 0u;
}

bool PackageStorage::IsExist(const string& path) const
{
    const string& relativePath = GetRelativePath(path);
    ScopedLock lock(mLock);
    int32_t unitId = MatchUnit(relativePath);
    if (unitId < 0)
    {
        return false;
    }
    if (relativePath == mUnits[unitId].rootPath)
    {
        return true;
    }
    return mUnits[unitId].metas.find(relativePath) != mUnits[unitId].metas.end();
}

void PackageStorage::ListFile(
    const string& dirPath, fslib::FileList& fileList, 
    bool recursive, bool physicalPath, bool skipSecondaryRoot) const
{
    assert(false);
}

void PackageStorage::Close()
{
}

void PackageStorage::StoreFile(const FileNodePtr& fileNode, const FSDumpParam& param)
{
    if (FSFT_IN_MEM != fileNode->GetType())
    {
        INDEXLIB_FATAL_ERROR(
            UnSupported, "Package storage not support store [%d] file [%s]",
            fileNode->GetType(), fileNode->GetPath().c_str());
    }
    uint32_t physicalFileId = (uint32_t)-1;
    BufferedFileOutputStreamPtr stream;
    int32_t unitId = MatchUnit(GetRelativePath(fileNode->GetPath()));
    {
        ScopedLock lock(mLock);
        auto ret = DoAllocatePhysicalStream(mUnits[unitId], fileNode->GetPath());
        physicalFileId = ret.first;
        stream = ret.second;
    }
    stream->Write(fileNode->GetBaseAddress(), fileNode->GetLength());
    StoreLogicalFile(unitId, fileNode->GetPath(), physicalFileId, fileNode->GetLength());
}

void PackageStorage::RemoveFile(const string& filePath, bool mayNonExist)
{
    assert(!IsExist(filePath));
    assert(mayNonExist);
}

bool PackageStorage::IsExistOnDisk(const string& path) const
{
    assert(false);
    return false;
}

void PackageStorage::MakeRoot(const string& rootPath)
{
    const string& relativePath = GetRelativePath(rootPath);
    IE_LOG(DEBUG, "Add root path [%s]", rootPath.c_str());
    ScopedLock lock(mLock);
    int32_t unitId = MatchUnit(relativePath);
    if (unitId >= 0)
    {
        if (rootPath.size() > mUnits[unitId].rootPath.size())
        {
            IE_LOG(INFO, "New root path [%s] match existing root path [%s]",
               rootPath.c_str(), mUnits[unitId].rootPath.c_str());
        }
        else
        {
            INDEXLIB_FATAL_ERROR(Exist, "Root path [%s] match with existing root path [%s]",
                                 rootPath.c_str(), mUnits[unitId].rootPath.c_str());
        }
    }
    Unit unit(relativePath);
    mUnits.push_back(unit);
}

void PackageStorage::Recover(const std::string& rootPath, int32_t recoverMetaId,
                             const fslib::FileList& fileListInRoot)
{
    IE_LOG(INFO, "recover package storage[%s], metaid[%d] in [%s]",
           rootPath.c_str(), recoverMetaId, mDescription.c_str());
    const std::string& relativePath = GetRelativePath(rootPath);
    ScopedLock lock(mLock);
    uint32_t unitId = MatchUnit(relativePath);
    Unit& unit = mUnits[unitId];
    set<string> dataFileSet;
    set<string> uselessFileSet;
    string recoverMetaPath;
    VersionedPackageFileMeta::Recognize(mDescription, recoverMetaId, fileListInRoot,
            dataFileSet, uselessFileSet, recoverMetaPath);
    IE_LOG(INFO, "recognize recover info from [%s], find [%lu] data file, [%lu] useless file",
           recoverMetaPath.c_str(), dataFileSet.size(), uselessFileSet.size());
    // clean all files when recoverMetaId < 0
    if (recoverMetaId < 0)
    {
        uselessFileSet.insert(dataFileSet.begin(), dataFileSet.end());
        if (!recoverMetaPath.empty())
        {
            uselessFileSet.insert(recoverMetaPath);
        }
        CleanData(unit, uselessFileSet);
        return;
    }
    // no matched package file meta 
    if (recoverMetaPath.empty())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "recover package file meta failed, "
                             "can not find meta [%s] with metaId [%d] in [%s]",
                             mDescription.c_str(), recoverMetaId, rootPath.c_str());
    }
    // has matched package file meta
    VersionedPackageFileMeta packageMeta;
    packageMeta.Load(PathUtil::JoinPath(rootPath, recoverMetaPath));
    for (auto iter = packageMeta.Begin(); iter != packageMeta.End(); iter++)
    {
        unit.metas.insert(make_pair(unit.rootPath + "/" + iter->GetFilePath(), *iter));
    }
    //for (const string& fileName : packageMeta.GetPhysicalFileNames())
    const vector<string>& fileNames = packageMeta.GetPhysicalFileNames();
    const vector<string>& fileTags = packageMeta.GetPhysicalFileTags();
    assert(fileNames.size() == fileTags.size());
    for (size_t i = 0; i < fileNames.size(); ++i)
    {
        unit.physicalStreams.push_back(make_tuple(fileNames[i], BufferedFileOutputStreamPtr(), fileTags[i]));
    }
    unit.nextMetaId = packageMeta.GetVersionId() + 1;
    
    set<string> fileNameSet(fileNames.begin(), fileNames.end());
    set_difference(dataFileSet.begin(), dataFileSet.end(),
                   fileNameSet.begin(), fileNameSet.end(),
                   inserter(uselessFileSet, uselessFileSet.end()));
    CleanData(unit, uselessFileSet);
}

void PackageStorage::CleanData(const Unit& unit, const set<string>& fileNames)
{
    IE_LOG(INFO, "recover prepare to remove [%lu] files: [%s]", fileNames.size(),
           StringUtil::toString(fileNames.begin(), fileNames.end()).c_str());
    string absPath = JoinRoot(unit.rootPath);
    for (const auto& iter : fileNames)
    {
        FileSystemWrapper::DeleteIfExist(PathUtil::JoinPath(absPath, iter));
    }
}

pair<uint32_t, BufferedFileOutputStreamPtr> PackageStorage::AllocatePhysicalStream(
    int32_t unitId, const string& logicalFilePath)
{
    ScopedLock lock(mLock);
    assert(unitId >= 0 && (size_t)unitId < mUnits.size());
    return DoAllocatePhysicalStream(mUnits[unitId], logicalFilePath);
}

pair<uint32_t, BufferedFileOutputStreamPtr> PackageStorage::DoAllocatePhysicalStream(
        Unit& unit, const std::string& logicalFilePath)
{
    string tag = ExtractTag(logicalFilePath);
    auto freeIt = unit.freePhysicalFileIds.find(tag);
    if (freeIt == unit.freePhysicalFileIds.end())
    {
        freeIt = unit.freePhysicalFileIds.insert(freeIt, make_pair(tag, queue<uint32_t>()));
    }
    if (freeIt->second.empty())
    {
        BufferedFileOutputStreamPtr stream(new BufferedFileOutputStream(mOptions.raidConfig));
        uint32_t fileId = unit.physicalStreams.size();
        string physicalFileName =
            VersionedPackageFileMeta::GetPackageDataFileName(mDescription, fileId);
        string physicalFilePath = PathUtil::JoinPath(JoinRoot(unit.rootPath), physicalFileName);
        IE_LOG(INFO, "create file[%s]", physicalFilePath.c_str());
        stream->Open(physicalFilePath);
        unit.physicalStreams.push_back(make_tuple(physicalFileName, stream, tag));
        return make_pair(fileId, std::get<1>(unit.physicalStreams.back()));
    }
    uint32_t fileId = freeIt->second.front();
    assert(fileId < unit.physicalStreams.size());
    const auto& stream = std::get<1>(unit.physicalStreams[fileId]);
    freeIt->second.pop();
    size_t paddingLen = ((stream->GetLength() + mFileAlignSize - 1) & (~ (mFileAlignSize - 1))) - stream->GetLength();
    vector<char> paddingBuffer(paddingLen, 0);
    stream->Write(paddingBuffer.data(), paddingLen);
    return make_pair(fileId, stream);
}

void PackageStorage::StoreLogicalFile(
    int32_t unitId, const std::string& logicalFilePath, storage::FileBuffer* buffer)
{
    ScopedLock lock(mLock);
    mIsSynced = false;
    mUnits[unitId].unflushedBuffers.push_back(
        UnflushedFile{logicalFilePath, buffer});
}

void PackageStorage::StoreLogicalFile(
    int32_t unitId, const string& logicalFilePath,
    uint32_t physicalFileId, size_t fileLength)
{
    string logicalRelativePath = GetRelativePath(logicalFilePath); 
    ScopedLock lock(mLock);
    // eg:            /partition_0_65535/segment_2_level_1/index
    // unit.rootPath: /partition_0_65535/segment_2_level_1
    // after substr:  index
    Unit& unit = mUnits[unitId];
    PackageFileMeta::InnerFileMeta meta(
        logicalRelativePath.substr(unit.rootPath.size() + 1), false);
    if (physicalFileId >= unit.physicalStreams.size())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "store logicalFile [%s] failed",
                             logicalFilePath.c_str());
    }
    const auto& stream = std::get<1>(unit.physicalStreams[physicalFileId]);
    const std::string& tag = std::get<2>(unit.physicalStreams[physicalFileId]);
    assert(stream->GetLength() >= fileLength);
    size_t offset = stream->GetLength() - fileLength;
    meta.Init(offset, fileLength, physicalFileId);
    unit.metas.insert(make_pair(logicalRelativePath, meta));
    unit.freePhysicalFileIds[tag].push(physicalFileId);
}

bool PackageStorage::Sync()
{
    ScopedLock lock(mLock);
    if (mIsSynced)
    {
        return true;
    }
    for (uint32_t unitId = 0; unitId < mUnits.size(); ++unitId)
    {
        Unit& unit = mUnits[unitId];
        for (auto& unflushedFile : unit.unflushedBuffers)
        {
            auto ret = DoAllocatePhysicalStream(unit, unflushedFile.logicalFilePath);

            uint32_t physicalFileId = ret.first;
            const auto& stream = ret.second;
            storage::FileBuffer* buffer = unflushedFile.buffer;
            stream->Write(buffer->GetBaseAddr(), buffer->GetCursor());
            StoreLogicalFile(unitId, unflushedFile.logicalFilePath,
                             physicalFileId, buffer->GetCursor());
            DELETE_AND_SET_NULL(unflushedFile.buffer);
        }
        unit.unflushedBuffers.clear();
    }
    mIsSynced = true;
    return true;
}

// need use DirectoryMerger::MergePackageFiles mv to final package file meta
int32_t PackageStorage::Commit()
{
    Sync();
    int32_t metaId = 0;
    ScopedLock lock(mLock);
    for (const Unit& unit : mUnits)
    {
        metaId = max(metaId, unit.nextMetaId);
    }
    for (uint32_t unitId = 0; unitId < mUnits.size(); ++unitId)
    {
        Unit& unit = mUnits[unitId];
        unit.nextMetaId = metaId + 1;
        VersionedPackageFileMeta packageFileMeta(metaId);
        packageFileMeta.SetFileAlignSize(mFileAlignSize);
        for (auto& tuple : unit.physicalStreams)
        {
            string& fileName = std::get<0>(tuple);
            auto& stream = std::get<1>(tuple);
            string& fileTag = std::get<2>(tuple);
            if (stream)
            {
                stream->Close();
                stream.reset();
            }
            packageFileMeta.AddPhysicalFile(fileName, fileTag);
        }
        unit.freePhysicalFileIds.clear();
        assert(unit.unflushedBuffers.empty());
        
        for (const auto& iter : unit.metas)
        {
            packageFileMeta.AddInnerFile(iter.second);
        }
        packageFileMeta.Store(JoinRoot(unit.rootPath), mDescription);
        // OPTIMIZE: remove old meta version
    }
    IE_LOG(INFO, "commit [%lu] unit, metaId [%d], description [%s]",
           mUnits.size(), metaId, mDescription.c_str());
    return metaId;
}

void PackageStorage::AssertDescription(const string& description)
{
    if (mDescription != description)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "description [%s] not equal to previous description [%s]",
                             description.c_str(), mDescription.c_str());
    }
}

std::string PackageStorage::ExtractTag(const string& logicalFilePath) const
{
    if (mTagFunction)
    {
        assert(logicalFilePath.compare(0, mRootPath.size(), mRootPath) == 0);
        string relativePath = logicalFilePath.substr(mRootPath.size() + 1);
        return mTagFunction(relativePath);
    }
    return "";
}

IE_NAMESPACE_END(file_system);


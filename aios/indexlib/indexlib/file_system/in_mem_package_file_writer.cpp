#include "indexlib/file_system/in_mem_package_file_writer.h"
#include "indexlib/file_system/in_mem_file_writer.h"
#include "indexlib/file_system/package_file_flush_operation.h"
#include "indexlib/file_system/directory_file_node_creator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
using namespace fslib;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemPackageFileWriter);

InMemPackageFileWriter::InMemPackageFileWriter(
        const string& filePath, InMemStorage* inMemStorage,
        const util::BlockMemoryQuotaControllerPtr& memController)
    : PackageFileWriter(filePath)
    , mInMemStorage(inMemStorage)
    , mMemController(memController)
{
}

InMemPackageFileWriter::~InMemPackageFileWriter() 
{
    if (!mIsClosed && !mInnerFileWriters.empty())
    {
        IE_LOG(ERROR, "InMemPackageFileWriter Destruct before Close");
    }
}

FileWriterPtr InMemPackageFileWriter::CreateInnerFileWriter(
    const std::string& filePath, const FSWriterParam& param)
{
    ScopedLock scopeLock(mLock);

    string basePath = PathUtil::GetParentDirPath(mFilePath);
    string absPath = PathUtil::JoinPath(basePath, filePath);
    string normPath = PathUtil::NormalizePath(absPath);

    IE_LOG(DEBUG, "InnerFilePath:%s", normPath.c_str());

    InnerFileWriterMap::const_iterator iter = mInnerFileWriters.find(normPath);
    if (iter != mInnerFileWriters.end())
    {
        INDEXLIB_FATAL_ERROR(Exist, "InnerFileWriter for [%s] already exist!",
                             normPath.c_str());
        return FileWriterPtr();
    }

    string parentDirPath = PathUtil::GetParentDirPath(normPath);
    AddDirIfNotExist(parentDirPath);
    FileWriterPtr fileWriter(new InMemFileWriter(mMemController, this, param));
    fileWriter->Open(normPath);
    mInnerFileWriters[normPath] = fileWriter;
    return fileWriter;
}

void InMemPackageFileWriter::MakeInnerDir(const string& dirPath)
{
    ScopedLock scopeLock(mLock);

    string basePath = PathUtil::GetParentDirPath(mFilePath);
    string absPath = PathUtil::JoinPath(basePath, dirPath);
    string normPath = PathUtil::NormalizePath(absPath);

    AddDirIfNotExist(normPath);
}

void InMemPackageFileWriter::DoClose()
{
    if (mInnerFileWriters.size() != mInnerFileNodes.size())
    {
        INDEXLIB_FATAL_ERROR(
                InconsistentState, 
                "All InnerFileWriter should Close before PackageFileWriter!");
        return;
    }

    // add innerFileNodes to fileNodeCache
    for (size_t i = 0; i < mInnerFileNodes.size(); i++)
    {
        mInMemStorage->StoreWhenNonExist(mInnerFileNodes[i]);
    }

    vector<FileNodePtr> innerFiles;
    vector<FSDumpParam> dumpParamVec;
    CollectInnerFileAndDumpParam(innerFiles, dumpParamVec);
    
    // mount package file meta
    PackageFileMetaPtr packFileMeta = GeneratePackageFileMeta(
            mFilePath, mInnerDirs, innerFiles);
    mInMemStorage->MountPackageFile(mFilePath, packFileMeta);

    if (mInMemStorage->NeedFlush())
    {
        // add flush package file operation
        FileFlushOperationPtr flushOperation(new PackageFileFlushOperation(mFilePath, packFileMeta,
            innerFiles, dumpParamVec, mInMemStorage->GetOptions().raidConfig));
        mInMemStorage->AddFlushOperation(flushOperation);
    }

    mInnerInMemFileMap.clear();
    mInnerFileNodes.clear();
    mInnerFileWriters.clear();
    mInnerDirs.clear();
}

void InMemPackageFileWriter::GetPhysicalFiles(vector<string>& filePaths) const
{
    filePaths.push_back(mFilePath + PACKAGE_FILE_META_SUFFIX);
    filePaths.push_back(mFilePath + PACKAGE_FILE_DATA_SUFFIX + "0");
}

void InMemPackageFileWriter::StoreFile(const FileNodePtr& fileNode)
{
    ScopedLock scopeLock(mLock);

    fileNode->SetInPackage(true);
    mInnerFileNodes.push_back(fileNode);
}

void InMemPackageFileWriter::StoreInMemoryFile(
        const InMemFileNodePtr& inMemFileNode,
        const FSDumpParam& dumpParam)
{
    ScopedLock scopeLock(mLock);
    
    inMemFileNode->SetInPackage(true);
    mInnerInMemFileMap[inMemFileNode->GetPath()] =
        make_pair(inMemFileNode, dumpParam);
}

void InMemPackageFileWriter::AddDirIfNotExist(const string& absDirPath)
{
    string basePath = PathUtil::GetParentDirPath(mFilePath);
    if (absDirPath == basePath)
    {
        return;
    }

    string normalBaseDirPath = FileSystemWrapper::NormalizeDir(basePath);

    assert(absDirPath.find(normalBaseDirPath) == 0);
    string relativePath = absDirPath.substr(normalBaseDirPath.length());

    while (!relativePath.empty())
    {
        string path = PathUtil::JoinPath(basePath, relativePath);
        mInnerDirs.insert(path);
        relativePath = PathUtil::GetParentDirPath(relativePath);
    }

    assert(mInMemStorage);
    if (!mInMemStorage->IsExist(absDirPath))
    {
        mInMemStorage->MakeDirectoryRecursive(absDirPath, false);
    }
}

PackageFileMetaPtr InMemPackageFileWriter::GeneratePackageFileMeta(
        const string& packageFilePath,
        const set<string>& innerDirs,
        const vector<FileNodePtr>& innerFiles)
{
    vector<FileNodePtr> fileNodes;
    fileNodes.reserve(innerDirs.size() + innerFiles.size());

    DirectoryFileNodeCreator dirNodeCreator;

    // add inner dir file nodes
    set<string>::const_iterator iter = innerDirs.begin();
    for ( ; iter != innerDirs.end(); iter++)
    {
        const string& absDirPath = *iter;
        fileNodes.push_back(dirNodeCreator.CreateFileNode(
                                absDirPath, FSOT_UNKNOWN, true));
    }

    // add inner file nodes
    fileNodes.insert(fileNodes.end(), 
                     innerFiles.begin(), innerFiles.end());

    PackageFileMetaPtr fileMeta(new PackageFileMeta);
    size_t pageSize = getpagesize();
    fileMeta->InitFromFileNode(packageFilePath, fileNodes, pageSize);
    return fileMeta;
}

void InMemPackageFileWriter::GetDumpParamForFileNodes(
    const vector<FileNodePtr>& fileNodeVec, vector<FSDumpParam>& dumpParamVec)
{
    for (size_t i = 0; i < fileNodeVec.size(); ++i)
    {
        const string& filePath = fileNodeVec[i]->GetPath();
        InnerFileWriterMap::const_iterator iter = mInnerFileWriters.find(filePath);
        if (iter == mInnerFileWriters.end())
        {
            INDEXLIB_FATAL_ERROR(NonExist, "InnerFileWriter for [%s] do not exist!",
                                 filePath.c_str());
        }
        dumpParamVec.push_back(iter->second->GetWriterParam());
        IE_LOG(DEBUG, "%s param: isAtomic[%d]", filePath.c_str(),
               iter->second->GetWriterParam().atomicDump);
    }

}

void InMemPackageFileWriter::CollectInnerFileAndDumpParam(
        vector<FileNodePtr>& fileNodeVec, vector<FSDumpParam>& dumpParamVec)
{
    assert(fileNodeVec.empty());
    assert(dumpParamVec.empty());
    fileNodeVec.reserve(mInnerFileNodes.size() + mInnerInMemFileMap.size());
    dumpParamVec.reserve(mInnerFileNodes.size() + mInnerInMemFileMap.size());
    
    fileNodeVec.insert(fileNodeVec.end(),
                       mInnerFileNodes.begin(), mInnerFileNodes.end());
    GetDumpParamForFileNodes(mInnerFileNodes, dumpParamVec);

    InnerInMemFileMap::const_iterator iter = mInnerInMemFileMap.begin();
    for (; iter != mInnerInMemFileMap.end(); iter++)
    {
        fileNodeVec.push_back(iter->second.first);
        dumpParamVec.push_back(iter->second.second);
    }
}

IE_NAMESPACE_END(file_system);


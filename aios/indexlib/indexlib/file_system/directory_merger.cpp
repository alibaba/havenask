#include <autil/StringUtil.h>
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/directory_merger.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/versioned_package_file_meta.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/package_file_meta.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DirectoryMerger);

DirectoryMerger::DirectoryMerger() 
{
}

DirectoryMerger::~DirectoryMerger() 
{
}

void DirectoryMerger::MoveFiles(const FileList &files, const string &targetDir)
{
    // opt for packagefile
    for (size_t i = 0; i < files.size(); i++)
    {
        string folder, fileName;
        SplitFileName(files[i], folder, fileName);
        if (FileSystemWrapper::IsDir(files[i]))
        {
            string dest = FileSystemWrapper::JoinPath(targetDir, fileName);
            MkDirIfNotExist(dest);
            FileList subFiles;
            FileSystemWrapper::ListDir(files[i], subFiles);
            for (size_t j = 0; j < subFiles.size(); j++)
            {
                string subSrc = FileSystemWrapper::JoinPath(files[i], subFiles[j]);
                string subDest = FileSystemWrapper::JoinPath(dest, subFiles[j]);
                try
                {
                    FileSystemWrapper::Rename(subSrc, subDest);
                }
                catch (misc::ExistException& e)
                {
                    if (!FileSystemWrapper::IsDir(subSrc))
                    {
                        IE_LOG(ERROR, "rename file [%s] to exist path [%s]",
                                subSrc.c_str(), subDest.c_str());
                        throw;
                    }
                    IE_LOG(WARN, "rename directory [%s] to exist path [%s]",
                            subSrc.c_str(), subDest.c_str());
                    FileList subFileList = ListFiles(files[i], subFiles[j]);
                    MoveFiles(subFileList, subDest);
                }
                catch (...)
                {
                    IE_LOG(ERROR, "rename path [%s] to [%s] failed", subSrc.c_str(), subDest.c_str());
                    throw;
                }
                IE_LOG(INFO, "rename path [%s] to [%s] OK", subSrc.c_str(), subDest.c_str());
            }
        }
        else
        {
            const string& srcFile = files[i];
            const string& dstFile = FileSystemWrapper::JoinPath(targetDir, fileName);
            FileSystemWrapper::Rename(srcFile, dstFile);
            IE_LOG(INFO, "rename file [%s] to path [%s]", srcFile.c_str(), dstFile.c_str());
        }
    }
}

void DirectoryMerger::SplitFileName(const string &input, string &folder, string &fileName)
{
    size_t found;
    found = input.find_last_of("/\\");
    if (found == string::npos) {
        fileName = input;
        return;
    }
    folder = input.substr(0,found);
    fileName = input.substr(found+1);
}

void DirectoryMerger::MkDirIfNotExist(const string &dir)
{
    if (!FileSystemWrapper::IsExist(dir))
    {
        FileSystemWrapper::MkDir(dir, true);
    }
}

FileList DirectoryMerger::ListFiles(const string &dir, const string &subDir)
{
    FileList fileList;
    string dirPath = FileSystemWrapper::JoinPath(dir, subDir);
    try
    {
        FileSystemWrapper::ListDir(dirPath, fileList);
    }
    catch (misc::NonExistException& e)
    {
        return fileList;
    }
    catch (...)
    {
        throw;
    }
    
    for (size_t i = 0; i < fileList.size(); i++)
    {
        fileList[i] = FileSystemWrapper::JoinPath(dirPath, fileList[i]);
    }
    return fileList;
}

/////////////////////

bool DirectoryMerger::MergePackageFiles(const string& dir)
{
    FileList metaFileNames;
    if (!CollectPackageMetaFiles(dir, metaFileNames))
    {
        // no need merge, may be recover
        CleanMetaFiles(dir, metaFileNames);
        return true;
    }
    if (metaFileNames.empty())
    {
        // non package segment
        return false;
    }

    sort(metaFileNames.begin(), metaFileNames.end());
    MergePackageMeta(dir, metaFileNames);
    CleanMetaFiles(dir, metaFileNames);
    return true;
}

void DirectoryMerger::CleanMetaFiles(const string& dir, const FileList& metaFileNames)
{
    for (const string& metaFileName : metaFileNames)
    {
        FileSystemWrapper::DeleteIfExist(PathUtil::JoinPath(dir, metaFileName));
    }    
}

bool DirectoryMerger::CollectPackageMetaFiles(const string& dir, FileList& metaFileNames)
{
    FileList allFileNames;
    FileSystemWrapper::ListDir(dir, allFileNames);
    
    string packageMetaPrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX;
    map<string, pair<int32_t, string>> descriptionToMeta;
    bool ret = true;
    for (const string& fileName : allFileNames)
    {
        if (!StringUtil::startsWith(fileName, packageMetaPrefix))
        {
            continue;
        }
        // eg: package_file.__meta__
        if (fileName.size() == packageMetaPrefix.size())
        {
            assert(fileName == string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX);
            IE_LOG(INFO, "final package file meta [%s] already exist", fileName.c_str());
            ret = false;
            continue;
        }
        // eg: package_file.__meta__.i0t0.0
        int32_t versionId = VersionedPackageFileMeta::GetVersionId(fileName);
        if (versionId < 0)
        {
            // eg: package_file.__meta__.i0t0.0.__tmp__
            FileSystemWrapper::DeleteFile(PathUtil::JoinPath(dir, fileName));
            continue;
        }
        string description = VersionedPackageFileMeta::GetDescription(fileName);
        auto iter = descriptionToMeta.find(description);
        if (iter == descriptionToMeta.end())
        {
            descriptionToMeta[description] = make_pair(versionId, fileName);
        }
        else if (versionId > iter->second.first)
        {
            FileSystemWrapper::DeleteFile(PathUtil::JoinPath(dir, iter->second.second));
            descriptionToMeta[description] = make_pair(versionId, fileName);
        }
        else
        {
            assert(versionId < iter->second.first);
            FileSystemWrapper::DeleteFile(PathUtil::JoinPath(dir, fileName));
        }
    }
    
    for (auto it = descriptionToMeta.begin(); it != descriptionToMeta.end(); ++it)
    {
        metaFileNames.push_back(it->second.second);
    }
    return ret;
}

void DirectoryMerger::MergePackageMeta(const string& dir, const FileList& metaFileNames)
{
    PackageFileMeta mergedMeta;
    set<PackageFileMeta::InnerFileMeta> dedupInnerFileMetas;
    uint32_t baseFileId = 0;
    for (const string& metaFileName : metaFileNames)
    {
        VersionedPackageFileMeta meta;
        meta.Load(PathUtil::JoinPath(dir, metaFileName));
        for (auto it = meta.Begin(); it != meta.End(); ++it)
        {
            auto tempIt = dedupInnerFileMetas.find(it->GetFilePath());
            if (tempIt == dedupInnerFileMetas.end())
            {
                PackageFileMeta::InnerFileMeta newMeta(*it);
                newMeta.SetDataFileIdx(newMeta.IsDir() ? 0 : newMeta.GetDataFileIdx() + baseFileId);
                dedupInnerFileMetas.insert(newMeta);
            }
            else if (!it->IsDir() || !tempIt->IsDir())
            {
                INDEXLIB_FATAL_ERROR(InconsistentState, "repeated package data file [%s], [%d,%d]",
                        it->GetFilePath().c_str(), it->IsDir(), tempIt->IsDir());
            }
        }
        if (meta.GetFileAlignSize() != mergedMeta.GetFileAlignSize())
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                                 "inconsistent file align size %lu != %lu",
                                 meta.GetFileAlignSize(), mergedMeta.GetFileAlignSize());
        }
        vector<string> newDataFileNames;
        const vector<string>& dataFileNames = meta.GetPhysicalFileNames();
        const vector<string>& dataFileTags = meta.GetPhysicalFileTags();
        MovePackageDataFile(baseFileId, dir, dataFileNames, dataFileTags, newDataFileNames);
        mergedMeta.AddPhysicalFiles(newDataFileNames, meta.GetPhysicalFileTags());
        baseFileId += dataFileNames.size();
    }
    for (PackageFileMeta::InnerFileMeta innerFile : dedupInnerFileMetas)
    {
        mergedMeta.AddInnerFile(innerFile);
    }

    StorePackageFileMeta(dir, mergedMeta);
}

void DirectoryMerger::MovePackageDataFile(
    uint32_t baseFileId, const string& dir, const vector<string>& srcFileNames,
    const vector<string>& srcFileTags, vector<string>& newFileNames)
{
    assert(srcFileTags.size() == srcFileNames.size());
    for (size_t i = 0; i < srcFileNames.size(); ++i)
    {
        string newFileName = PackageFileMeta::GetPackageFileDataPath(
            PACKAGE_FILE_PREFIX, srcFileTags[i], baseFileId + i);
        newFileNames.push_back(newFileName);
        string srcFilePath = PathUtil::JoinPath(dir, srcFileNames[i]);
        string dstFilePath = PathUtil::JoinPath(dir, newFileName);
        try
        {
            FileSystemWrapper::Rename(srcFilePath, dstFilePath);
        }
        catch (misc::NonExistException& e)
        {
            if (!FileSystemWrapper::IsExist(dstFilePath))
            {
                INDEXLIB_FATAL_ERROR(NonExist, "move file [%s] to [%s]",
                                     srcFilePath.c_str(), dstFilePath.c_str());
            }
        }
        catch (misc::ExistException& e)
        {
            if (!FileSystemWrapper::IsExist(srcFilePath))
            {
                INDEXLIB_FATAL_ERROR(NonExist, "move file [%s] to [%s]",
                                     srcFilePath.c_str(), dstFilePath.c_str());
            }
        }
        catch (...)
        {
            IE_LOG(ERROR, "rename package data file [%s] to [%s] failed",
                   srcFilePath.c_str(), dstFilePath.c_str());
            throw;
        }
        IE_LOG(INFO, "rename package data file [%s] to [%s]",
               srcFilePath.c_str(), dstFilePath.c_str());
    }
}

void DirectoryMerger::StorePackageFileMeta(const string& dir, PackageFileMeta& mergedMeta)
{
    if (mergedMeta.GetPhysicalFileNames().empty() && mergedMeta.GetInnerFileCount() > 0)
    {
        // touch a empty file
        string emptyDataFileName = PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, "", 0);
        string emptyDataFilePath = PathUtil::JoinPath(dir, emptyDataFileName);
        FileSystemWrapper::Store(emptyDataFilePath, "", true);
        mergedMeta.AddPhysicalFile(emptyDataFileName, "");
    }

    try
    {
        mergedMeta.Store(dir);
    }
    catch (misc::ExistException& e)
    {
        // normal
    }
    catch (...)
    {
        throw;
    }
}

IE_NAMESPACE_END(file_system);


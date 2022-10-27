#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentFileListWrapper);


bool SegmentFileListWrapper::Load(const std::string& path, IndexFileList& meta)
{
    string filePath = PathUtil::JoinPath(path, SEGMENT_FILE_LIST);
    string legacyFilePath = PathUtil::JoinPath(path, DEPLOY_INDEX_FILE_NAME);
    if (!meta.Load(filePath) && !meta.Load(legacyFilePath))
    {
        IE_LOG(ERROR, "[segment_file_list] and [deploy_index] all load fail in directory [%s]",
               path.c_str());
        return false;
    }
    return true;
}

bool SegmentFileListWrapper::Load(const DirectoryPtr& directory, IndexFileList& meta)
{
    string content;
    if (!directory)
    {
        return false;
    }
    string filePath = SEGMENT_FILE_LIST;
    string legacyFilePath = DEPLOY_INDEX_FILE_NAME;
    if (!meta.Load(directory, filePath) && !meta.Load(directory, legacyFilePath))
    {
        IE_LOG(WARN, "[segment_file_list] and [deploy_index] all load fail in directory [%s]",
               directory->GetPath().c_str());
        return false;
    }
    return true;
}


bool SegmentFileListWrapper::Dump(const std::string& path, const std::string& lifecycle)
{
    
    FileList fileList;
    string legacyIndexFile = FileSystemWrapper::JoinPath(path, DEPLOY_INDEX_FILE_NAME);
    string deployIndexFile = FileSystemWrapper::JoinPath(path, SEGMENT_FILE_LIST);
    FileSystemWrapper::DeleteIfExist(legacyIndexFile);
    FileSystemWrapper::DeleteIfExist(deployIndexFile);
    try
    {
        FileSystemWrapper::ListDirRecursive(path, fileList);
    }
    catch (const misc::NonExistException &e)
    {
        IE_LOG(ERROR, "catch NonExist exception: %s", e.what());
        return false;
    }
    catch (const autil::legacy::ExceptionBase &e)
    {
        IE_LOG(ERROR, "Catch exception: %s", e.what());
        throw e;
    }
    catch (...)
    {
        IE_LOG(ERROR, "catch exception when ListDirRecursive");
        throw;
    }
    return Dump(path, fileList, lifecycle);
}

bool SegmentFileListWrapper::Dump(const std::string& path,
                                  const fslib::FileList& fileList,
                                  const std::string& lifecycle)
{    
    IndexFileList dpFileMetas;
    dpFileMetas.lifecycle = lifecycle;
    FileList::const_iterator it = fileList.begin();
    for (; it != fileList.end(); it++)
    {
        const string& fileName = *it;
        if (*fileName.rbegin() == '/')
        {
            dpFileMetas.Append(FileInfo(fileName));
        }
        else
        {
            string filePath = FileSystemWrapper::JoinPath(path, fileName);
            fslib::FileMeta fileMeta;
            FileSystemWrapper::GetFileMeta(filePath, fileMeta);
            dpFileMetas.Append(
                FileInfo(fileName, fileMeta.fileLength, fileMeta.lastModifyTime));
        }
    }
    string content = dpFileMetas.ToString();
    string legacyIndexFile = FileSystemWrapper::JoinPath(path, DEPLOY_INDEX_FILE_NAME);
    string deployIndexFile = FileSystemWrapper::JoinPath(path, SEGMENT_FILE_LIST);
    FileSystemWrapper::AtomicStore(deployIndexFile, content.c_str());
    if (needDumpLegacyFile())
    {
        FileSystemWrapper::AtomicStore(legacyIndexFile, content.c_str());
    }
    return true;
}

void SegmentFileListWrapper::Dump(const file_system::DirectoryPtr& directory,
                                  const SegmentInfoPtr& segmentInfo,
                                  const std::string& lifecycle)
{
    assert(directory);
    FileList fileList;
    directory->RemoveFile(DEPLOY_INDEX_FILE_NAME, true);
    directory->RemoveFile(SEGMENT_FILE_LIST, true);
    directory->ListFile("", fileList, true, true);
    Dump(directory, fileList, segmentInfo, lifecycle);
}

void SegmentFileListWrapper::Dump(const DirectoryPtr& directory, const fslib::FileList& fileList,
                                  const SegmentInfoPtr& segmentInfo,
                                  const std::string& lifecycle)
{
    assert(directory);
    IndexFileList dpIndexMeta;
    dpIndexMeta.lifecycle = lifecycle;
    int64_t segmentInfoLen = -1;
    FileList::const_iterator it = fileList.begin();
    for (; it != fileList.end(); it++)
    {
        const string& path = *it;
        if ((!directory->IsExistInPackageMountTable(path) && !directory->IsExist(path)) ||
            directory->IsDir(path))
        {
            dpIndexMeta.Append(FileInfo(path));
        }
        else
        {
            fslib::FileMeta fileMeta;
            directory->GetFileMeta(path, fileMeta);
            dpIndexMeta.Append(
                FileInfo(path, fileMeta.fileLength, fileMeta.lastModifyTime));
            if (path == SEGMENT_INFO_FILE_NAME)
            {
                segmentInfoLen = fileMeta.fileLength;
            }
        }
    }

    if (segmentInfo)
    {
        // add user set segmentInfo to dp meta, if user set null do nothing
        if (segmentInfoLen == -1)
        {
            dpIndexMeta.Append(FileInfo(SEGMENT_INFO_FILE_NAME,
                            segmentInfo->ToString().size(), (uint64_t)-1));
        }
        else if (segmentInfoLen != (int64_t)segmentInfo->ToString().size())
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "segment file length [%ld] not match expect length [%lu]",
                    segmentInfoLen, segmentInfo->ToString().size());
        }
    }

    string fileContent = dpIndexMeta.ToString();
    directory->Store(SEGMENT_FILE_LIST, fileContent);
    if (needDumpLegacyFile())
    {
        directory->Store(DEPLOY_INDEX_FILE_NAME, fileContent);
    }
}

bool SegmentFileListWrapper::IsExist(const std::string& path)
{
    string legacyIndexFile = FileSystemWrapper::JoinPath(path, DEPLOY_INDEX_FILE_NAME);
    string deployIndexFile = FileSystemWrapper::JoinPath(path, SEGMENT_FILE_LIST);
    return FileSystemWrapper::IsExist(deployIndexFile) || FileSystemWrapper::IsExist(legacyIndexFile);
}

bool SegmentFileListWrapper::needDumpLegacyFile()
{
    return EnvUtil::GetEnv("INDEXLIB_COMPATIBLE_DEPLOY_INDEX_FILE", true);
}



IE_NAMESPACE_END(index_base);


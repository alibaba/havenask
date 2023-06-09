/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"

#include <string>
#include <vector>

#include "autil/EnvUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/DeleteOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/RetryUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentFileListWrapper);

namespace {

void ConvertFileListToFileInfos(const DirectoryPtr& directory, const std::vector<std::string>& fileList,
                                std::vector<FileInfo>& fileInfos)
{
    fileInfos.reserve(fileList.size());
    for (auto it = fileList.begin(); it != fileList.end(); it++) {
        auto& fileName = *it;
        if (*fileName.rbegin() == '/') {
            fileInfos.emplace_back(fileName);
        } else {
            fileInfos.emplace_back(fileName, directory->GetFileLength(fileName));
        }
    }
}

} // namespace

bool SegmentFileListWrapper::Load(const DirectoryPtr& directory, IndexFileList& meta)
{
    string content;
    if (!directory) {
        return false;
    }
    string filePath = SEGMENT_FILE_LIST;
    string legacyFilePath = DEPLOY_INDEX_FILE_NAME;
    if (!meta.Load(directory->GetIDirectory(), filePath) && !meta.Load(directory->GetIDirectory(), legacyFilePath)) {
        IE_LOG(WARN, "[segment_file_list] and [deploy_index] all load fail in directory [%s]",
               directory->DebugString().c_str());
        return false;
    }
    return true;
}

bool SegmentFileListWrapper::TEST_Dump(const std::string& path, const fslib::FileList& fileList)
{
    IndexFileList dpFileMetas;
    FileList::const_iterator it = fileList.begin();
    for (; it != fileList.end(); it++) {
        const string& fileName = *it;
        if (*fileName.rbegin() == '/') {
            dpFileMetas.Append(FileInfo(fileName));
        } else {
            string filePath = PathUtil::JoinPath(path, fileName);
            fslib::FileMeta fileMeta;
            THROW_IF_FS_ERROR(FslibWrapper::GetFileMeta(filePath, fileMeta), "path[%s]", filePath.c_str());
            dpFileMetas.Append(FileInfo(fileName, fileMeta.fileLength, fileMeta.lastModifyTime));
        }
    }
    string content = dpFileMetas.ToString();
    string legacyIndexFile = PathUtil::JoinPath(path, DEPLOY_INDEX_FILE_NAME);
    string deployIndexFile = PathUtil::JoinPath(path, SEGMENT_FILE_LIST);

    FslibWrapper::DeleteFileE(legacyIndexFile, DeleteOption::NoFence(true));
    FslibWrapper::DeleteFileE(deployIndexFile, DeleteOption::NoFence(true));

    FslibWrapper::AtomicStoreE(deployIndexFile, content);
    if (needDumpLegacyFile()) {
        FslibWrapper::AtomicStoreE(legacyIndexFile, content);
    }
    return true;
}

bool SegmentFileListWrapper::Dump(const file_system::DirectoryPtr& directory, const std::string& lifecycle)
{
    if (directory == nullptr) {
        IE_LOG(ERROR, "empty directory.");
        return false;
    }

    file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
    directory->RemoveFile(DEPLOY_INDEX_FILE_NAME, removeOption);
    directory->RemoveFile(SEGMENT_FILE_LIST, removeOption);
    FslibWrapper::DeleteFileE(PathUtil::JoinPath(directory->GetOutputPath(), DEPLOY_INDEX_FILE_NAME),
                              DeleteOption::Fence(directory->GetFenceContext().get(), true));
    FslibWrapper::DeleteFileE(PathUtil::JoinPath(directory->GetOutputPath(), SEGMENT_FILE_LIST),
                              DeleteOption::Fence(directory->GetFenceContext().get(), true));

    std::vector<FileInfo> fileInfos;
    try {
        directory->ListPhysicalFile("", fileInfos, true);
    } catch (const util::NonExistException& e) {
        IE_LOG(ERROR, "catch NonExist exception: %s", e.what());
        return false;
    } catch (const autil::legacy::ExceptionBase& e) {
        IE_LOG(ERROR, "Catch exception: %s", e.what());
        throw e;
    } catch (...) {
        IE_LOG(ERROR, "catch exception when ListDirRecursive");
        throw;
    }

    DoDump(directory, fileInfos, nullptr, lifecycle);
    return true;
}

bool SegmentFileListWrapper::Dump(const file_system::DirectoryPtr& directory, const std::vector<std::string>& fileList)
{
    std::vector<FileInfo> fileInfos;
    ConvertFileListToFileInfos(directory, fileList, fileInfos);
    DoDump(directory, fileInfos, nullptr, "");
    return true;
}

void SegmentFileListWrapper::Dump(const file_system::DirectoryPtr& directory, const SegmentInfoPtr& segmentInfo)
{
    assert(directory);
    std::vector<FileInfo> fileInfos;
    auto prepareDump = [&]() -> void {
        file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
        directory->RemoveFile(DEPLOY_INDEX_FILE_NAME, removeOption);
        directory->RemoveFile(SEGMENT_FILE_LIST, removeOption);
        FslibWrapper::DeleteFileE(PathUtil::JoinPath(directory->GetOutputPath(), DEPLOY_INDEX_FILE_NAME),
                                  DeleteOption::Fence(directory->GetFenceContext().get(), /*mayNonExist=*/true));
        FslibWrapper::DeleteFileE(PathUtil::JoinPath(directory->GetOutputPath(), SEGMENT_FILE_LIST),
                                  DeleteOption::Fence(directory->GetFenceContext().get(), /*mayNonExist=*/true));
        fileInfos.clear();
        directory->ListPhysicalFile(/*path=*/"", fileInfos, /*recursive=*/true);
    };
    RetryUtil::RetryE(prepareDump);
    DoDump(directory, fileInfos, segmentInfo, /*lifecycle=*/"");
}

void SegmentFileListWrapper::Dump(const DirectoryPtr& directory, const fslib::FileList& fileList,
                                  const SegmentInfoPtr& segmentInfo)
{
    std::vector<FileInfo> fileInfos;
    ConvertFileListToFileInfos(directory, fileList, fileInfos);
    DoDump(directory, fileInfos, segmentInfo, "");
}

bool SegmentFileListWrapper::IsExist(const file_system::DirectoryPtr& dir)
{
    return dir->IsExist(SEGMENT_FILE_LIST) || dir->IsExist(DEPLOY_INDEX_FILE_NAME);
}

bool SegmentFileListWrapper::needDumpLegacyFile()
{
    return autil::EnvUtil::getEnv("INDEXLIB_COMPATIBLE_DEPLOY_INDEX_FILE", true);
}

void SegmentFileListWrapper::DoDump(const file_system::DirectoryPtr& directory,
                                    const std::vector<file_system::FileInfo>& fileInfos,
                                    const SegmentInfoPtr& segmentInfo, const std::string& lifecycle)
{
    assert(directory);
    IndexFileList dpIndexMeta;
    dpIndexMeta.deployFileMetas = std::move(fileInfos);
    dpIndexMeta.lifecycle = lifecycle;
    if (segmentInfo) {
        // add user set segmentInfo to dp meta, if user set null do nothing
        // It's possible that there are duplicate segment_info added, thus need to Dedup() below.
        dpIndexMeta.Append(FileInfo(SEGMENT_INFO_FILE_NAME, segmentInfo->ToString().size(), (uint64_t)-1));
    }

    dpIndexMeta.Dedup();
    string fileContent = dpIndexMeta.ToString();
    auto writerOption = WriterOption::AtomicDump();
    writerOption.notInPackage = true;
    directory->Store(SEGMENT_FILE_LIST, fileContent, writerOption);
    if (needDumpLegacyFile()) {
        directory->Store(DEPLOY_INDEX_FILE_NAME, fileContent, writerOption);
    }
}
}} // namespace indexlib::index_base

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
#include "indexlib/file_system/relocatable/Relocator.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/relocatable/RelocatableFolder.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, Relocator);

std::pair<Status, std::shared_ptr<RelocatableFolder>>
Relocator::CreateFolder(const std::string& folderRoot, const std::shared_ptr<util::MetricProvider>& metricProvider)
{
    auto ec = FslibWrapper::DeleteDir(folderRoot, DeleteOption::MayNonExist()).Code();
    if (ec != FSEC_OK) {
        RETURN2_IF_STATUS_ERROR(Status::IOError(), nullptr, "delete [%s] failed: ec[%d]", folderRoot.c_str(), ec);
    }
    AUTIL_LOG(INFO, "folder root[%s] deleted", folderRoot.c_str());

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = indexlib::file_system::FSST_DISK;
    auto [status, fs] =
        FileSystemCreator::Create(/*name*/ "relocatable_global_root", folderRoot, fsOptions, metricProvider,
                                  /*isOverride*/ true, FenceContext::NoFence())
            .StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create file system for folder[%s] failed", folderRoot.c_str());
    return {Status::OK(), std::make_shared<RelocatableFolder>(IDirectory::Get(fs))};
}

Status Relocator::SealFolder(const std::shared_ptr<RelocatableFolder>& folder)
{
    // TODO: seal on non-toplevel folder should fail
    auto fs = folder->_folderRoot->GetFileSystem();
    std::vector<std::string> entryList;
    RETURN_IF_STATUS_ERROR(fs->ListDir(/*rawPath*/ "", indexlib::file_system::ListOption(), entryList).Status(),
                           "list [%s] failed", folder->_folderRoot->GetOutputPath().c_str());
    if (entryList.empty()) {
        AUTIL_LOG(INFO, "empty[%s], seal nothing", folder->_folderRoot->GetOutputPath().c_str());
        return Status::OK();
    }
    return fs->CommitPreloadDependence(/*fenceContext*/ nullptr).Status();
}

Status Relocator::AddSource(const std::string& sourceDir)
{
    assert(!sourceDir.empty());
    _sourceDirs.push_back(sourceDir);
    return Status::OK();
}

Status Relocator::Relocate()
{
    AUTIL_LOG(INFO, "begin relocate [%s] to [%s]", autil::StringUtil::toString(_sourceDirs).c_str(),
              _targetDir.c_str());
    for (const auto& sourceDir : _sourceDirs) {
        EntryTableBuilder builder;
        auto options = std::make_shared<FileSystemOptions>();
        auto entryTable = builder.CreateEntryTable(/*name*/ "", sourceDir, options);
        if (!entryTable) {
            AUTIL_LOG(ERROR, "create entry table for [%s] failed", sourceDir.c_str());
            return Status::InternalError("create entry table failed");
        }
        auto ec = builder.MountVersion(sourceDir, INVALID_VERSIONID, /*logicalPath*/ "", FSMT_READ_ONLY);
        // if no entry meta found, ec is FSEC_OK
        if (ec != FSEC_OK) {
            AUTIL_LOG(ERROR, "mount entry table failed in [%s], ec[%d]", sourceDir.c_str(), ec);
            return Status::IOError("mount entry table failed");
        }
        std::string root = "";
        RETURN_IF_STATUS_ERROR(RelocateDirectory(*entryTable, root), "relocate [%s] failed", sourceDir.c_str());
    }
    AUTIL_LOG(INFO, "end relocate");
    return Status::OK();
}

Status Relocator::RelocateDirectory(const EntryTable& entryTable, const std::string& relativePath)
{
    auto entries = entryTable.ListDir(relativePath, /*recursive*/ false);
    if (entries.empty() && relativePath == "") {
        // empty entry table, do nothing
        return Status::OK();
    }
    RETURN_IF_STATUS_ERROR(MakeDirectory(relativePath), "make dir[%s] failed", relativePath.c_str());
    for (const auto& entryMeta : entries) {
        if (entryMeta.IsInPackage()) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "can not relocate package file.");
        }
        const auto& metaRoot = entryMeta.GetPhysicalRoot();
        const auto& metaPath = entryMeta.GetPhysicalPath();
        if (entryMeta.IsFile()) {
            RETURN_IF_STATUS_ERROR(RelocateFile(metaRoot, metaPath), "relocate [%s][%s] failed", metaRoot.c_str(),
                                   metaPath.c_str());
        } else if (entryMeta.IsDir()) {
            auto srcPath = util::PathUtil::JoinPath(relativePath, metaPath);
            RETURN_IF_STATUS_ERROR(RelocateDirectory(entryTable, srcPath), "relocate [%s] failed", srcPath.c_str());
        }
    }
    return Status::OK();
}

Status Relocator::MakeDirectory(const std::string& relativePath)
{
    auto targetPath = util::PathUtil::JoinPath(_targetDir, relativePath);
    auto status = FslibWrapper::MkDir(targetPath, /*recursive*/ true, /*mayExist*/ true).Status();
    RETURN_IF_STATUS_ERROR(status, "mkdir [%s] failed", targetPath.c_str());
    return Status::OK();
}

Status Relocator::RelocateFile(const std::string& sourceRoot, const std::string& relativePath)
{
    auto srcPath = util::PathUtil::JoinPath(sourceRoot, relativePath);
    auto dstPath = util::PathUtil::JoinPath(_targetDir, relativePath);
    auto ec = FslibWrapper::Rename(srcPath, dstPath).Code();
    if (ec == FSEC_NOENT) {
        auto [status, exist] = FslibWrapper::IsExist(dstPath).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "is exist for [%s] failed", dstPath.c_str());
        if (exist) {
            ec = FSEC_EXIST;
        }
    }
    if (ec == FSEC_EXIST) {
        AUTIL_LOG(INFO, "file [%s] exists", dstPath.c_str());
        return Status::OK();
    }

    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "rename [%s] to [%s] failed, ec[%d]", srcPath.c_str(), dstPath.c_str(), ec);
        return toStatus(ec);
    }
    AUTIL_LOG(INFO, "rename [%s] to [%s]", srcPath.c_str(), dstPath.c_str());
    return Status::OK();
}

} // namespace indexlib::file_system

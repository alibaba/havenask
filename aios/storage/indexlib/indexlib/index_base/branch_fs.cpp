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
#include "indexlib/index_base/branch_fs.h"

#include "autil/EnvUtil.h"
#include "autil/NetUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/default_branch_hinter.h"
#include "indexlib/util/PathUtil.h"

using autil::StringUtil;
using namespace std;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, BranchFS);

BranchFS::BranchFS(const string& root, const string& branchName) : mRoot(root), mBranch(branchName), mIsLegacy(false) {}

BranchFS::~BranchFS() {}

std::unique_ptr<BranchFS> BranchFS::CreateWithAutoFork(const std::string& root,
                                                       const file_system::FileSystemOptions& fsOptions,
                                                       const util::MetricProviderPtr& metricProvider,
                                                       IBranchHinter* hinter)
{
    DefaultBranchHinter defaultHinter;
    IBranchHinter* useHinter = hinter ? hinter : &defaultHinter;
    string branchName = useHinter->GetNewBranchName(root);
    return AutoFork(root, branchName, fsOptions, metricProvider, useHinter);
}

std::unique_ptr<BranchFS> BranchFS::Fork(const std::string& root, const std::string& srcBranchName,
                                         const std::string& branchName, const file_system::FileSystemOptions& fsOptions,
                                         const util::MetricProviderPtr& metricProvider)

{
    auto branchFs = std::unique_ptr<BranchFS>(new BranchFS(root, branchName));
    if (!branchFs) {
        IE_LOG(ERROR, "fork from branch [%s] failed", srcBranchName.c_str());
        return nullptr;
    }
    branchFs->Init(metricProvider, fsOptions);
    if (branchFs->IsLegacy()) {
        return branchFs;
    }
    IE_LOG(INFO, "BranchFS of path [%s] is forked from srcBranch [%s], new branchName[%s]", root.c_str(),
           srcBranchName.c_str(), branchName.c_str());
    auto reporter = branchFs->GetFileSystem()->GetFileSystemMetricsReporter();
    if (srcBranchName == branchName) {
        if (reporter) {
            reporter->ReportBranchCreate(branchName);
        }
        return branchFs;
    }
    if (branchFs->MountBranch(root, srcBranchName, INVALID_VERSIONID, FSMT_READ_ONLY)) {
        branchFs->CopyCheckPoints();
        // mount again, make sure all files recorded by checkpoint will be mounted
        if (branchFs->MountBranch(root, srcBranchName, INVALID_VERSIONID, FSMT_READ_ONLY)) {
            // make sure metafiles in local branch will replace metafiles in src branch
            branchFs->CopyUnSharedMetaFiles();
            branchFs->CommitPreloadDependence();
            if (reporter) {
                reporter->ReportBranchCreate(branchName);
            }
            return branchFs;
        }
    }
    IE_LOG(ERROR, "fork from branch [%s] failed", srcBranchName.c_str());
    return nullptr;
}

std::unique_ptr<BranchFS> BranchFS::AutoFork(const std::string& root, const std::string& branchName,
                                             const file_system::FileSystemOptions& fsOptions,
                                             const util::MetricProviderPtr& metricProvider, IBranchHinter* hinter)
{
    bool exist = false;
    auto ec = FslibWrapper::IsExist(root, exist);
    THROW_IF_FS_ERROR(ec, "root [%s] is failed", root.c_str());
    if (!exist) {
        auto branchFs = BranchFS::Create(root, branchName);
        if (!branchFs) {
            IE_LOG(ERROR, "new branch fs [%s] failed", branchName.c_str());
            return nullptr;
        }
        hinter->UpdateCountFile(root, branchName);
        branchFs->Init(metricProvider, fsOptions);
        return branchFs;
    }

    std::string srcBranchName;
    GetSrcBranchFromHinter(hinter, root, srcBranchName);
    hinter->UpdateCountFile(root, branchName);
    return Fork(root, srcBranchName, branchName, fsOptions, metricProvider);
}

void BranchFS::GetSrcBranchFromHinter(IBranchHinter* hinter, const std::string& root, string& branchName)
{
    DefaultBranchHinter defaultHinter;
    if (!hinter->SelectFastestBranch(root, branchName)) {
        defaultHinter.SelectFastestBranch(root, branchName);
        return;
    }
    auto [ec, isExist] = FslibWrapper::IsExist(GetBranchFSPhysicalRoot(root, branchName));
    if (ec != FSEC_OK || !isExist) {
        IE_LOG(WARN, "get root [%s] src branch [%s] error, ec is [%d], exist is [%d]", root.c_str(), branchName.c_str(),
               ec, isExist);
        defaultHinter.SelectFastestBranch(root, branchName);
    }
}

void BranchFS::Init(const util::MetricProviderPtr& metricProvider, const FileSystemOptions& fsOptions)
{
    // branch fs no fence!
    CheckIsLegacy();
    string fsRoot = GetBranchFSPhysicalRoot(mRoot, mBranch);
    string fileSystemName = "branchFs" + (mBranch.empty() ? string() : string("_")) + mBranch;
    mLogicalFS = FileSystemCreator::Create(fileSystemName, fsRoot, fsOptions, metricProvider, false).GetOrThrow();
    auto ec = mLogicalFS->MountVersion(fsRoot, INVALID_VERSIONID, "", FSMT_READ_ONLY, nullptr);
    if (ec == FSEC_NOENT) {
        IE_LOG(INFO, "no version find in [%s]", fsRoot.c_str());
    }
    THROW_IF_FS_ERROR(ec, "mount version failed");

    if (mIsLegacy && !mBranch.empty()) {
        THROW_IF_FS_ERROR(mLogicalFS->MountDir(fsRoot, "", "", FSMT_READ_WRITE, false), "mount dir failed");
    }
}

bool BranchFS::MountBranch(const std::string& root, const std::string& branchName, int32_t versionId,
                           file_system::FSMountType fsMountType)
{
    if (!MountVersion(root, branchName, versionId, fsMountType)) {
        return false;
    }
    string branchRoot = GetBranchFSPhysicalRoot(root, branchName);
    THROW_IF_FS_ERROR(mLogicalFS->MountDir(branchRoot, "", "", fsMountType, true), "mount dir failed");
    return true;
}

bool BranchFS::MountVersion(const std::string& root, const std::string& branchName, int32_t versionId,
                            file_system::FSMountType fsMountType)
{
    string branchRoot = GetBranchFSPhysicalRoot(root, branchName);
    if (!FslibWrapper::IsDir(branchRoot).GetOrThrow()) {
        IE_LOG(ERROR, "src branchRoot [%s] is not exsist!", branchRoot.c_str());
        return false;
    }
    if (fsMountType == FSMT_READ_WRITE && !GetRootDirectory()->IsExist(PathUtil::GetParentDirPath(branchName))) {
        GetRootDirectory()->MakeDirectory(PathUtil::GetParentDirPath(branchName), DirectoryOption::Package());
    }
    THROW_IF_FS_ERROR(mLogicalFS->MountVersion(branchRoot, versionId, "", fsMountType, nullptr),
                      "mount version failed");
    return true;
}

string BranchFS::GetBranchFSPhysicalRoot(const string& root, const string& branchName)
{
    return PathUtil::JoinPath(root, branchName);
}

string BranchFS::GetBranchFSPhysicalRoot(const string& branchName) const
{
    return PathUtil::JoinPath(mRoot, branchName);
}

void BranchFS::CommitToDefaultBranch(IBranchHinter* hinter)
{
    DefaultBranchHinter defaultHinter;
    IBranchHinter* useHinter = hinter ? hinter : &defaultHinter;
    useHinter->CommitToDefaultBranch(mRoot, mBranch);
}

string BranchFS::GetDefaultBranch(const std::string& root, IBranchHinter* hinter)
{
    DefaultBranchHinter defaultHinter;
    IBranchHinter* useHinter = hinter ? hinter : &defaultHinter;
    return useHinter->GetDefaultBranch(root);
}

file_system::DirectoryPtr BranchFS::GetDirectoryFromDefaultBranch(const std::string& directoryRoot,
                                                                  const file_system::FileSystemOptions& fsOptions,
                                                                  IBranchHinter* hinter, std::string* branchName)
{
    string defaultBranch = GetDefaultBranch(directoryRoot, hinter);
    auto ret = FslibWrapper::IsExist(PathUtil::JoinPath(directoryRoot, defaultBranch));
    THROW_IF_FS_ERROR(ret.ec, "Check IsExist of Path [%s] failed", directoryRoot.c_str());
    if (!ret.result) {
        THROW_IF_FS_ERROR(FSEC_NOENT, "Get Directory From Default Branch of Path [%s] failed", directoryRoot.c_str());
    }
    auto fs = BranchFS::Create(directoryRoot, defaultBranch);
    fs->Init(nullptr, fsOptions);
    *branchName = defaultBranch;
    return fs->GetRootDirectory();
}

void BranchFS::CopyUnSharedMetaFiles()
{
    assert(mIsLegacy == false);
    if (mLogicalFS->IsExist(TRUNCATE_META_DIR_NAME).GetOrThrow()) {
        THROW_IF_FS_ERROR(mLogicalFS->CopyToOutputRoot(TRUNCATE_META_DIR_NAME, true), "copy truncate meta dir failed");
    }

    if (mLogicalFS->IsExist(ADAPTIVE_DICT_DIR_NAME).GetOrThrow()) {
        THROW_IF_FS_ERROR(mLogicalFS->CopyToOutputRoot(ADAPTIVE_DICT_DIR_NAME, true), "copy adaptive dict dir failed");
    }
}

void BranchFS::CopyCheckPoints()
{
    if (mLogicalFS->IsExist(MERGE_ITEM_CHECK_POINT_DIR_NAME).GetOrThrow()) {
        THROW_IF_FS_ERROR(mLogicalFS->CopyToOutputRoot(MERGE_ITEM_CHECK_POINT_DIR_NAME, true),
                          "copy merge item check point dir failed");
    }
    // for alter field task
    if (mLogicalFS->IsExist(ALTER_FIELD_CHECK_POINT_DIR_NAME).GetOrThrow()) {
        THROW_IF_FS_ERROR(mLogicalFS->CopyToOutputRoot(ALTER_FIELD_CHECK_POINT_DIR_NAME, true),
                          "copy alter filed check point dir failed");
    }
}

/*
     only allow to update the preloaded entryTable,
     which means this does not allow to be called if there is any else entryTable except the preloaded ones.

     I.E:

     case that is not allow to update

        entryTable.preload entryTable.0

     case that is allow to update

        entryTable.preload
 */
void BranchFS::UpdatePreloadDependence()
{
    if (mBranch.empty()) {
        return;
    }
    string fsRoot = GetBranchFSPhysicalRoot(mRoot, mBranch);
    string backupPreloadFilePath = PathUtil::JoinPath(fsRoot, ENTRY_TABLE_PRELOAD_BACK_UP_FILE_NAME);
    THROW_IF_FS_ERROR(mLogicalFS->RemoveFile(ENTRY_TABLE_PRELOAD_BACK_UP_FILE_NAME, RemoveOption::MayNonExist()),
                      "remove ENTRY_TABLE_PRELOAD_BACK_UP_FILE_NAME failed");
    auto ec = FslibWrapper::DeleteFile(backupPreloadFilePath, DeleteOption::NoFence(true)).Code();
    if (ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "remove old backup file failed , filename[%s]",
                             ENTRY_TABLE_PRELOAD_BACK_UP_FILE_NAME);
    }
    THROW_IF_FS_ERROR(mLogicalFS->Rename(ENTRY_TABLE_PRELOAD_FILE_NAME, ENTRY_TABLE_PRELOAD_BACK_UP_FILE_NAME,
                                         FenceContext::NoFence()),
                      "rename ENTRY_TABLE_PRELOAD_FILE_NAME failed");
    CommitPreloadDependence();
}

void BranchFS::CommitPreloadDependence()
{
    THROW_IF_FS_ERROR(mLogicalFS->CommitPreloadDependence(file_system::FenceContext::NoFence()),
                      "CommitPreloadDependence failed");
}

void BranchFS::CheckIsLegacy()
{
    mIsLegacy = false;
    string fsRoot = GetBranchFSPhysicalRoot(mRoot, mBranch);
    string preloadFilePath = PathUtil::JoinPath(fsRoot, ENTRY_TABLE_PRELOAD_FILE_NAME);
    auto ret = FslibWrapper::IsExist(preloadFilePath);
    if (ret.ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "check is legacy failed, filename[%s]", preloadFilePath.c_str());
    }
    if (!ret.result) {
        string backupPreloadFilePath = PathUtil::JoinPath(fsRoot, ENTRY_TABLE_PRELOAD_BACK_UP_FILE_NAME);
        auto ret = FslibWrapper::IsExist(backupPreloadFilePath);
        if (ret.ec != FSEC_OK) {
            INDEXLIB_FATAL_ERROR(FileIO, "check is legacy failed, filename[%s]", backupPreloadFilePath.c_str());
        }
        if (ret.result) {
            auto fsEC = FslibWrapper::Rename(backupPreloadFilePath, preloadFilePath /*no fence*/).Code();
            if (fsEC != FSEC_OK) {
                INDEXLIB_FATAL_ERROR(FileIO, "recover old ENTRY_TABLE_PRELOAD_FILE of path[%s] failed",
                                     backupPreloadFilePath.c_str());
            }
            mIsLegacy = true;
        } else {
            auto ret = EntryTableBuilder::GetLastVersion(fsRoot);
            if (ret.ec != FSEC_OK && ret.ec != FSEC_NOENT) {
                INDEXLIB_FATAL_ERROR(FileIO, "check is legacy failed");
            }
            auto versionId = ret.result;
            if (versionId != -1) {
                mIsLegacy = true;
            }
        }
    } else {
        mIsLegacy = true;
    }
}

bool BranchFS::IsBranchPath(const std::string& path) { return path.find(BRANCH_DIR_NAME_PREFIX) != string::npos; }

string BranchFS::TEST_EstimateFastestBranchName(const std::string& root)
{
    string branchName;
    DefaultBranchHinter hinter;
    hinter.SelectFastestBranch(root, branchName);
    return branchName;
}

void BranchFS::TEST_MoveToMainRoad()
{
    string actualPath = BranchFS::GetBranchFSPhysicalRoot(mRoot, mBranch);
    if (actualPath != mRoot) {
        fslib::FileList files;
        auto ec = FslibWrapper::ListDir(actualPath, files).Code();
        THROW_IF_FS_ERROR(ec, "list branch path [%s] failed", actualPath.c_str());
        for (const auto& file : files) {
            string srcFile = FslibWrapper::JoinPath(actualPath, file);
            string destFile = FslibWrapper::JoinPath(mRoot, file);
            auto ec = FslibWrapper::DeleteFile(destFile, DeleteOption::NoFence(true)).Code();
            THROW_IF_FS_ERROR(ec, "delete branch file [%s] failed", destFile.c_str());
            auto ec2 = FslibWrapper::Rename(srcFile, destFile).Code();
            THROW_IF_FS_ERROR(ec2, "move branch file [%s] to dest [%s] failed", srcFile.c_str(), destFile.c_str());
        }
        IE_LOG(INFO, "branch fs move from actualPath [%s] to mainRoad [%s] success", actualPath.c_str(), mRoot.c_str());
    }
    string markFile = FslibWrapper::JoinPath(mRoot, MARKED_BRANCH_INFO);
    FslibWrapper::DeleteFileE(markFile, DeleteOption::NoFence(true));
}

}} // namespace indexlib::index_base

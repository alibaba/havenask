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
#include "indexlib/index_base/common_branch_hinter.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, CommonBranchHinter);

static const string BRANCH_COUNT_FILE = "branch_count_file";
const int64_t CommonBranchHinter::MAX_BRANCH_COUNT = 500;

CommonBranchHinter::CommonBranchHinter(const Option& option) : mOption(option)
{
    assert(!mOption.branchNamePolicy.empty());
}

CommonBranchHinter::~CommonBranchHinter() {}

std::string CommonBranchHinter::GetNewBranchName(const string& rootPath)
{
    string branchNameWithoutPrefix;
    if (mOption.branchNamePolicy == Option::BNP_LEGACY) {
        branchNameWithoutPrefix = mOption.branchId == 0 ? "" : std::to_string(mOption.branchId);
    } else {
        mFileList.clear();
        string maxBranchName;
        string epochId;
        if (ReachMaxBranchLimit(rootPath, mFileList, maxBranchName)) {
            epochId = ExtractEpochFromBranch(maxBranchName);
        } else {
            epochId = mOption.epochId;
        }
        string branchIdStr = std::to_string(mOption.branchId + 1000).substr(1);
        branchNameWithoutPrefix = epochId + branchIdStr;
    }
    return branchNameWithoutPrefix.empty() ? "" : (string(BRANCH_DIR_NAME_PREFIX) + branchNameWithoutPrefix);
}

bool CommonBranchHinter::BranchNewer(const std::string& lBranchName, const std::string& rBranchName) const
{
    return EpochIdUtil::CompareGE(ExtractEpochFromBranch(lBranchName), ExtractEpochFromBranch(rBranchName));
}

bool CommonBranchHinter::CanOperateBranch(const std::string& branchName) const
{
    if (mOption.branchNamePolicy != Option::BNP_NORMAL) {
        return true;
    }
    return EpochIdUtil::CompareGE(mOption.epochId, ExtractEpochFromBranch(branchName));
}

bool CommonBranchHinter::ReachMaxBranchLimit(const std::string& rootPath, fslib::FileList& dirList,
                                             std::string& branchName)
{
    int64_t maxBranchCount = autil::EnvUtil::getEnv("MAX_BRANCH_COUNT", MAX_BRANCH_COUNT);
    string countFile = FslibWrapper::JoinPath(rootPath, string(BRANCH_COUNT_FILE) + ".inline" + TEMP_FILE_SUFFIX);
    string countStr;
    auto ec = FslibWrapper::StatPanguInlineFile(countFile, countStr).Code();
    if (ec == FSEC_OK) {
        vector<string> branchCountInfos = StringUtil::split(countStr, " ");
        assert(branchCountInfos.size() <= 2);
        if (branchCountInfos.empty()) {
            return false;
        }
        int64_t branchCount = -1;
        if (StringUtil::fromString(branchCountInfos[0], branchCount) && branchCount > maxBranchCount) {
            branchName = branchCountInfos.size() == 2 ? (BRANCH_DIR_NAME_PREFIX + branchCountInfos[1]) : "";
            IE_LOG(WARN, "branch count [%ld] more than max branch count [%ld], will use old branch [%s]", branchCount,
                   MAX_BRANCH_COUNT, branchName.c_str());
            return true;
        }
        return false;
    }

    ec = FslibWrapper::ListDir(rootPath, dirList).Code();
    if (ec == FSEC_NOENT) {
        return false;
    }

    // assert(ec == FSEC_OK);
    THROW_IF_FS_ERROR(ec, "list rootPath [%s] failed", rootPath.c_str());
    int64_t branchCount = 0;
    string lastBranch = "";
    for (const string& dirName : dirList) {
        if (IsBranchDirectory(dirName)) {
            branchCount++;
            if (BranchNewer(dirName, lastBranch)) {
                lastBranch = dirName;
            }
        }
    }
    branchName = lastBranch;
    if (branchCount > maxBranchCount) {
        IE_LOG(WARN, "branch count [%ld] more than max branch count [%ld], will not create branch", branchCount,
               MAX_BRANCH_COUNT);
        return true;
    }
    return false;
}

void CommonBranchHinter::CommitToDefaultBranch(const std::string& rootPath, const std::string& branchName)
{
    string branchInfoFile = PathUtil::JoinPath(rootPath, MARKED_BRANCH_INFO);
    auto ret = FslibWrapper::IsExist(branchInfoFile);
    THROW_IF_FS_ERROR(ret.ec, "Check IsExist of branch_info file [%s] failed", branchInfoFile.c_str());
    if (ret.result) {
        IE_LOG(INFO, "branchInfoFile already exsist, will not commit again");
        return;
    }
    FslibWrapper::AtomicStoreE(branchInfoFile, branchName);
    vector<string> clearFiles;
    GetClearFile(rootPath, branchName, clearFiles);
    for (auto& file : clearFiles) {
        auto ec = FslibWrapper::DeleteFile(file, DeleteOption::NoFence(false)).Code();
        if (ec != FSEC_OK) {
            IE_LOG(WARN, "remove file [%s] failed, ec is [%d]", file.c_str(), ec);
        }
    }
}

string CommonBranchHinter::GetDefaultBranch(const std::string& root)
{
    string branchInfoFile = PathUtil::JoinPath(root, MARKED_BRANCH_INFO);
    string branchName;
    try {
        FslibWrapper::AtomicLoadE(branchInfoFile, branchName);
    } catch (util::NonExistException& e) {
        IE_LOG(WARN, "branchInfoFile is not exsist in path[%s]", root.c_str());
        return "";
    }
    IE_LOG(INFO, "select branch [%s] as default branch", branchName.c_str());
    return branchName;
}

void CommonBranchHinter::UpdateCountFile(const std::string& root, const std::string& branchName)
{
    if ((FslibWrapper::IsExist(FslibWrapper::JoinPath(root, branchName))).GetOrThrow()) {
        return;
    }

    string countFile = FslibWrapper::JoinPath(root, string(BRANCH_COUNT_FILE) + ".inline" + TEMP_FILE_SUFFIX);
    string currentCount;
    int retryTime = 0;
    int maxRetryTime = 4;
    do {
        if (retryTime > maxRetryTime) {
            IE_LOG(WARN, "update counter file time more than max [%d], will not update", maxRetryTime);
            return;
        }
        retryTime++;
        auto ec = FslibWrapper::StatPanguInlineFile(countFile, currentCount).Code();
        if (ec == FSEC_NOTSUP) {
            IE_LOG(INFO, "StatPanguInlineFile only support pangu fs, not write inline file [%s]", countFile.c_str());
            return;
        }
        if (ec == FSEC_NOENT) {
            auto ec = FslibWrapper::CreatePanguInlineFile(countFile).Code();
            if (ec == FSEC_EXIST || ec == FSEC_OK) {
                continue;
            }
        }
        if (ec == FSEC_OK) {
            vector<string> branchCountInfos = StringUtil::split(currentCount, " ");
            int64_t numCount = 0;
            string branchWithoutPrefix =
                IsBranchDirectory(branchName) ? branchName.substr(string(BRANCH_DIR_NAME_PREFIX).size()) : "";
            if (branchCountInfos.empty() || !StringUtil::fromString(branchCountInfos[0], numCount)) {
                numCount = 1;
            } else if (branchCountInfos.size() == 2 && branchCountInfos[1] == branchWithoutPrefix) {
                IE_LOG(INFO, "old epoch id is same with new branch [%s], prehaps to many branchs",
                       branchWithoutPrefix.c_str());
                return;
            } else {
                numCount++;
            }
            string newCount = StringUtil::toString(numCount) + " " + branchWithoutPrefix;
            auto ec = FslibWrapper::UpdatePanguInlineFileCAS(countFile, currentCount, newCount).Code();
            if (ec != FSEC_OK) {
                sleep(1 << retryTime);
                continue;
            }
            IE_LOG(INFO, "update count file [%s] content from [%s] to [%s]", countFile.c_str(), currentCount.c_str(),
                   newCount.c_str());
            return;
        }
    } while (true);
}

string CommonBranchHinter::ExtractEpochFromBranch(const std::string& branchName)
{
    size_t prefix = string(BRANCH_DIR_NAME_PREFIX).size();
    size_t suffix = 3;
    if (branchName.size() <= prefix + suffix) {
        return "";
    }
    assert(branchName.find(BRANCH_DIR_NAME_PREFIX) == 0);
    return branchName.substr(prefix, branchName.size() - prefix - suffix);
}

bool CommonBranchHinter::IsBranchDirectory(const std::string& dir)
{
    return autil::StringUtil::startsWith(dir, BRANCH_DIR_NAME_PREFIX);
}

}} // namespace indexlib::index_base

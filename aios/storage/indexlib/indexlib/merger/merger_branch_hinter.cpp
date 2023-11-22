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
#include "indexlib/merger/merger_branch_hinter.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergerBranchHinter);

const string MergerBranchHinter::MERGER_PROGRESS = "merger_progress_for_branch";

MergerBranchHinter::MergerBranchHinter(const Option& option) : index_base::CommonBranchHinter(option) {}

MergerBranchHinter::~MergerBranchHinter() {}

void MergerBranchHinter::GetClearFile(const std::string& rootPath, const std::string& branchName,
                                      vector<string>& clearFiles)
{
    string filePath = PathUtil::JoinPath(rootPath, MERGER_PROGRESS);
    clearFiles.push_back(filePath);
    IE_LOG(INFO, "clear commit files [%s]", StringUtil::toString(clearFiles).c_str());
}

bool MergerBranchHinter::SelectFastestBranch(const std::string& rootPath, std::string& branchName)
{
    string progressPath = PathUtil::JoinPath(rootPath, MERGER_PROGRESS);
    string currentProgressStr;
    auto ec = FslibWrapper::Load(progressPath, currentProgressStr).Code();
    if (ec != FSEC_OK) {
        IE_LOG(INFO, "load file [%s] failed, ec is [%d]", progressPath.c_str(), ec);
        return false;
    }
    int64_t progress = 0;
    if (!ParseHinterFile(currentProgressStr, progress, branchName)) {
        return false;
    }
    return true;
}

void MergerBranchHinter::StorePorgressIfNeed(const std::string& rootPath, const std::string& branchName,
                                             int64_t progress)
{
    string progressPath = PathUtil::JoinPath(rootPath, MERGER_PROGRESS);
    string currentProgressStr;
    auto ec = FslibWrapper::Load(progressPath, currentProgressStr).Code();
    bool needStore = true;
    if (ec == FSEC_OK) {
        int64_t currentProgress = 0;
        string branchName;
        if (ParseHinterFile(currentProgressStr, currentProgress, branchName) && currentProgress >= progress) {
            needStore = false;
        }
    }

    if (needStore) {
        ec = FslibWrapper::AtomicStore(progressPath, branchName + " " + std::to_string(progress), true,
                                       FenceContext::NoFence())
                 .Code();
        if (ec != FSEC_OK) {
            IE_LOG(WARN, "store branch [%s] progress to rootPath [%s] failed, ec is [%d], ignore", branchName.c_str(),
                   rootPath.c_str(), ec);
        }
        IE_LOG(INFO, "store merger branch hinter success, path [%s], progress[%ld], branch [%s]", progressPath.c_str(),
               progress, branchName.c_str());
    }
}

bool MergerBranchHinter::ParseHinterFile(const string& progressStr, int64_t& progress, string& branchName)
{
    vector<string> currentInfos = autil::StringUtil::split(progressStr, " ");
    size_t size = currentInfos.size();
    if ((size != 2 && size != 1) || !autil::StringUtil::fromString(currentInfos[size - 1], progress)) {
        IE_LOG(ERROR, "parse hint file failed, content [%s] ", progressStr.c_str());
        return false;
    }
    branchName = size == 2 ? currentInfos[0] : "";
    return true;
}
}} // namespace indexlib::merger

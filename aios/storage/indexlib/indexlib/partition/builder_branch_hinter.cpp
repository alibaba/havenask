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
#include "indexlib/partition/builder_branch_hinter.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, BuilderBranchHinter);

const string BuilderBranchHinter::BUILDER_PROGRESS = "builder_progress_for_branch";

BuilderBranchHinter::BuilderBranchHinter(const Option& option) : index_base::CommonBranchHinter(option) {}

BuilderBranchHinter::~BuilderBranchHinter() {}

void BuilderBranchHinter::GetClearFile(const std::string& rootPath, const std::string& branchName,
                                       vector<string>& clearFiles)
{
    string filePath = PathUtil::JoinPath(rootPath, BUILDER_PROGRESS);
    clearFiles.push_back(filePath);
    IE_LOG(INFO, "clear commit files [%s]", StringUtil::toString(clearFiles).c_str());
}

bool BuilderBranchHinter::SelectFastestBranch(const std::string& rootPath, string& branchName)
{
    string progressPath = PathUtil::JoinPath(rootPath, BUILDER_PROGRESS);
    string currentProgress;
    auto ec = FslibWrapper::Load(progressPath, currentProgress).Code();
    if (ec != FSEC_OK) {
        IE_LOG(INFO, "load file [%s] failed, ec is [%d]", progressPath.c_str(), ec);
        return false;
    }
    int64_t offset;
    uint64_t src;
    if (!ParseHinterFile(currentProgress, src, offset, branchName)) {
        return false;
    }
    return true;
}

void BuilderBranchHinter::StorePorgressIfNeed(const std::string& rootPath, const string& branchName,
                                              const string& progress)
{
    int64_t newOffset = 0;
    int64_t newSrc = 0;
    document::IndexLocator locator;
    if (!locator.fromString(progress)) {
        IE_LOG(WARN, "new progress [%s] is not valid locator, ignore", progress.c_str())
        return;
    }
    newOffset = locator.getOffset();
    newSrc = locator.getSrc();
    string progressPath = PathUtil::JoinPath(rootPath, BUILDER_PROGRESS);
    string currentProgress;
    auto ec = FslibWrapper::Load(progressPath, currentProgress).Code();
    if (ec == FSEC_OK) {
        int64_t currentOffset = 0;
        uint64_t currentSrc = 0;
        string branchName;
        if (ParseHinterFile(currentProgress, currentSrc, currentOffset, branchName) &&
            (currentSrc >= newSrc && currentOffset >= newOffset)) {
            IE_LOG(INFO, "branch [%s] progress [%s] is not faster than currentProgress [%s], no need store",
                   branchName.c_str(), locator.toDebugString().c_str(), currentProgress.c_str());
            return;
        }
    }

    ec = FslibWrapper::AtomicStore(progressPath, branchName + " " + locator.toString(), true, FenceContext::NoFence())
             .Code();
    if (ec != FSEC_OK) {
        IE_LOG(WARN, "store branch [%s] progress to rootPath [%s] failed, ec is [%d], ignore", branchName.c_str(),
               rootPath.c_str(), ec);
    } else {
        IE_LOG(INFO, "store branch [%s] progress to rootPath [%s] success", branchName.c_str(), rootPath.c_str());
    }
}

bool BuilderBranchHinter::ParseHinterFile(const string& currentProgress, uint64_t& src, int64_t& offset,
                                          string& branchName)
{
    vector<string> currentInfos = autil::StringUtil::split(currentProgress, " ");
    document::IndexLocator currentLocator;
    size_t size = currentInfos.size();
    if ((size != 2 && size != 1) || !currentLocator.fromString(currentInfos[size - 1])) {
        IE_LOG(ERROR, "parse hint file failed, content [%s] ", currentProgress.c_str());
        return false;
    }
    offset = currentLocator.getOffset();
    branchName = size == 2 ? currentInfos[0] : "";
    return true;
}
}} // namespace indexlib::partition

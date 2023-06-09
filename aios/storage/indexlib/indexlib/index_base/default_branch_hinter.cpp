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
#include "indexlib/index_base/default_branch_hinter.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, DefaultBranchHinter);

DefaultBranchHinter::DefaultBranchHinter() : CommonBranchHinter(CommonBranchHinter::Option::Legacy(0, "")) {}

DefaultBranchHinter::~DefaultBranchHinter() {}

void DefaultBranchHinter::GetClearFile(const std::string& rootPath, const std::string& branchName,
                                       vector<string>& clearFiles)
{
}

bool DefaultBranchHinter::SelectFastestBranch(const std::string& rootPath, std::string& branchName)
{
    // here we assume the largest branch is the branch with most logicalFiles.
    FileSystemOptions fsOptions;
    uint32_t maxFileCount = 0;
    fsOptions.isOffline = true;
    if (mFileList.empty()) {
        auto ec = FslibWrapper::ListDir(rootPath, mFileList).Code();
        if (ec == FSEC_NOENT) {
            branchName = "";
            return true;
        }
        THROW_IF_FS_ERROR(ec, "list branch path [%s] failed", rootPath.c_str());
    }

    // if none branch, just use main path
    string fastBranchName = "";
    for (const string& dirName : mFileList) {
        if (!IsBranchDirectory(dirName)) {
            continue;
        }
        FileList files;
        uint32_t fileCount = 0;
        auto fs = BranchFS::Create(rootPath, dirName);
        fs->Init(nullptr, fsOptions);
        auto branchDir = fs->GetRootDirectory();
        branchDir->ListDir("", files, true);
        for (const auto& fileName : files) {
            // exclude the branchDir
            if (IsBranchDirectory(fileName)) {
                continue;
            }
            ++fileCount;
        }
        if (fileCount > maxFileCount) {
            maxFileCount = fileCount;
            fastBranchName = dirName;
        }
    }
    branchName = fastBranchName;
    return true;
}

}} // namespace indexlib::index_base

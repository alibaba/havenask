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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/common_branch_hinter_option.h"

namespace indexlib { namespace index_base {

class CommonBranchHinter : public BranchFS::IBranchHinter
{
public:
    using Option = CommonBranchHinterOption;
    explicit CommonBranchHinter(const Option& option);
    virtual ~CommonBranchHinter();

    CommonBranchHinter(const CommonBranchHinter&) = delete;
    CommonBranchHinter& operator=(const CommonBranchHinter&) = delete;
    CommonBranchHinter(CommonBranchHinter&&) = delete;
    CommonBranchHinter& operator=(CommonBranchHinter&&) = delete;

public:
    std::string GetNewBranchName(const std::string& rootPath) override;
    void CommitToDefaultBranch(const std::string& rootPath, const std::string& branch) override;
    std::string GetDefaultBranch(const std::string& root) override;
    void UpdateCountFile(const std::string& root, const std::string& branchName) override;
    bool BranchNewer(const std::string& lBranchName, const std::string& rBranchName) const;
    bool CanOperateBranch(const std::string& branchName) const;
    static std::string ExtractEpochFromBranch(const std::string& branchName);
    static bool IsBranchDirectory(const std::string& dir);

    const Option& GetOption() const { return mOption; }

protected:
    // get which files should be cleaned when commit to default branch
    virtual void GetClearFile(const std::string& rootPath, const std::string& branchName,
                              std::vector<std::string>& fileList) = 0;

    bool ReachMaxBranchLimit(const std::string& rootPath, fslib::FileList& dirList, std::string& branchName);

protected:
    Option mOption;

    // when @ReachMaxBranchLimit called, mFileLists may filled, maybe reduce list on @SelectFastestBranch
    fslib::FileList mFileList;
    const static int64_t MAX_BRANCH_COUNT;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CommonBranchHinter);

}} // namespace indexlib::index_base

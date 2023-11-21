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

#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/index_base/common_branch_hinter.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class MergerBranchHinter : public index_base::CommonBranchHinter
{
public:
    using Option = index_base::CommonBranchHinterOption;
    explicit MergerBranchHinter(const Option& option);
    ~MergerBranchHinter();

    MergerBranchHinter(const MergerBranchHinter&) = delete;
    MergerBranchHinter& operator=(const MergerBranchHinter&) = delete;
    MergerBranchHinter(MergerBranchHinter&&) = delete;
    MergerBranchHinter& operator=(MergerBranchHinter&&) = delete;

public:
    bool SelectFastestBranch(const std::string& rootPath, std::string& branchName) override;
    void StorePorgressIfNeed(const std::string& rootPath, const std::string& branchName, int64_t progress);

protected:
    // virtual for test
    virtual void GetClearFile(const std::string& rootPath, const std::string& branchName,
                              std::vector<std::string>& clearFiles) override;

    bool ParseHinterFile(const std::string& progressStr, int64_t& progress, std::string& branchName);

private:
    static const std::string MERGER_PROGRESS;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergerBranchHinter);

}} // namespace indexlib::merger

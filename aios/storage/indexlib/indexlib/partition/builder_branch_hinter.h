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
#include "indexlib/index_base/common_branch_hinter.h"

namespace indexlib { namespace partition {

class BuilderBranchHinter : public index_base::CommonBranchHinter
{
public:
    using Option = index_base::CommonBranchHinter::Option;
    explicit BuilderBranchHinter(const Option& option);
    ~BuilderBranchHinter();

    BuilderBranchHinter(const BuilderBranchHinter&) = delete;
    BuilderBranchHinter& operator=(const BuilderBranchHinter&) = delete;
    BuilderBranchHinter(BuilderBranchHinter&&) = delete;
    BuilderBranchHinter& operator=(BuilderBranchHinter&&) = delete;

public:
    bool SelectFastestBranch(const std::string& rootPath, std::string& branchName) override;
    void StorePorgressIfNeed(const std::string& rootPath, const std::string& branchName, const std::string& progress);

private:
    void GetClearFile(const std::string& rootPath, const std::string& branchName,
                      std::vector<std::string>& clearFiles) override;
    bool ParseHinterFile(const std::string& currentProgress, uint64_t& src, int64_t& offset, std::string& branchName);

private:
    static const std::string BUILDER_PROGRESS;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuilderBranchHinter);

}} // namespace indexlib::partition

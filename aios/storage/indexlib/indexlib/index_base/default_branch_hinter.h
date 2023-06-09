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

namespace indexlib { namespace index_base {

class DefaultBranchHinter : public CommonBranchHinter
{
public:
    DefaultBranchHinter();
    ~DefaultBranchHinter();

    DefaultBranchHinter(const DefaultBranchHinter&) = delete;
    DefaultBranchHinter& operator=(const DefaultBranchHinter&) = delete;
    DefaultBranchHinter(DefaultBranchHinter&&) = delete;
    DefaultBranchHinter& operator=(DefaultBranchHinter&&) = delete;

public:
    bool SelectFastestBranch(const std::string& rootPath, std::string& branchName) override;
    void GetClearFile(const std::string& rootPath, const std::string& branchName,
                      std::vector<std::string>& clearFiles) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultBranchHinter);

}} // namespace indexlib::index_base

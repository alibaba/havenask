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
#include "indexlib/index_base/common_branch_hinter_option.h"

#include "autil/EnvUtil.h"
#include "autil/Lock.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/Exception.h"
using namespace std;

namespace indexlib::index_base {
AUTIL_LOG_SETUP(indexlib.index_base, CommonBranchHinterOption);

const string CommonBranchHinterOption::BNP_NORMAL = "ts";
const string CommonBranchHinterOption::BNP_LEGACY = "legacy";

CommonBranchHinterOption CommonBranchHinterOption::Normal(uint32_t branchId, const std::string& epochId)
{
    CommonBranchHinterOption option;
    option.branchNamePolicy = autil::EnvUtil::getEnv("BRANCH_NAME_POLICY", BNP_NORMAL);
    assert(option.branchNamePolicy == BNP_LEGACY || option.branchNamePolicy == BNP_NORMAL);
    assert(!epochId.empty() || option.branchNamePolicy == BNP_LEGACY);
    option.branchId = branchId;
    option.epochId = epochId;
    return option;
}

CommonBranchHinterOption CommonBranchHinterOption::Legacy(uint32_t branchId, const std::string& epochId)
{
    CommonBranchHinterOption option;
    option.branchNamePolicy = BNP_LEGACY;
    option.branchId = branchId;
    option.epochId = epochId;
    return option;
}

CommonBranchHinterOption CommonBranchHinterOption::Test(uint32_t branchId)
{
    CommonBranchHinterOption option;
    option.branchId = branchId;
    option.branchNamePolicy = autil::EnvUtil::getEnv("BRANCH_NAME_POLICY", BNP_NORMAL);
    option.epochId = util::EpochIdUtil::TEST_GenerateEpochId();
    return option;
}

} // namespace indexlib::index_base

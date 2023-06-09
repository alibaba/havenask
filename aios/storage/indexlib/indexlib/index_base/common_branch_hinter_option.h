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

#include "autil/Log.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib::index_base {

struct CommonBranchHinterOption {
    // BNP means branch name poilicy
    static const std::string BNP_NORMAL;
    static const std::string BNP_LEGACY;

    // TODO (yiping.typ): remove branchId
    uint32_t branchId = 0;    // branch id (as backup id)
    std::string epochId = ""; // main road epoch, empty means no fence on main road
    std::string branchNamePolicy =
        ""; // branch name policy, help branch hinter generate branch name with @branchId and @epochId

    // for normal mode, @epochId should not be empty
    static CommonBranchHinterOption Normal(uint32_t branchId, const std::string& epochId);

    // for legacy mode, we use @branchId as branch epochid
    static CommonBranchHinterOption Legacy(uint32_t branchId, const std::string& epochId);

    static CommonBranchHinterOption Test(uint32_t branchId = 0);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index_base

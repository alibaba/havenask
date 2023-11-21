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
#include "navi/util/EnvParam.h"

#include "autil/EnvUtil.h"

namespace navi {

static const std::string NAVI_ENV_FORCE_WAIT_NO_QUERY_TIME = "forceWaitNoQueryTime";
static constexpr int64_t DEFAULT_FORCE_WAIT_NO_QUERY_TIME = 0; // 0s

EnvParam::EnvParam() {
    forceWaitNoQueryTime = autil::EnvUtil::getEnv(NAVI_ENV_FORCE_WAIT_NO_QUERY_TIME, DEFAULT_FORCE_WAIT_NO_QUERY_TIME);
}

EnvParam::~EnvParam() {
}

const EnvParam &EnvParam::inst() {
    static EnvParam inst;
    return inst;
}

} // namespace navi

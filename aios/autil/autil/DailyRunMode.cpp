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
#include "autil/DailyRunMode.h"

#include <iosfwd>
#include <stdlib.h>

#include "autil/EnvUtil.h"
#include "autil/Log.h"

using namespace std;

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, DailyRunMode);

const std::string DailyRunMode::DAILY_RUN_MODE("DAILY_RUN_MODE");
bool DailyRunMode::_isEnable = autil::EnvUtil::hasEnv(DAILY_RUN_MODE);
bool DailyRunMode::enable() { return _isEnable; }

} // namespace autil

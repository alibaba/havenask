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
#include "suez/deploy/IndexChecker.h"

#include <cstddef>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace fslib::util;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, IndexChecker);

namespace suez {

static const int CHECK_INDEX_INVERVAL = 10; // 10s

IndexChecker::IndexChecker() {}

IndexChecker::~IndexChecker() {}

void IndexChecker::cancel() {
    ScopedLock lock(_mutex);
    if (_state) {
        *_state = CANCELLED;
        _state.reset();
    }
}

DeployStatus IndexChecker::waitIndexReady(const std::string &checkPath) {
    auto state = std::make_shared<State>(RUNNING);
    {
        ScopedLock lock(_mutex);
        _state = state;
    }
    size_t checkTimes = 0;
    do {
        if (*state == CANCELLED) {
            AUTIL_LOG(INFO, "CheckIndex for [%s] canceled", checkPath.c_str());
            return DS_CANCELLED;
        }
        bool exist = false;
        bool ret = FileUtil::isExist(checkPath, exist);

        if (!ret) {
            AUTIL_LOG(INFO, "determine file[%s] exist failed", checkPath.c_str());
            return DS_FAILED;
        }
        if (exist) {
            AUTIL_LOG(INFO, "CheckIndex [%s] done", checkPath.c_str());
            return DS_DEPLOYDONE;
        }
        if ((++checkTimes) % 10 == 0) {
            AUTIL_LOG(INFO, "CheckIndex waiting for [%s]", checkPath.c_str());
        }
        sleep(CHECK_INDEX_INVERVAL);
    } while (true);
    return DS_FAILED;
}

} // namespace suez

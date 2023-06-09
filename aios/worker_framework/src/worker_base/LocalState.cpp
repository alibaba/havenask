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
#include "worker_framework/LocalState.h"

#include "autil/Log.h"
#include "fslib/fs/FileSystem.h"

namespace worker_framework {
AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework, LocalState);

LocalState::LocalState(std::string stateFile) : _stateFile(std::move(stateFile)) {}

LocalState::~LocalState() {}

WorkerState::ErrorCode LocalState::write(const std::string &content) {
    auto ec = fslib::fs::FileSystem::writeFile(_stateFile, content);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "write %s to %s failed, error code: %d", content.c_str(), _stateFile.c_str(), (int)ec);
        return WorkerState::EC_FAIL;
    }
    return WorkerState::EC_OK;
}

WorkerState::ErrorCode LocalState::read(std::string &content) {
    auto ec = fslib::fs::FileSystem::isExist(_stateFile);
    if (ec == fslib::EC_FALSE) {
        return WorkerState::EC_NOT_EXIST;
    } else if (ec != fslib::EC_TRUE) {
        return WorkerState::EC_FAIL;
    } else {
        ec = fslib::fs::FileSystem::readFile(_stateFile, content);
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "read from %s, error code: %d", _stateFile.c_str(), (int)ec);
            return WorkerState::EC_FAIL;
        }
        return WorkerState::EC_OK;
    }
}

} // namespace worker_framework

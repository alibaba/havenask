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

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "worker_framework/WorkerState.h"

namespace worker_framework {

class ZkState : public WorkerState {
public:
    // stateFile can be: zfs://host/path/to/stateFile
    // or /path/to/stateFile
    ZkState(cm_basic::ZkWrapper *zk, const std::string &stateFile);
    ZkState(cm_basic::ZkWrapper *zk, bool ownZk, const std::string &stateFile);
    ~ZkState();

private:
    ZkState(const ZkState &);
    ZkState &operator=(const ZkState &);

public:
    /* override */ ErrorCode write(const std::string &content);
    /* override */ ErrorCode read(std::string &content);

public:
    // for test
    const std::string &getLastContent() const { return _lastContent; }

public:
    static std::string getZkRelativePath(const std::string &zkPath);

public:
    // zkPath should be in zfs://host/path/to/stateFile format
    static bool write(const std::string &zkPath, const std::string &content);
    static bool read(const std::string &zkPath, std::string &content);

    static bool write(cm_basic::ZkWrapper *zk, const std::string &zkPath, const std::string &content);
    static bool read(cm_basic::ZkWrapper *zk, const std::string &zkPath, std::string &content);

public:
    static ZkState *create(const std::string &stateFile);

private:
    cm_basic::ZkWrapper *_zk;
    bool _ownZk;
    std::string _stateFile;
    std::string _lastContent; // content cache
};

typedef std::shared_ptr<ZkState> ZkStatePtr;

}; // namespace worker_framework

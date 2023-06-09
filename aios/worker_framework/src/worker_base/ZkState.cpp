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
#include "worker_framework/ZkState.h"

#include "autil/Log.h"
#include "worker_framework/PathUtil.h"
#include "worker_framework/compatible.h"

using namespace std;
using namespace worker_framework;

namespace worker_framework {

AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework.worker_base, ZkState);

ZkState::ZkState(cm_basic::ZkWrapper *zk, const string &stateFile) : ZkState(zk, false, stateFile) {}

ZkState::ZkState(cm_basic::ZkWrapper *zk, bool ownZk, const string &stateFile) : _zk(zk), _ownZk(ownZk) {
    _stateFile = getZkRelativePath(stateFile);
}

ZkState::~ZkState() {
    if (_ownZk) {
        delete _zk;
        _zk = nullptr;
    }
}

WorkerState::ErrorCode ZkState::write(const string &content) {
    if (_lastContent == content) {
        return WorkerState::EC_OK;
    }
    if (_ownZk && _zk->isBad()) {
        if (!_zk->open()) {
            AUTIL_LOG(ERROR, "reconnect to zk failed, state file: %s", _stateFile.c_str());
            return EC_FAIL;
        }
    }
    if (!_zk->touch(_stateFile, content, true)) {
        AUTIL_LOG(ERROR, "touch node[%s] failed, zk status: %d", _stateFile.c_str(), (int)_zk->getStatus());
        return EC_FAIL;
    } else {
        _lastContent = content;
        return EC_UPDATE;
    }
}

WorkerState::ErrorCode ZkState::read(string &content) {
    content.clear();
    if (_ownZk && _zk->isBad()) {
        if (!_zk->open()) {
            AUTIL_LOG(ERROR, "reconnect to zk failed, state file: %s", _stateFile.c_str());
            return EC_FAIL;
        }
    }
    ZOO_ERRORS ret = _zk->getData(_stateFile, content);
    if (ZOK != ret) {
        if (ZNONODE == ret) {
            return WorkerState::EC_NOT_EXIST;
        } else {
            AUTIL_LOG(ERROR,
                      "read zk state[%s] failed, error[%s], zk status: %d",
                      _stateFile.c_str(),
                      zerror(ret),
                      (int)_zk->getStatus());
            return WorkerState::EC_FAIL;
        }
    } else {
        if (content != _lastContent) {
            _lastContent = content;
            return WorkerState::EC_UPDATE;
        } else {
            return WorkerState::EC_OK;
        }
    }
}

bool ZkState::write(const string &zkPath, const string &content) {
    string zkHost = PathUtil::getHostFromZkPath(zkPath);
    unique_ptr<cm_basic::ZkWrapper> zk(new cm_basic::ZkWrapper(zkHost));
    if (!zk->open()) {
        return false;
    }
    return write(zk.get(), zkPath, content);
}

bool ZkState::write(cm_basic::ZkWrapper *zk, const string &zkPath, const string &content) {
    ZkState state(zk, zkPath);
    WorkerState::ErrorCode ec = state.write(content);
    return ec != WorkerState::EC_FAIL;
}

bool ZkState::read(const string &zkPath, string &content) {
    string zkHost = PathUtil::getHostFromZkPath(zkPath);
    unique_ptr<cm_basic::ZkWrapper> zk(new cm_basic::ZkWrapper(zkHost));
    if (!zk->open()) {
        return false;
    }
    return read(zk.get(), zkPath, content);
}

bool ZkState::read(cm_basic::ZkWrapper *zk, const string &zkPath, string &content) {
    ZkState state(zk, zkPath);
    WorkerState::ErrorCode ec = state.read(content);
    return ec != WorkerState::EC_FAIL && ec != WorkerState::EC_NOT_EXIST;
}

string ZkState::getZkRelativePath(const string &zkPath) {
    string path = PathUtil::getPathFromZkPath(zkPath);
    return path == "" ? zkPath : path;
}

ZkState *ZkState::create(const std::string &stateFile) {
    string zkHost = PathUtil::getHostFromZkPath(stateFile);
    if (zkHost.empty()) {
        AUTIL_LOG(ERROR, "invalid zk state file name: %s", stateFile.c_str());
        return nullptr;
    }
    std::unique_ptr<cm_basic::ZkWrapper> zk(new cm_basic::ZkWrapper(zkHost));
    if (!zk->open()) {
        return nullptr;
    }
    return new ZkState(zk.release(), true, stateFile);
}

}; // namespace worker_framework

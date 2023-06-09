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
#include "suez/table/ZkVersionSynchronizer.h"

#include "autil/Log.h"
#include "suez/table/TablePathDefine.h"
#include "worker_framework/PathUtil.h"
#include "worker_framework/ZkState.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, ZkVersionSynchronizer);

class ZkStateWrapper final : public VersionState {
public:
    ZkStateWrapper(const std::shared_ptr<cm_basic::ZkWrapper> &zk,
                   const std::string &zkPath,
                   const int64_t readInterval)
        : _zk(zk), _readInterval(readInterval), _lastReadTimestamp(0) {
        _state = std::make_unique<worker_framework::ZkState>(_zk.get(), zkPath);
    }

public:
    ErrorCode write(const std::string &content) override { return _state->write(content); }

    ErrorCode read(std::string &content) override {
        auto now = autil::TimeUtility::currentTimeInSeconds();
        if (now < _lastReadTimestamp + _readInterval) {
            content = _state->getLastContent();
            return EC_OK;
        }
        _lastReadTimestamp = now;
        return _state->read(content);
    }

private:
    std::shared_ptr<cm_basic::ZkWrapper> _zk;
    std::unique_ptr<worker_framework::ZkState> _state;
    const int64_t _readInterval;
    int64_t _lastReadTimestamp;
};

ZkVersionSynchronizer::ZkVersionSynchronizer(const VersionSynchronizerConfig &config) : _config(config) {}

bool ZkVersionSynchronizer::init() {
    auto zk = connectToZk();
    if (zk.get() == nullptr) {
        return false;
    }
    _zkWrapper = std::move(zk);
    return true;
}

void ZkVersionSynchronizer::remove(const PartitionId &pid) {
    autil::ScopedLock lock(_mutex);
    for (size_t i = 0; i < size_t(VST_MAX); ++i) {
        _states.erase(std::make_pair(pid, VersionStateType(i)));
    }
}

bool ZkVersionSynchronizer::supportSyncFromPersist(const PartitionMeta &target) const { return true; }

bool ZkVersionSynchronizer::syncFromPersist(const PartitionId &pid,
                                            const std::string &configPath,
                                            TableVersion &version) {
    static std::string suffix;
    return doSyncFromPersist<TableVersion, false>(pid, VST_VERSION, suffix, version);
}

bool ZkVersionSynchronizer::persistVersion(const PartitionId &pid,
                                           const std::string &localConfigPath,
                                           const TableVersion &version) {
    static std::string suffix;
    return persistToZk(pid, VST_VERSION, suffix, version);
}

VersionState *ZkVersionSynchronizer::createVersionState(const PartitionId &pid, const std::string &fileNameSuffix) {
    // called by mutex hold
    std::string fileName = TablePathDefine::constructPartitionVersionFileName(pid) + fileNameSuffix;
    return new ZkStateWrapper(_zkWrapper, _config.zkRoot + "/" + fileName, _config.syncIntervalInSec);
}

bool ZkVersionSynchronizer::getVersionList(const PartitionId &pid, std::vector<TableVersion> &versions) {
    static std::string suffix = "_list";
    return doSyncFromPersist<std::vector<TableVersion>, true>(pid, VST_VERSION_LIST, suffix, versions);
}

bool ZkVersionSynchronizer::updateVersionList(const PartitionId &pid, const std::vector<TableVersion> &versions) {
    static std::string suffix = "_list";
    return persistToZk(pid, VST_VERSION_LIST, suffix, versions);
}

std::shared_ptr<cm_basic::ZkWrapper> ZkVersionSynchronizer::connectToZk() const {
    std::string zkHost = worker_framework::PathUtil::getHostFromZkPath(_config.zkRoot);
    if (zkHost.empty()) {
        AUTIL_LOG(ERROR, "invalid zkRoot: [%s]", _config.zkRoot.c_str());
        return nullptr;
    }

    auto zkWrapper = std::make_shared<cm_basic::ZkWrapper>(zkHost, _config.zkTimeoutInMs, true);
    if (!zkWrapper->open()) {
        AUTIL_LOG(ERROR, "connect to zk[%s] failed", zkHost.c_str());
        return nullptr;
    }
    return zkWrapper;
}

template <typename T>
bool ZkVersionSynchronizer::persistToZk(const PartitionId &pid,
                                        const VersionStateType &type,
                                        const std::string &suffix,
                                        const T &ctx) {
    auto state = getOrCreateVersionState(pid, suffix, type);
    if (state.get() == nullptr) {
        AUTIL_LOG(ERROR, "create version state for %s failed", ToJsonString(pid, true).c_str());
        return false;
    }
    std::string writeCtx = autil::legacy::ToJsonString(ctx, true);
    auto ret = state->write(writeCtx);
    if (ret == VersionState::EC_FAIL) {
        AUTIL_LOG(ERROR, "partition[%s]: sync version %s failed", ToJsonString(pid, true).c_str(), writeCtx.c_str());
        return false;
    } else {
        if (ret == VersionState::EC_UPDATE) {
            AUTIL_LOG(INFO,
                      "partition[%s]: latest version %s is persisted",
                      ToJsonString(pid, true).c_str(),
                      writeCtx.c_str());
        }
        return true;
    }
}

template <typename T, bool getDataOnNotUpdated>
bool ZkVersionSynchronizer::doSyncFromPersist(const PartitionId &pid,
                                              const VersionStateType &type,
                                              const std::string &suffix,
                                              T &version) {
    auto state = getOrCreateVersionState(pid, suffix, type);
    if (state.get() == nullptr) {
        return false;
    }
    std::string content;
    auto fillCtx = [&](const std::string ctx) {
        try {
            autil::legacy::FromJsonString(version, ctx);
            return true;
        } catch (const std::exception &e) {
            AUTIL_LOG(ERROR, "parse  %s failed, error: %s", ctx.c_str(), e.what());
            return false;
        }
    };

    auto ec = state->read(content);
    if (ec == VersionState::EC_FAIL) {
        return false;
    } else if (ec == VersionState::EC_UPDATE) {
        return fillCtx(content);
    } else if (ec == VersionState::EC_NOT_EXIST) {
        // EC_NOT_EXIST
        return true;
    } else {
        if (getDataOnNotUpdated) {
            return fillCtx(content);
        }
        return true;
    }
}

std::shared_ptr<VersionState> ZkVersionSynchronizer::getOrCreateVersionState(const PartitionId &pid,
                                                                             const std::string &fileNameSuffix,
                                                                             const VersionStateType &type) {
    autil::ScopedLock lock(_mutex);
    if (!_zkWrapper || _zkWrapper->isBad()) {
        AUTIL_LOG(INFO, "connection to zookeeper is bad, try to reconnect...");
        auto zk = connectToZk();
        if (zk.get() == nullptr) {
            AUTIL_LOG(WARN, "reconnect to zookeeper failed");
            return nullptr;
        }
        _zkWrapper = std::move(zk);
        AUTIL_LOG(INFO, "established a new connection to zookeeper, clear original states...");
        _states.clear();
    }
    auto it = _states.find(std::make_pair(pid, type));
    if (it != _states.end()) {
        return it->second;
    }
    std::shared_ptr<VersionState> state(createVersionState(pid, fileNameSuffix));
    if (state.get() != nullptr) {
        _states.emplace(std::make_pair(pid, type), state);
    }
    return state;
}
} // namespace suez

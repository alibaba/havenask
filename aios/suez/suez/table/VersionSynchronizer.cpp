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
#include "suez/table/VersionSynchronizer.h"

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "suez/sdk/PathDefine.h"
#include "suez/table/BsVersionSynchronizer.h"
#include "suez/table/LocalVersionSynchronizer.h"
#include "suez/table/ZkVersionSynchronizer.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, VersionSynchronizer);

bool VersionSynchronizerConfig::initFromEnv() {
    auto configStr = autil::EnvUtil::getEnv("version_sync_config", "");
    std::string zkRoot;
    if (!configStr.empty()) {
        try {
            autil::legacy::FromJsonString(*this, configStr);
            if (!check()) {
                return false;
            }
            zkRoot = this->zkRoot;
        } catch (const std::exception &e) {
            AUTIL_LOG(ERROR, "parse version_sync_config: %s failed, error: %s", configStr.c_str(), e.what());
            return false;
        }
    } else if (autil::EnvUtil::getEnv("localMode", false)) {
        zkRoot = "LOCAL";
    } else {
        zkRoot = autil::EnvUtil::getEnv("zk_root", "");
        if (zkRoot.empty()) {
            AUTIL_LOG(ERROR, "version_sync_config, localMode and zk_root, neither is specified");
            return false;
        }
    }
    constructVersionRoot(zkRoot);
    return true;
}

void VersionSynchronizerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("zk_root", zkRoot, zkRoot);
    json.Jsonize("zk_timeout_in_ms", zkTimeoutInMs, zkTimeoutInMs);
    json.Jsonize("sync_interval_in_sec", syncIntervalInSec, syncIntervalInSec);
    json.Jsonize("sync_from_bs_admin", syncFromBsAdmin, syncFromBsAdmin);
}

bool VersionSynchronizerConfig::check() const { return !zkRoot.empty(); }

void VersionSynchronizerConfig::constructVersionRoot(const std::string &zkRoot) {
    if (zkRoot == "LOCAL" || zkRoot == "BS") {
        this->zkRoot = zkRoot;
    } else {
        this->zkRoot = PathDefine::getVersionStateRoot(zkRoot);
    }
}

VersionSynchronizer::~VersionSynchronizer() {}

VersionSynchronizer *VersionSynchronizer::create() {
    VersionSynchronizerConfig config;
    if (!config.initFromEnv()) {
        return nullptr;
    }
    AUTIL_LOG(INFO, "create ZkVersionSynchronizer with config: %s", ToJsonString(config).c_str());
    if (config.zkRoot == "LOCAL") {
        AUTIL_LOG(INFO, "sync version to local");
        return new LocalVersionSynchronizer();
    }
    if (config.zkRoot == "BS") {
        AUTIL_LOG(INFO, "sync version to bs");
        return new BsVersionSynchronizer(config);
    }
    AUTIL_LOG(INFO, "sync version to zk");
    return new ZkVersionSynchronizer(config);
}

} // namespace suez

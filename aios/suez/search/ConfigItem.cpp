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
#include "suez/search/ConfigItem.h"

#include "autil/StringUtil.h"
#include "suez/deploy/FileDeployer.h"
#include "suez/sdk/PathDefine.h"

namespace suez {

ConfigItem::ConfigItem(const ConfigId &id, const ConfigMeta &meta, DeployManager *deployManager)
    : _id(id)
    , _meta(meta)
    , _deployer(new FileDeployer(deployManager))
    , _keepCount(meta.getKeepCount())
    , _customBizInfo(meta.getCustomBizInfo())
    , _status(DS_UNKNOWN)
    , _error(ERROR_NONE) {}

ConfigItem::~ConfigItem() {}

void ConfigItem::deploy() {
    auto status = _deployer->deploy(_meta.getRemoteConfigPath(), getLocalConfigPath());
    setStatus(status);
    switch (status) {
    case DS_DEPLOYDONE:
    case DS_CANCELLED:
        setError(ERROR_NONE);
        break;
    case DS_DISKQUOTA:
        setError(DEPLOY_DISK_QUOTA_ERROR);
        break;
    default:
        setError(DEPLOY_INDEX_ERROR);
        break;
    }
}

std::string ConfigItem::getLocalConfigPath() const {
    return PathDefine::join(PathDefine::getLocalConfigRoot(_id.name, getDirName()),
                            autil::StringUtil::toString(_id.version)) +
           "/";
}

std::string ConfigItem::getDirName() const { return (_id.type == CT_BIZ) ? PathDefine::BIZ : PathDefine::APP; }

void ConfigItem::cancel() { _deployer->cancel(); }

size_t ConfigItem::getKeepCount() const { return _keepCount; }

void ConfigItem::setKeepCount(size_t keepCount) { _keepCount = keepCount; }

DeployStatus ConfigItem::getStatus() const {
    autil::ScopedLock lock(_mutex);
    return _status;
}

void ConfigItem::setStatus(DeployStatus status) {
    autil::ScopedLock lock(_mutex);
    _status = status;
}

SuezError ConfigItem::getError() const {
    autil::ScopedLock lock(_mutex);
    return _error;
}

void ConfigItem::setError(SuezError error) {
    autil::ScopedLock lock(_mutex);
    _error = error;
}

bool ConfigItem::deployDone() const { return getStatus() == DS_DEPLOYDONE; }

ConfigMeta ConfigItem::getCurrentMeta() const {
    ConfigMeta meta = _meta;
    meta.setLocalConfigPath(getLocalConfigPath());
    meta.setError(getError());
    meta.setKeepCount(getKeepCount());
    meta.setCustomBizInfo(_customBizInfo);
    return meta;
}

void ConfigItem::setCustomBizInfo(const autil::legacy::json::JsonMap &customBizInfo) { _customBizInfo = customBizInfo; }

bool ConfigItem::needDeploy() const { return _meta.needDeploy(); }

} // namespace suez

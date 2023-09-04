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

#include "autil/Lock.h"
#include "suez/common/InnerDef.h"
#include "suez/sdk/BizMeta.h"

namespace suez {

enum ConfigType {
    CT_BIZ,
    CT_APP
};

struct ConfigId {
    std::string name;
    int64_t version;
    ConfigType type;

    ConfigId() = default;

    ConfigId(const std::string &name_, int64_t version_, ConfigType type_)
        : name(name_), version(version_), type(type_) {}

    bool operator<(const ConfigId &rhs) const { return name < rhs.name || (name == rhs.name && version < rhs.version); }
};

using ConfigMeta = BizMeta;
using ConfigMetas = std::map<ConfigId, ConfigMeta>;

class DeployManager;
class FileDeployer;

class ConfigItem {
public:
    ConfigItem(const ConfigId &id, const ConfigMeta &meta, DeployManager *deployManager);
    ~ConfigItem();

public:
    void deploy();
    void cancel();
    size_t getKeepCount() const;
    void setKeepCount(size_t keepCount);
    DeployStatus getStatus() const;
    void setStatus(DeployStatus status);
    SuezError getError() const;
    void setError(SuezError error);
    std::string getDirName() const;
    bool deployDone() const;
    std::string getLocalConfigPath() const;
    ConfigMeta getCurrentMeta() const;
    void setCustomBizInfo(const autil::legacy::json::JsonMap &customBizInfo);
    bool needDeploy() const;

private:
    ConfigId _id;
    ConfigMeta _meta;
    std::unique_ptr<FileDeployer> _deployer;

    // runtime status
    size_t _keepCount;
    autil::legacy::json::JsonMap _customBizInfo;
    DeployStatus _status;
    SuezError _error;

    mutable autil::ThreadMutex _mutex;
};

using ConfigItemPtr = std::shared_ptr<ConfigItem>;

} // namespace suez

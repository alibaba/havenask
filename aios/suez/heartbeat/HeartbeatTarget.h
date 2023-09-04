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

#include <stdint.h>
#include <string>

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "suez/common/InnerDef.h"
#include "suez/common/TableMeta.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/ServiceInfo.h"
#include "worker_framework/DataOption.h"

namespace suez {

class HeartbeatTarget : public autil::legacy::Jsonizable {
public:
    HeartbeatTarget();
    ~HeartbeatTarget();

public:
    bool parseFrom(const std::string &targetStr, const std::string &signature, bool preload = false);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    const autil::legacy::json::JsonMap &getTarget() const { return _target; }
    const std::string &getSignature() const { return _signature; }
    const TableMetas &getTableMetas() const { return _tableMetas; }
    TableMetas &getTableMetas() { return _tableMetas; }
    void setTableMetas(const TableMetas &tableMetas) { _tableMetas = tableMetas; }
    const BizMetas &getBizMetas() const { return _bizMetas; }
    const AppMeta &getAppMeta() const { return _appMeta; }
    const worker_framework::DataOption &getDeployConfig() const { return _deployConfig; }
    const ServiceInfo &getServiceInfo() const { return _serviceInfo; }
    int64_t getTargetVersion() const { return _targetVersion; }

    int64_t getCompatibleTargetVersionTimestamp() const {
        if (_targetVersion != INVALID_TARGET_VERSION) {
            return _targetVersion;
        }
        return 0;
    }
    const autil::legacy::json::JsonMap &getCustomAppInfo() const { return _customAppInfo; }
    bool cleanDisk() const { return _cleanDisk; }
    uint32_t getDiskSize() const { return _diskSize; }

    bool preload() const { return _preload; }

    void setPreload(bool preload) { _preload = preload; }

private:
    bool checkTarget() const;

private:
    autil::legacy::json::JsonMap _target;
    std::string _signature;
    TableMetas _tableMetas;
    BizMetas _bizMetas;
    AppMeta _appMeta;
    worker_framework::DataOption _deployConfig;
    ServiceInfo _serviceInfo;
    autil::legacy::json::JsonMap _customAppInfo;
    int64_t _targetVersion;
    uint32_t _diskSize;
    bool _cleanDisk;
    bool _preload;
};

} // namespace suez

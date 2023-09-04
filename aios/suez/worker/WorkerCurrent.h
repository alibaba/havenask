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

#include <string>

#include "suez/common/TableMeta.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/sdk/SuezError.h"

namespace suez {

class WorkerCurrent {
public:
    WorkerCurrent();

public:
    void setAddress(std::string arpcAddress, std::string httpAddress);
    const std::string &getArpcAddress() const;
    const std::string &getHttpAddress() const;

    int64_t getTargetVersion() const;
    void setTargetVersion(int64_t targetVersion);

    const TableMetas &getTableInfos() const;
    TableMetas *getTableMetas();
    void setTableInfos(const TableMetas &tableInfos);

    const BizMetas &getBizInfos() const;
    void setBizInfos(const BizMetas &bizInfos);

    const AppMeta &getAppMeta() const;
    void setAppMeta(const AppMeta &appMeta);

    const autil::legacy::json::JsonMap &getServiceInfo() const;
    void setServiceInfo(const autil::legacy::json::JsonMap &serviceInfo);

    const ServiceInfo &getZoneInfo() const;
    void setZoneInfo(const ServiceInfo &serviceInfo);

    void setError(const SuezError &error);

    void toHeartbeatFormat(autil::legacy::json::JsonMap &current) const;

    uint64_t calculateSignature() const;

private:
    int64_t _targetVersion;
    std::string _arpcAddress;
    std::string _httpAddress;
    TableMetas _tableInfos;
    BizMetas _bizInfos;
    AppMeta _appInfo;
    autil::legacy::json::JsonMap _serviceInfo; // service info updated by biz
    ServiceInfo _zoneInfo;
    SuezError _errorInfo;
};

} // namespace suez

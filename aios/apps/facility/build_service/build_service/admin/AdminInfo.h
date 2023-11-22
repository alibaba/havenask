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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/util/Singleton.h"

namespace build_service { namespace admin {

class AdminInfo : public indexlib::util::Singleton<AdminInfo>
{
public:
    AdminInfo() : _adminHttpPort(0) {}
    ~AdminInfo() {}

private:
    AdminInfo(const AdminInfo&);
    AdminInfo& operator=(const AdminInfo&);

public:
    void setAdminAddress(const std::string& address) { _adminAddress = address; }
    void setAdminHttpPort(const uint16_t& port) { _adminHttpPort = port; }
    std::string getAdminAddress() const { return _adminAddress; }
    uint32_t getAdminHttpPort() const { return _adminHttpPort; }

private:
    std::string _adminAddress;
    uint32_t _adminHttpPort;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AdminInfo);

}} // namespace build_service::admin

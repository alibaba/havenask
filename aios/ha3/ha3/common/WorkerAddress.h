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
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {

class WorkerAddress
{
public:
    WorkerAddress(const std::string &address);
    WorkerAddress(const std::string &ip, uint16_t port);
    ~WorkerAddress();
public:
    const std::string& getAddress() const {return _address;}
    const std::string& getIp() const {return _ip;}
    uint16_t getPort() const {return _port;}
private:
    std::string _address;
    std::string _ip;
    uint16_t _port;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<WorkerAddress> WorkerAddressPtr;

} // namespace common
} // namespace isearch


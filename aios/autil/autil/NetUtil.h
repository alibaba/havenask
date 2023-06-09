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
#include <vector>

#include "autil/Log.h"

namespace autil {

class NetUtil
{
public:
    static bool GetIp(std::vector<std::string>& ips);
    static bool GetDefaultIp(std::string& ip);
    static bool GetHostName(std::string& hostname);
    static uint16_t randomPort();
    static std::string inetNtoa(int32_t ip);
    static std::string getBindIp();
    static bool isMacAddr(const std::string& addr);
    static bool GetMachineName(std::string& host);

private:
    static uint16_t getPort(uint16_t from = 1025, uint16_t to = 60001);
private:
    static const std::string HOST_INFO_PATH;
private:
    AUTIL_LOG_DECLARE();
};

}

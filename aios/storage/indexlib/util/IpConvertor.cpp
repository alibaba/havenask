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
#include "indexlib/util/IpConvertor.h"

#include <memory>
#include <stdio.h>
#include <vector>

#include "autil/StringUtil.h"

namespace indexlibv2::util {

bool IpConvertor::IpToNum(const std::string& ipStr, uint32_t& num)
{
    auto strs = autil::StringUtil::split(ipStr, ".", true);
    if (strs.size() != 4) {
        return false;
    }
    uint32_t ip = 0;
    for (size_t i = 0; i < strs.size(); i++) {
        uint8_t value;
        if (!autil::StringUtil::strToUInt8(strs[i].data(), value)) {
            return false;
        }
        switch (i) {
        case 0:
            ip += value << 24;
            break;
        case 1:
            ip += value << 16;
            break;
        case 2:
            ip += value << 8;
            break;
        case 3:
            ip += value;
            break;
        default:
            return false;
        };
    }
    num = ip;
    return true;
}

bool IpConvertor::IpPortToNum(const std::string& ipPort, uint64_t& num)
{
    auto strs = autil::StringUtil::split(ipPort, ":", true);
    if (strs.size() != 2) {
        return false;
    }
    uint16_t port = 0;
    if (!autil::StringUtil::strToUInt16(strs[1].data(), port)) {
        return false;
    }
    uint32_t ip = 0;
    if (!IpToNum(strs[0], ip)) {
        return false;
    }
    num = 0;
    num |= ip;
    num <<= 16;
    num |= port;
    return true;
}

std::string IpConvertor::NumToIp(uint32_t ip)
{
    char strTemp[17];
    snprintf(strTemp, 17, "%d.%d.%d.%d", (ip & 0xff000000) >> 24, (ip & 0x00ff0000) >> 16, (ip & 0x0000ff00) >> 8,
             (ip & 0x000000ff));
    return std::string(strTemp);
}

std::string IpConvertor::NumToIpPort(uint64_t addr)
{
    uint32_t ip = addr >> 16;
    uint16_t port = (addr & 0x000000000000ffff);
    return std::string(NumToIp(ip)) + ":" + std::to_string(port);
}

} // namespace indexlibv2::util

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
#include <utility>
#include <vector>

struct in_addr;

namespace isearch {
namespace util {

class NetFunction {
public:
    typedef std::vector<std::pair<std::string, in_addr>> AllIPType;
    static bool getAllIP(AllIPType &ips);
    static std::string chooseIP(const AllIPType &ips);
    static bool getPrimaryIP(std::string &ip);
    static bool containIP(const std::string &msg, const std::string &ip);
    static uint32_t encodeIp(const std::string &ip);
};

} // namespace util
} // namespace isearch

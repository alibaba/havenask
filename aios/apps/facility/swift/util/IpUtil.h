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

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace util {

class IpUtil {
public:
    IpUtil();
    ~IpUtil();

private:
    IpUtil(const IpUtil &);
    IpUtil &operator=(const IpUtil &);

public:
    static std::string getIp();

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(IpUtil);

} // end namespace util
} // end namespace swift

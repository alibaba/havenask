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

#include "swift/common/Common.h"

namespace swift {
namespace auth {

class ClientAuthorizerInfo {
public:
    ClientAuthorizerInfo() {}
    ClientAuthorizerInfo(const std::string &userName, const std::string &passWD) : username(userName), passwd(passWD) {}

public:
    bool isEmpty() { return username.empty() || passwd.empty(); }

public:
    std::string username;
    std::string passwd;
};
SWIFT_TYPEDEF_PTR(ClientAuthorizerInfo);

} // namespace auth
} // end namespace swift

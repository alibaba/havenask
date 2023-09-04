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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "swift/common/Common.h"

namespace swift {
namespace config {
class SwiftConfig : public autil::legacy::Jsonizable {
public:
    SwiftConfig();
    ~SwiftConfig();

public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

public:
    bool validate() const;
    std::string getApplicationId() const;

public:
    std::string userName;
    std::string serviceName;
    std::string hippoRoot;
    std::string zkRoot;
    uint16_t amonitorPort;

private:
    static const uint16_t DEFAULT_AMONITOR_PORT = 10086;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftConfig);

} // namespace config
} // namespace swift

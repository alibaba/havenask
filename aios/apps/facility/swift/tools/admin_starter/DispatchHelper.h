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

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace config {
class AdminConfig;
} // namespace config
} // namespace swift

namespace swift {
namespace tools {

class DispatchHelper {
public:
    DispatchHelper();
    virtual ~DispatchHelper();

private:
    DispatchHelper(const DispatchHelper &);
    DispatchHelper &operator=(const DispatchHelper &);

public:
    virtual bool init(config::AdminConfig *config);
    virtual bool startApp() = 0;
    virtual bool stopApp() = 0;
    virtual bool updateAdminConf() = 0;
    virtual bool getAppStatus() = 0;

protected:
    bool writeVersionFile();
    bool updateAdminVersion();

protected:
    config::AdminConfig *_config;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(DispatchHelper);

} // namespace tools
} // namespace swift

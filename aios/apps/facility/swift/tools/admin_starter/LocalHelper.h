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
#include "swift/tools/admin_starter/DispatchHelper.h"

namespace swift {
namespace tools {

class LocalHelper : public DispatchHelper {
public:
    LocalHelper();
    LocalHelper(const std::string &binPath, const std::string &workDir);
    virtual ~LocalHelper();

private:
    LocalHelper(const LocalHelper &);
    LocalHelper &operator=(const LocalHelper &);

public:
    bool startApp() override;
    bool stopApp() override;
    bool updateAdminConf() override;
    bool getAppStatus() override;

private:
    bool startLocalAdminDaemon(const std::string &swiftDir);

private:
    bool appExist();

private:
    std::string _binPath;
    std::string _workDir;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace tools
} // namespace swift

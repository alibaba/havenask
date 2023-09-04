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

namespace suez {

class AdminCmdLog {
public:
    AdminCmdLog();
    ~AdminCmdLog();

private:
    AdminCmdLog(const AdminCmdLog &);
    AdminCmdLog &operator=(const AdminCmdLog &);

public:
    void setOperateCmd(const std::string &operateCmd) { _operateCmd = operateCmd; }

    void setRequest(const std::string &request) { _request = request; }

    void setSuccess(bool success) { _success = success; }
    void setCost(int32_t cost) { _cost = cost; }

private:
    void log();

private:
    bool _success;
    std::string _operateCmd;
    std::string _request;
    int32_t _cost;
};

} // namespace suez

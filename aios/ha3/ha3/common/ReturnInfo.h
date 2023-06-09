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

#include "ha3/common/ErrorDefine.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {

class ReturnInfo
{
public:
    ReturnInfo(ErrorCode c = ERROR_NONE, const std::string &msg = "");
    ~ReturnInfo();
public:
    operator bool() const {
        return code == ERROR_NONE;
    }
    bool operator! () const {
        return code != ERROR_NONE;
    }
public:
    void fail(const std::string &message = "",
              ErrorCode c = ERROR_NORMAL_FAILED)
    {
        if (!message.empty()) {
            errorMsg += message;
        }
        code = c;
    }
public:
    ErrorCode code;
    std::string errorMsg;
};

#define AUTIL_LOG_AND_FAILD(ret, errorMsg)         \
    string errorMessage = errorMsg;              \
    ret.fail(errorMessage);                      \
    AUTIL_LOG(ERROR, "%s", errorMessage.c_str())

} // namespace common
} // namespace isearch

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

#include <exception>
#include <string>

#include "iquan/common/Common.h"

namespace iquan {

class IquanException : public std::exception {
public:
    IquanException(const std::string reason, int code = IQUAN_FAIL)
        : _reason(reason)
        , _code(code) {}

    virtual const char *what() const throw() {
        return _reason.c_str();
    }

    int code() const {
        return _code;
    }

private:
    std::string _reason;
    int _code;
};

} // namespace iquan

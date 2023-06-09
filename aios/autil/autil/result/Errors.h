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

#include <cstdarg>
#include <cstdio>
#include <string>
#include <system_error>

#include "autil/result/Result.h"

namespace autil::result {

class RuntimeError : public Error {
public:
    RuntimeError(std::string &&msg) : _msg{msg} {}

    std::string message() const override { return _msg; }

    std::unique_ptr<Error> clone() const override { return std::make_unique<RuntimeError>(*this); }

    static auto make(std::string msg) { return std::unique_ptr<RuntimeError>(new RuntimeError(std::move(msg))); }

    [[gnu::format(printf, 1, 2)]] static auto make(const char *fmt, ...) {
        va_list ap1;
        va_start(ap1, fmt);
        int n = ::vsnprintf(nullptr, 0, fmt, ap1);
        va_end(ap1);
        std::string msg(n, 0);
        va_list ap2;
        va_start(ap2, fmt);
        ::vsnprintf(msg.data(), n + 1, fmt, ap2);
        va_end(ap2);
        return make(std::move(msg));
    }

protected:
    std::string _msg;
};

class SystemError : public RuntimeError {
public:
    SystemError(std::error_code ec) : RuntimeError{ec.message()}, _ec{ec} {}

    std::unique_ptr<Error> clone() const override { return std::make_unique<SystemError>(*this); }

    static auto make(std::error_code ec) { return std::make_unique<SystemError>(ec); }

protected:
    std::error_code _ec;
};

}    // namespace autil::result

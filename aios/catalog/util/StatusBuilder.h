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

#include <sstream>

#include "catalog/proto/CatalogService.pb.h"

namespace catalog {

#define CATALOG_CHECK_OK(s)                                                                                            \
    do {                                                                                                               \
        const auto &__s = (s);                                                                                         \
        if (!isOk(__s)) {                                                                                              \
            return __s;                                                                                                \
        }                                                                                                              \
    } while (0)

#define CATALOG_CHECK(EXP, ...)                                                                                        \
    do {                                                                                                               \
        if (!(EXP)) {                                                                                                  \
            return ::catalog::StatusBuilder::make(__VA_ARGS__);                                                        \
        }                                                                                                              \
    } while (0)

// 如果response中已经包含错误时，不再更新覆盖
#define CATALOG_REQUIRES_OK(RESPONSE, STATUS)                                                                          \
    do {                                                                                                               \
        auto &&__s = (STATUS);                                                                                         \
        auto *__response = (RESPONSE);                                                                                 \
        if (!isOk(__s)) {                                                                                              \
            if (!__response->has_status() || isOk(__response->status())) {                                             \
                __response->mutable_status()->Swap(&__s);                                                              \
            }                                                                                                          \
            return;                                                                                                    \
        }                                                                                                              \
    } while (0)

// 如果response中已经包含错误时，不再更新覆盖
#define CATALOG_REQUIRES(RESPONSE, EXP, ...)                                                                           \
    do {                                                                                                               \
        auto *__response = (RESPONSE);                                                                                 \
        if (!(EXP)) {                                                                                                  \
            if (!__response->has_status() || isOk(__response->status())) {                                             \
                ::catalog::StatusBuilder builder(__response->mutable_status());                                        \
                builder.build(__VA_ARGS__);                                                                            \
            }                                                                                                          \
            return;                                                                                                    \
        }                                                                                                              \
    } while (0)

using Status = ::catalog::proto::ResponseStatus;

bool isOk(const Status &status);

class StatusBuilder {
public:
    explicit StatusBuilder(Status *status);

public:
    void setOk() { build(Status::OK); }

    template <typename... Args>
    void build(Status::Code code, Args &&...args) {
        std::stringstream ss;
        buildImpl(ss, code, std::forward<Args>(args)...);
    }

public:
    static Status ok() { return make(Status::OK); }

    template <typename... Args>
    static Status make(Status::Code code, Args &&...args) {
        Status ret;
        StatusBuilder builder(&ret);
        builder.build(code, std::forward<Args>(args)...);
        return ret;
    }

private:
    template <typename Arg, typename... Args>
    void buildImpl(std::stringstream &ss, Status::Code code, Arg &&arg, Args &&...args) {
        ss << arg;
        buildImpl(ss, code, std::forward<Args>(args)...);
    }

    void buildImpl(std::stringstream &ss, Status::Code code) {
        _status->set_code(code);
        if (code != Status::OK) {
            auto msg = ss.str();
            if (!msg.empty()) {
                _status->set_message(msg);
            } else {
                _status->set_message(proto::ResponseStatus_Code_Name(code));
            }
        }
    }

private:
    Status *_status;
};

} // namespace catalog

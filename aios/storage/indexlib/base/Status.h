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

#include <cstdio>
#include <cstring>
#include <inttypes.h>
#include <sstream>
#include <stdarg.h>
#include <string>

#include "autil/StringView.h"
#include "autil/result/Errors.h"
#include "autil/result/Result.h"

namespace indexlib {
namespace detail {

enum class StatusCode : uint8_t {
    // Status code for indexlib's users, starting from 0 to 128.
    EC_OK = 0,
    EC_CORRUPTION = 1,
    EC_IOERROR = 2,
    EC_NOMEM = 3,
    EC_CONFIGERROR = 4,
    EC_INVALID_ARGS = 5,
    EC_INTERNAL = 6,
    EC_OUT_OF_RANGE = 7,
    // EC_NOENT = 8,
    EC_ABORT = 9,
    EC_EOF = 10,
    EC_UNKNOWN = 128,
    // Status code for indexlib's internal use, starting from 129.
    EC_NEED_DUMP = 129,
    EC_UNIMPLEMENT = 130,
    EC_UNINITIALIZE = 131,
    EC_NOT_FOUND = 132,
    EC_EXPIRED = 133,
    EC_EXIST = 134,
    EC_SEALED = 135,
};

//////////////////////////////////////////////////////////////////////
struct StatusError : public autil::result::RuntimeError {
public:
    StatusCode code;

    StatusError(StatusCode code, std::string&& msg) : autil::result::RuntimeError {std::move(msg)}, code {code} {}

    std::string message() const override;

    std::unique_ptr<autil::result::Error> clone() const override { return std::make_unique<StatusError>(*this); }
};

//////////////////////////////////////////////////////////////////////
template <typename Self>
class StatusApi
{
public:
    bool IsOK() const { return self()->is_ok(); }

    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    std::string ToString() const { return self()->is_ok() ? "OK" : self()->get_error().message(); }

public:
    bool IsCorruption() const { return self()->code() == StatusCode::EC_CORRUPTION; }
    bool IsIOError() const { return self()->code() == StatusCode::EC_IOERROR; }
    bool IsNoMem() const { return self()->code() == StatusCode::EC_NOMEM; }
    bool IsAbort() const { return self()->code() == StatusCode::EC_ABORT; }
    bool IsUnknown() const { return self()->code() == StatusCode::EC_UNKNOWN; }
    bool IsConfigError() const { return self()->code() == StatusCode::EC_CONFIGERROR; }
    bool IsInvalidArgs() const { return self()->code() == StatusCode::EC_INVALID_ARGS; }
    bool IsInternalError() const { return self()->code() == StatusCode::EC_INTERNAL; }
    bool IsOutOfRange() const { return self()->code() == StatusCode::EC_OUT_OF_RANGE; }
    bool IsEof() const { return self()->code() == StatusCode::EC_EOF; }
    // bool IsNoEntry() const { return self()->code() == StatusCode::EC_NOENT; }

    bool IsNeedDump() const { return self()->code() == StatusCode::EC_NEED_DUMP; }
    bool IsUnimplement() const { return self()->code() == StatusCode::EC_UNIMPLEMENT; }
    bool IsUninitialize() const { return self()->code() == StatusCode::EC_UNINITIALIZE; }
    bool IsNotFound() const { return self()->code() == StatusCode::EC_NOT_FOUND; }
    bool IsExpired() const { return self()->code() == StatusCode::EC_EXPIRED; }
    bool IsExist() const { return self()->code() == StatusCode::EC_EXIST; }
    bool IsSealed() const { return self()->code() == StatusCode::EC_SEALED; }

    bool IsOKOrNotFound() const { return IsOK() || IsNotFound(); }

private:
    StatusCode code() const
    {
        if (self()->is_ok()) {
            return StatusCode::EC_OK;
        }
        if (auto e = self()->template as<StatusError>()) {
            return e->code;
        }
        return StatusCode::EC_UNKNOWN;
    }

    const Self* self() const noexcept { return static_cast<const Self*>(this); }
    Self* self() noexcept { return static_cast<Self*>(this); }
};

//////////////////////////////////////////////////////////////////////
class ErrorStatus : public autil::result::ErrorResult<StatusError>, public StatusApi<ErrorStatus>
{
public:
    using Super = autil::result::ErrorResult<StatusError>;
    using Super::Super;

    ErrorStatus(ErrorStatus&& other) = default;
    ErrorStatus(const ErrorStatus& other) : Super(other.clone()) {}
    ErrorStatus& operator=(ErrorStatus&& other) = default;
    ErrorStatus& operator=(const ErrorStatus& other)
    {
        if (&other == this)
            return *this;
        this->~ErrorStatus();
        return *new (this) ErrorStatus(other);
    }
};

//////////////////////////////////////////////////////////////////////
template <typename T = void>
class [[nodiscard]] Status : public autil::result::Result<T>, public StatusApi<Status<T>>
{
public:
    using Super = autil::result::Result<T>;
    using Super::Super;

    Status(ErrorStatus&& other, const autil::SourceLocation& loc = autil::SourceLocation::current())
        : Super(other.steal_error(), loc)
    {
    }
    Status(const ErrorStatus& other) : Super(other.clone()) {}

    Status(Status&& other) = default;
    Status(const Status& other, const autil::SourceLocation& loc = autil::SourceLocation::current())
        : Super(other.clone(loc))
    {
    }
    Status& operator=(Status&& other) = default;
    Status& operator=(const Status& other)
    {
        if (&other == this)
            return *this;
        this->~Status();
        return *new (this) Status(other);
    }

public:
    // derived methods in Result<T>(autil/result/Result.h):
    // is_ok, is_err, get, get_err, steal_value, steal_error

public:
    // Return a success status.
    static Status OK() { return Status(); }

    // NAME() or NAME(const char* fmt, ...)
#define IB_STATUS_IMPL_STATUS_CODE_(NAME, EC)                                                                          \
    static ErrorStatus NAME() { return std::make_unique<StatusError>(StatusCode::EC, ""); }                            \
    static ErrorStatus NAME(const char* fmt, ...)                                                                      \
    {                                                                                                                  \
        va_list ap;                                                                                                    \
        va_start(ap, fmt);                                                                                             \
        char buffer[2048];                                                                                             \
        vsnprintf(buffer, sizeof(buffer), fmt, ap);                                                                    \
        va_end(ap);                                                                                                    \
        return std::make_unique<StatusError>(StatusCode::EC, std::string(buffer));                                     \
    }

    IB_STATUS_IMPL_STATUS_CODE_(Corruption, EC_CORRUPTION);
    IB_STATUS_IMPL_STATUS_CODE_(IOError, EC_IOERROR);
    IB_STATUS_IMPL_STATUS_CODE_(NoMem, EC_NOMEM);
    IB_STATUS_IMPL_STATUS_CODE_(Abort, EC_ABORT);
    IB_STATUS_IMPL_STATUS_CODE_(Unknown, EC_UNKNOWN);
    IB_STATUS_IMPL_STATUS_CODE_(ConfigError, EC_CONFIGERROR);
    IB_STATUS_IMPL_STATUS_CODE_(InvalidArgs, EC_INVALID_ARGS);
    IB_STATUS_IMPL_STATUS_CODE_(InternalError, EC_INTERNAL);
    IB_STATUS_IMPL_STATUS_CODE_(OutOfRange, EC_OUT_OF_RANGE);
    IB_STATUS_IMPL_STATUS_CODE_(Eof, EC_EOF);
    // IB_STATUS_IMPL_STATUS_CODE_(NoEntry, EC_NOENT);

    IB_STATUS_IMPL_STATUS_CODE_(NeedDump, EC_NEED_DUMP);
    IB_STATUS_IMPL_STATUS_CODE_(Unimplement, EC_UNIMPLEMENT);
    IB_STATUS_IMPL_STATUS_CODE_(Uninitialize, EC_UNINITIALIZE);
    IB_STATUS_IMPL_STATUS_CODE_(NotFound, EC_NOT_FOUND);
    IB_STATUS_IMPL_STATUS_CODE_(Expired, EC_EXPIRED);
    IB_STATUS_IMPL_STATUS_CODE_(Exist, EC_EXIST);
    IB_STATUS_IMPL_STATUS_CODE_(Sealed, EC_SEALED);

#undef IB_STATUS_IMPL_STATUS_CODE_
};
} // namespace detail

using Status = detail::Status<void>;
template <typename T>
using StatusOr = detail::Status<T>;

#define RETURN_STATUS_DIRECTLY_IF_ERROR(status) AR_RET_IF_ERR(status)

#define RETURN_IF_STATUS_ERROR(statusOrFunc, format, args...)                                                          \
    AR_RET_IF_ERR_IMPL_(statusOrFunc, AUTIL_LOG(ERROR, format " status[%s]", ##args, __e->message().c_str()))

#define RETURN2_IF_STATUS_ERROR(statusOrFunc, result, format, args...)                                                 \
    do {                                                                                                               \
        indexlib::Status __status__ = (statusOrFunc);                                                                  \
        if (!__status__.IsOK()) {                                                                                      \
            AUTIL_LOG(ERROR, format " status[%s]", ##args, __status__.ToString().c_str());                             \
            return std::make_pair(std::move(__status__), result);                                                      \
        }                                                                                                              \
    } while (0)

#define RETURN2_STATUS_DIRECTLY_IF_ERROR(status, result)                                                               \
    do {                                                                                                               \
        indexlib::Status __status__ = (status);                                                                        \
        if (!__status__.IsOK()) {                                                                                      \
            return std::make_pair(std::move(__status__), result);                                                      \
        }                                                                                                              \
    } while (0)

#define RETURN3_IF_STATUS_ERROR(statusOrFunc, result1, result2, format, args...)                                       \
    do {                                                                                                               \
        indexlib::Status __status__ = (statusOrFunc);                                                                  \
        if (!__status__.IsOK()) {                                                                                      \
            AUTIL_LOG(ERROR, format " status[%s] ", ##args, __status__.ToString().c_str());                            \
            return std::make_tuple(std::move(__status__), result1, result2);                                           \
        }                                                                                                              \
    } while (0)

#define STATUS_MACROS_IMPL_CONCAT_INNER_(x, y) x##y
#define STATUS_MACROS_IMPL_CONCAT_(x, y) STATUS_MACROS_IMPL_CONCAT_INNER_(x, y)

#define ASSIGN_OR_RETURN(__lhs, __rhs)                                                                                 \
    ASSIGN_OR_RETURN_IMPL(STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__), __lhs, __rhs)

#define ASSIGN_OR_RETURN_IMPL(statusor, __lhs, __rhs)                                                                  \
    auto statusor = (__rhs);                                                                                           \
    if (!statusor.first.IsOK()) {                                                                                      \
        return statusor.first;                                                                                         \
    }                                                                                                                  \
    __lhs = std::move(statusor.second);

#define RETURN_STATUS_ERROR(ERROR_TYPE, format, args...)                                                               \
    do {                                                                                                               \
        auto err = indexlib::Status::ERROR_TYPE(format, ##args);                                                       \
        AUTIL_LOG(ERROR, " status[%s]", err.ToString().c_str());                                                       \
        return err;                                                                                                    \
    } while (0)

} // namespace indexlib

// TODO: rm
namespace indexlibv2 {
using indexlib::Status;
using indexlib::StatusOr;
} // namespace indexlibv2

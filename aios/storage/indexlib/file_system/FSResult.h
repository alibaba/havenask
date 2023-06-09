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

#include "indexlib/base/Status.h"
#include "indexlib/file_system/ErrorCode.h"

namespace indexlib { namespace file_system {

template <typename Result>
struct [[nodiscard]] FSResult {
    ErrorCode ec = FSEC_UNKNOWN;
    Result result;

    FSResult() noexcept = default;
    FSResult(ErrorCode ec_, const Result& result_) noexcept : ec(ec_), result(result_) {}
    FSResult(ErrorCode ec_, Result&& result_) noexcept : ec(ec_), result(std::move(result_)) {}

    const Result& GetOrThrow(const std::string& path = "", const std::string& msg = "") const noexcept(false)
    {
        THROW_IF_FS_ERROR(ec, "path[%s], msg[%s]", path.c_str(), msg.c_str());
        return Value();
    }
    Result&& GetOrThrow(const std::string& path = "", const std::string& msg = "") noexcept(false)
    {
        THROW_IF_FS_ERROR(ec, "path[%s], msg[%s]", path.c_str(), msg.c_str());
        return Value();
    }
    bool OK() const noexcept { return ec == FSEC_OK; }
    ErrorCode Code() const noexcept { return ec; }
    const Result& Value() const noexcept { return result; }
    Result&& Value() noexcept { return std::forward<Result>(result); }
    std::pair<ErrorCode, Result> CodeWith() const noexcept { return {ec, result}; }
    indexlib::Status Status() const noexcept { return toStatus(ec); }
    std::pair<indexlib::Status, Result> StatusWith() const noexcept { return {toStatus(ec), result}; }
};

template <>
struct [[nodiscard]] FSResult<void> {
    ErrorCode ec = FSEC_UNKNOWN;

    FSResult() noexcept = default;
    FSResult(ErrorCode ec_) noexcept : ec(ec_) {}
    void GetOrThrow(const std::string& path = "", const std::string& msg = "") const noexcept(false)
    {
        THROW_IF_FS_ERROR(ec, "path[%s], msg[%s]", path.c_str(), msg.c_str());
    }
    operator ErrorCode() noexcept { return ec; } // for compitable with ErrorCode
    bool OK() const noexcept { return ec == FSEC_OK; }
    ErrorCode Code() const noexcept { return ec; }
    indexlib::Status Status() const noexcept { return toStatus(ec); }
};
// for compitable with ErrorCode
inline bool operator==(const ErrorCode& ec, const FSResult<void>& ret) { return ec == ret.ec; }
inline bool operator!=(const ErrorCode& ec, const FSResult<void>& ret) { return ec != ret.ec; }
inline bool operator==(const FSResult<void>& ret, const ErrorCode& ec) { return ec == ret.ec; }
inline bool operator!=(const FSResult<void>& ret, const ErrorCode& ec) { return ec != ret.ec; }

}} // namespace indexlib::file_system

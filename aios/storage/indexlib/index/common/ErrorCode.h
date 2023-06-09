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

#include <memory>

#include "fslib/fslib.h"
#include "indexlib/base/Define.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

enum class ErrorCode : int {
    OK = 0,
    BufferOverflow = util::BufferOverflow,
    FileIO = util::FileIO,
    AssertEqual = util::AssertEqual,
    UnImplement = util::UnImplement,
    Schema = util::Schema,
    InitializeFailed = util::InitializeFailed,
    OutOfRange = util::OutOfRange,
    IndexCollapsed = util::IndexCollapsed,
    InconsistentState = util::InconsistentState,
    Runtime = util::Runtime,
    NonExist = util::NonExist,
    Duplicate = util::Duplicate,
    InvalidVersion = util::InvalidVersion,
    SegmentFile = util::SegmentFile,
    BadParameter = util::BadParameter,
    DumpingThread = util::DumpingThread,
    UnSupported = util::UnSupported,
    OutOfMemory = util::OutOfMemory,
    AssertCompatible = util::AssertCompatible,
    ReachMaxIndexSize = util::ReachMaxIndexSize,
    ReachMaxResource = util::ReachMaxResource,
    Exist = util::Exist,
    MemFileIO = util::MemFileIO,
    DocumentDeserialize = util::DocumentDeserialize,
    Timeout = util::Timeout,
};
typedef std::vector<ErrorCode> ErrorCodeVec;

inline std::string ErrorCodeToString(ErrorCode ec)
{
    switch (ec) {
    case ErrorCode::OK:
        return "OK";
    case ErrorCode::BufferOverflow:
        return "BufferOverflow";
    case ErrorCode::FileIO:
        return "FileIO";
    case ErrorCode::AssertEqual:
        return "AssertEqual";
    case ErrorCode::UnImplement:
        return "UnImplement";
    case ErrorCode::Schema:
        return "Schema";
    case ErrorCode::InitializeFailed:
        return "InitializeFailed";
    case ErrorCode::OutOfRange:
        return "OutOfRange";
    case ErrorCode::IndexCollapsed:
        return "IndexCollapsed";
    case ErrorCode::InconsistentState:
        return "InconsistentState";
    case ErrorCode::Runtime:
        return "Runtime";
    case ErrorCode::NonExist:
        return "NonExist";
    case ErrorCode::Duplicate:
        return "Duplicate";
    case ErrorCode::InvalidVersion:
        return "InvalidVersion";
    case ErrorCode::SegmentFile:
        return "SegmentFile";
    case ErrorCode::BadParameter:
        return "BadParameter";
    case ErrorCode::DumpingThread:
        return "DumpingThread";
    case ErrorCode::UnSupported:
        return "UnSupported";
    case ErrorCode::OutOfMemory:
        return "OutOfMemory";
    case ErrorCode::AssertCompatible:
        return "AssertCompatible";
    case ErrorCode::ReachMaxIndexSize:
        return "ReachMaxIndexSize";
    case ErrorCode::ReachMaxResource:
        return "ReachMaxResource";
    case ErrorCode::Exist:
        return "Exist";
    case ErrorCode::MemFileIO:
        return "MemFileIO";
    case ErrorCode::DocumentDeserialize:
        return "DocumentDeserialize";
    case ErrorCode::Timeout:
        return "Timeout";
    default:
        return "Unknown";
    }
}

inline ErrorCode ConvertFSErrorCode(file_system::ErrorCode fsec)
{
    ErrorCode ec;
    switch (fsec) {
    case file_system::ErrorCode::FSEC_OPERATIONTIMEOUT:
        ec = ErrorCode::Timeout;
        break;
    case file_system::ErrorCode::FSEC_OK:
        ec = ErrorCode::OK;
        break;
    default:
        ec = ErrorCode::FileIO;
    }
    return ec;
}

// TODO: should not throw, rm this
#define INDEXLIB_ERROR_CODE_THROW_CASE(ec)                                                                             \
    case index::ErrorCode::ec:                                                                                         \
        AUTIL_LEGACY_THROW(util::ExceptionSelector<util::ec>::ExceptionType, "")

__NOINLINE inline void ThrowThisError(index::ErrorCode ec)
{
    switch (ec) {
        INDEXLIB_ERROR_CODE_THROW_CASE(BufferOverflow);
        INDEXLIB_ERROR_CODE_THROW_CASE(FileIO);
        INDEXLIB_ERROR_CODE_THROW_CASE(AssertEqual);
        INDEXLIB_ERROR_CODE_THROW_CASE(UnImplement);
        INDEXLIB_ERROR_CODE_THROW_CASE(Schema);
        INDEXLIB_ERROR_CODE_THROW_CASE(InitializeFailed);
        INDEXLIB_ERROR_CODE_THROW_CASE(OutOfRange);
        INDEXLIB_ERROR_CODE_THROW_CASE(IndexCollapsed);
        INDEXLIB_ERROR_CODE_THROW_CASE(InconsistentState);
        INDEXLIB_ERROR_CODE_THROW_CASE(Runtime);
        INDEXLIB_ERROR_CODE_THROW_CASE(NonExist);
        INDEXLIB_ERROR_CODE_THROW_CASE(Duplicate);
        INDEXLIB_ERROR_CODE_THROW_CASE(InvalidVersion);
        INDEXLIB_ERROR_CODE_THROW_CASE(SegmentFile);
        INDEXLIB_ERROR_CODE_THROW_CASE(BadParameter);
        INDEXLIB_ERROR_CODE_THROW_CASE(DumpingThread);
        INDEXLIB_ERROR_CODE_THROW_CASE(UnSupported);
        INDEXLIB_ERROR_CODE_THROW_CASE(OutOfMemory);
        INDEXLIB_ERROR_CODE_THROW_CASE(AssertCompatible);
        INDEXLIB_ERROR_CODE_THROW_CASE(ReachMaxIndexSize);
        INDEXLIB_ERROR_CODE_THROW_CASE(ReachMaxResource);
        INDEXLIB_ERROR_CODE_THROW_CASE(Exist);
        INDEXLIB_ERROR_CODE_THROW_CASE(MemFileIO);
        INDEXLIB_ERROR_CODE_THROW_CASE(DocumentDeserialize);
        INDEXLIB_ERROR_CODE_THROW_CASE(Timeout);
    default:
        assert(false);
    }
}

inline void ThrowIfError(index::ErrorCode ec)
{
    if (unlikely(ec != index::ErrorCode::OK)) {
        ThrowThisError(ec);
    }
}
#undef INDEXLIB_ERROR_CODE_THROW_CASE

template <typename T>
class Result
{
public:
    Result(T value) noexcept : _value(value), _ec(ErrorCode::OK) {}
    Result(ErrorCode ec) noexcept : _value(T()), _ec(ec) {}
    bool Ok() const noexcept { return _ec == ErrorCode::OK; }
    const T& Value() const noexcept { return _value; }
    T&& Value() noexcept { return std::forward<T>(_value); }
    T ValueOrThrow() const
    {
        ThrowIfError(_ec);
        return Value();
    }
    ErrorCode GetErrorCode() const noexcept { return _ec; }

    template <typename R>
    Result<R> Transfer() const noexcept
    {
        return Result<R>(_ec);
    }

private:
    T _value;
    ErrorCode _ec;
};

#define IE_RETURN_CODE_IF_ERROR(ret)                                                                                   \
    if ((ret) != indexlib::index::ErrorCode::OK) {                                                                     \
        return ret;                                                                                                    \
    }

} // namespace indexlib::index

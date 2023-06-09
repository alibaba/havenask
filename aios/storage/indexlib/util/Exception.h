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
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "indexlib/util/IoExceptionController.h"

namespace indexlib { namespace util {
typedef autil::legacy::ExceptionBase ExceptionBase;
}} // namespace indexlib::util

#define DECLARE_INDEXLIB_EXCEPTION(ExceptionName)                                                                      \
    namespace indexlib { namespace util {                                                                              \
    class ExceptionName : public ExceptionBase                                                                         \
    {                                                                                                                  \
    public:                                                                                                            \
        AUTIL_LEGACY_DEFINE_EXCEPTION(ExceptionName, ExceptionBase);                                                   \
    };                                                                                                                 \
    }                                                                                                                  \
    }

DECLARE_INDEXLIB_EXCEPTION(BufferOverflowException);
DECLARE_INDEXLIB_EXCEPTION(FileIOException);
DECLARE_INDEXLIB_EXCEPTION(AssertEqualException);
DECLARE_INDEXLIB_EXCEPTION(AssertCompatibleException);
DECLARE_INDEXLIB_EXCEPTION(UnImplementException);
DECLARE_INDEXLIB_EXCEPTION(SchemaException);
DECLARE_INDEXLIB_EXCEPTION(InitializeFailedException);
DECLARE_INDEXLIB_EXCEPTION(OutOfRangeException);
DECLARE_INDEXLIB_EXCEPTION(IndexCollapsedException);
DECLARE_INDEXLIB_EXCEPTION(InconsistentStateException);
DECLARE_INDEXLIB_EXCEPTION(RuntimeException);
DECLARE_INDEXLIB_EXCEPTION(NonExistException);
DECLARE_INDEXLIB_EXCEPTION(DuplicateException);
DECLARE_INDEXLIB_EXCEPTION(InvalidVersionException);
DECLARE_INDEXLIB_EXCEPTION(SegmentFileException);
DECLARE_INDEXLIB_EXCEPTION(BadParameterException);
DECLARE_INDEXLIB_EXCEPTION(DumpingThreadException);
DECLARE_INDEXLIB_EXCEPTION(UnSupportedException);
DECLARE_INDEXLIB_EXCEPTION(OutOfMemoryException);
DECLARE_INDEXLIB_EXCEPTION(ReachMaxIndexSizeException);
DECLARE_INDEXLIB_EXCEPTION(ReachMaxResourceException);
DECLARE_INDEXLIB_EXCEPTION(RealtimeDumpException);
DECLARE_INDEXLIB_EXCEPTION(ExistException);
DECLARE_INDEXLIB_EXCEPTION(MemFileIOException);
DECLARE_INDEXLIB_EXCEPTION(DocumentDeserializeException);
DECLARE_INDEXLIB_EXCEPTION(TimeoutException);

namespace indexlib { namespace util {
template <typename T>
inline void SetFileIOExceptionStatus()
{
    return;
}

template <>
inline void SetFileIOExceptionStatus<FileIOException>()
{
    IoExceptionController::SetFileIOExceptionStatus(true);
}

#define INDEXLIB_THROW(ExClass, format, args...)                                                                       \
    do {                                                                                                               \
        indexlib::util::SetFileIOExceptionStatus<ExClass>();                                                           \
        char buffer[1024];                                                                                             \
        snprintf(buffer, 1024, format, ##args);                                                                        \
        AUTIL_LEGACY_THROW(ExClass, buffer);                                                                           \
    } while (0)

#define INDEXLIB_FATAL_ERROR(ErrorType, format, args...)                                                               \
    ALOG_LOG(_logger, indexlib::util::ExceptionSelector<indexlib::util::ErrorType>::LogLevel, format, ##args);         \
    INDEXLIB_THROW(indexlib::util::ExceptionSelector<indexlib::util::ErrorType>::ExceptionType, format, ##args)

#define INDEXLIB_FATAL_ERROR_IF(condition, ErrorType, format, args...)                                                 \
    if ((condition)) {                                                                                                 \
        INDEXLIB_FATAL_ERROR(ErrorType, format, ##args);                                                               \
    }

#define INDEXLIB_THROW_WARN(ErrorType, format, args...)                                                                \
    AUTIL_LOG(WARN, format, ##args);                                                                                   \
    INDEXLIB_THROW(indexlib::util::ExceptionSelector<indexlib::util::ErrorType>::ExceptionType, format, ##args)

template <int ErrorType>
struct ExceptionSelector {
    static const uint32_t LogLevel = 0;

    typedef ExceptionBase ExceptionType;
};

#define DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(errorType, level, excepType)                                              \
    template <>                                                                                                        \
    struct ExceptionSelector<errorType> {                                                                              \
        const static uint32_t LogLevel = alog::LOG_LEVEL_##level;                                                      \
        typedef excepType ExceptionType;                                                                               \
    };

#define INDEXLIB_HANDLE_EPTR(eptr)                                                                                     \
    do {                                                                                                               \
        try {                                                                                                          \
            if (eptr) {                                                                                                \
                std::rethrow_exception(eptr);                                                                          \
            }                                                                                                          \
        } catch (const std::exception& e) {                                                                            \
            std::cout << "Caught exception \"\" << e.what() << \"\"\n\";";                                             \
        }                                                                                                              \
    } while (0)

static const int BufferOverflow = 1;
static const int FileIO = 2;
static const int AssertEqual = 3;
static const int UnImplement = 4;
static const int Schema = 5;
static const int InitializeFailed = 6;
static const int OutOfRange = 7;
static const int IndexCollapsed = 8;
static const int InconsistentState = 9;
static const int Runtime = 10;
static const int NonExist = 11;
static const int Duplicate = 12;
static const int InvalidVersion = 13;
static const int SegmentFile = 14;
static const int BadParameter = 15;
static const int DumpingThread = 16;
static const int UnSupported = 17;
static const int OutOfMemory = 18;
static const int AssertCompatible = 19;
static const int ReachMaxIndexSize = 20;
static const int ReachMaxResource = 21;
static const int Exist = 22;
static const int MemFileIO = 23;
static const int DocumentDeserialize = 24;
static const int Timeout = 25;

DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(BufferOverflow, ERROR, BufferOverflowException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(FileIO, ERROR, FileIOException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(AssertEqual, ERROR, AssertEqualException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(AssertCompatible, ERROR, AssertCompatibleException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(UnImplement, ERROR, UnImplementException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(Schema, ERROR, SchemaException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(InitializeFailed, ERROR, InitializeFailedException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(OutOfRange, ERROR, OutOfRangeException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(IndexCollapsed, ERROR, IndexCollapsedException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(InconsistentState, ERROR, InconsistentStateException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(Runtime, ERROR, RuntimeException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(NonExist, WARN, NonExistException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(Duplicate, ERROR, DuplicateException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(InvalidVersion, ERROR, InvalidVersionException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(SegmentFile, ERROR, SegmentFileException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(BadParameter, ERROR, BadParameterException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(DumpingThread, ERROR, DumpingThreadException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(UnSupported, ERROR, UnSupportedException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(OutOfMemory, ERROR, OutOfMemoryException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(ReachMaxIndexSize, ERROR, ReachMaxIndexSizeException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(ReachMaxResource, ERROR, ReachMaxResourceException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(Exist, WARN, ExistException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(MemFileIO, ERROR, MemFileIOException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(DocumentDeserialize, ERROR, DocumentDeserializeException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(Timeout, ERROR, TimeoutException);
}} // namespace indexlib::util

#ifndef __INDEXLIB_ERROR_CODE_H
#define __INDEXLIB_ERROR_CODE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include <fslib/fslib.h>

IE_NAMESPACE_BEGIN(common);

enum class ErrorCode : int
{
    OK = 0,
    BufferOverflow = misc::BufferOverflow,
    FileIO = misc::FileIO,
    AssertEqual = misc::AssertEqual,
    UnImplement = misc::UnImplement,
    Schema = misc::Schema,
    InitializeFailed = misc::InitializeFailed,
    OutOfRange = misc::OutOfRange,
    IndexCollapsed = misc::IndexCollapsed,
    InconsistentState = misc::InconsistentState,
    Runtime = misc::Runtime,
    NonExist = misc::NonExist,
    Duplicate = misc::Duplicate,
    InvalidVersion = misc::InvalidVersion,
    SegmentFile = misc::SegmentFile,
    BadParameter = misc::BadParameter,
    DumpingThread = misc::DumpingThread,
    UnSupported = misc::UnSupported,
    OutOfMemory = misc::OutOfMemory,
    AssertCompatible = misc::AssertCompatible,
    ReachMaxIndexSize = misc::ReachMaxIndexSize,
    ReachMaxResource = misc::ReachMaxResource,
    Exist = misc::Exist,
    MemFileIO = misc::MemFileIO,
    DocumentDeserialize = misc::DocumentDeserialize,
};

#define ERROR_CODE_THROW_CASE(ec)                                       \
    case common::ErrorCode::ec:                                         \
    AUTIL_LEGACY_THROW(misc::ExceptionSelector<misc::ec>::ExceptionType, "")  

#define THROW_ERROR_CODE(ec) do {                       \
        switch(ec) {                                    \
            ERROR_CODE_THROW_CASE(BufferOverflow);      \
            ERROR_CODE_THROW_CASE(FileIO);              \
            ERROR_CODE_THROW_CASE(AssertEqual);         \
            ERROR_CODE_THROW_CASE(UnImplement);         \
            ERROR_CODE_THROW_CASE(Schema);              \
            ERROR_CODE_THROW_CASE(InitializeFailed);    \
            ERROR_CODE_THROW_CASE(OutOfRange);          \
            ERROR_CODE_THROW_CASE(IndexCollapsed);      \
            ERROR_CODE_THROW_CASE(InconsistentState);   \
            ERROR_CODE_THROW_CASE(Runtime);             \
            ERROR_CODE_THROW_CASE(NonExist);            \
            ERROR_CODE_THROW_CASE(Duplicate);           \
            ERROR_CODE_THROW_CASE(InvalidVersion);      \
            ERROR_CODE_THROW_CASE(SegmentFile);         \
            ERROR_CODE_THROW_CASE(BadParameter);        \
            ERROR_CODE_THROW_CASE(DumpingThread);       \
            ERROR_CODE_THROW_CASE(UnSupported);         \
            ERROR_CODE_THROW_CASE(OutOfMemory);         \
            ERROR_CODE_THROW_CASE(AssertCompatible);    \
            ERROR_CODE_THROW_CASE(ReachMaxIndexSize);   \
            ERROR_CODE_THROW_CASE(ReachMaxResource);    \
            ERROR_CODE_THROW_CASE(Exist);               \
            ERROR_CODE_THROW_CASE(MemFileIO);           \
            ERROR_CODE_THROW_CASE(DocumentDeserialize); \
        default:                                        \
            assert(false);                              \
        }                                               \
    } while(0)                                          


inline void ThrowIfError(common::ErrorCode ec)
{
    if (unlikely(ec != common::ErrorCode::OK))
    {
        THROW_ERROR_CODE(ec);
    }
}

template<typename T>
class Result
{
public:
    Result(T value) noexcept
        : mValue(value)
        , mEc(ErrorCode::OK)
    {
    }
    Result(ErrorCode ec) noexcept
        : mEc(ec)
    {
    }
    bool Ok() const noexcept
    {
        return mEc == ErrorCode::OK;
    }
    T Value() const noexcept 
    {
        return mValue;
    }
    T ValueOrThrow() const
    {
        ThrowIfError(mEc);
        return Value();
    }
    ErrorCode GetErrorCode() const noexcept 
    {
        return mEc;
    }

    template<typename R>
    Result<R> Transfer() const noexcept
    {
        return Result<R>(mEc);
    }

private:
    T mValue;
    ErrorCode mEc;
};

template<typename T>
Result<T> MakeResult(T value)
{
    return Result<T>(value);
}

#define IE_RETURN_RESULT_IF_ERROR(type, ret)                        \
    if ((ret) != IE_NAMESPACE(common)::ErrorCode::OK)               \
    {                                                               \
        return IE_NAMESPACE(common)::Result<type>(ret);             \
    }

#define IE_RETURN_CODE_IF_ERROR(ret)                                \
    if ((ret) != IE_NAMESPACE(common)::ErrorCode::OK)               \
    {                                                               \
        return ret;                                                 \
    }

#undef ERROR_CODE_THROW_CASE

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ERROR_CODE_H

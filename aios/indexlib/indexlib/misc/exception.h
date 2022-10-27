#ifndef __INDEXLIB_EXCEPTION_H
#define __INDEXLIB_EXCEPTION_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/io_exception_controller.h"
#include "autil/legacy/exception.h"
#include <tr1/memory>
#include <string>
#include <cstdio>
#include <unistd.h>
#include <exception>
#include <stdexcept>

IE_NAMESPACE_BEGIN(misc);
typedef autil::legacy::ExceptionBase ExceptionBase;
IE_NAMESPACE_END(misc);

// for legacy code
// IE_NAMESPACE_BEGIN(common);
// typedef autil::legacy::ExceptionBase ExceptionBase;
// IE_NAMESPACE_END(common);

#define DECLARE_INDEXLIB_EXCEPTION(ExceptionName)                       \
    IE_NAMESPACE_BEGIN(misc);                                           \
    class ExceptionName : public ExceptionBase                          \
    {                                                                   \
    public:                                                             \
        AUTIL_LEGACY_DEFINE_EXCEPTION(ExceptionName, ExceptionBase);    \
    };                                                                  \
    IE_NAMESPACE_END(misc);                                             \


/*
    IE_NAMESPACE_BEGIN(common);                                         
    typedef IE_NAMESPACE(misc)::ExceptionName ExceptionName;            
    IE_NAMESPACE_END(common);
*/

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


IE_NAMESPACE_BEGIN(misc);
template<typename T>
inline void SetFileIOExceptionStatus() { return; }

template<>
inline void SetFileIOExceptionStatus<FileIOException>()
{
    IoExceptionController::SetFileIOExceptionStatus(true);
}

#define INDEXLIB_THROW(ExClass, format, args...)                        \
    do                                                                  \
    {                                                                   \
        IE_NAMESPACE(misc)::SetFileIOExceptionStatus<ExClass>();        \
        char buffer[1024];                                              \
        snprintf(buffer, 1024, format, ##args);                         \
        AUTIL_LEGACY_THROW(ExClass, buffer);                            \
    } while (0)

#define INDEXLIB_FATAL_ERROR(ErrorType, format, args...)                \
    __INTERNAL_IE_LOG(IE_NAMESPACE(misc)::ExceptionSelector<IE_NAMESPACE(misc)::ErrorType>::LogLevel, format, ##args); \
    INDEXLIB_THROW(IE_NAMESPACE(misc)::ExceptionSelector<IE_NAMESPACE(misc)::ErrorType>::ExceptionType, format, ##args)

#define INDEXLIB_FATAL_IO_ERROR(ErrorCode, format, args...)             \
    INDEXLIB_FATAL_ERROR(FileIO, "[%s] " format,                        \
                         fslib::fs::FileSystem::getErrorString(ErrorCode).c_str(), \
                         ##args);

#define INDEXLIB_THROW_WARN(ErrorType, format, args...)                \
    __INTERNAL_IE_LOG(alog::LOG_LEVEL_WARN, format, ##args);            \
    INDEXLIB_THROW(IE_NAMESPACE(misc)::ExceptionSelector<IE_NAMESPACE(misc)::ErrorType>::ExceptionType, format, ##args)

template <int ErrorType>
struct ExceptionSelector
{
    static const uint32_t LogLevel = 0;

    typedef ExceptionBase ExceptionType;
};

#define DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(errorType, level, excepType) \
    template <> struct ExceptionSelector<errorType>                     \
    {                                                                   \
        const static uint32_t LogLevel = alog::LOG_LEVEL_##level;       \
        typedef excepType ExceptionType;                                \
    };

#define INDEXLIB_HANDLE_EPTR(eptr) do           \
{                                               \
    try                                         \
    {                                           \
        if (eptr)                               \
        {                                       \
            std::rethrow_exception(eptr);       \
        }                                       \
    }                                                                   \
    catch(const std::exception& e)                                      \
    {                                                                   \
        std::cout << "Caught exception \"\" << e.what() << \"\"\n\";";  \
    }                                                                   \
} while(0)

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
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(NonExist, ERROR, NonExistException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(Duplicate, ERROR, DuplicateException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(InvalidVersion, ERROR, InvalidVersionException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(SegmentFile, ERROR, SegmentFileException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(BadParameter, ERROR, BadParameterException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(DumpingThread, ERROR, DumpingThreadException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(UnSupported, ERROR, UnSupportedException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(OutOfMemory, ERROR, OutOfMemoryException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(ReachMaxIndexSize, ERROR, ReachMaxIndexSizeException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(ReachMaxResource, ERROR, ReachMaxResourceException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(Exist, ERROR, ExistException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(MemFileIO, ERROR, MemFileIOException);
DECLARE_INDEXLIB_EXCEPTION_LOG_LEVEL(DocumentDeserialize, ERROR, DocumentDeserializeException);

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_EXCEPTION_H

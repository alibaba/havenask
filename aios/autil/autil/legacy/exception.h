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
/** Autil ExceptionBase
 * All exception classes in Autil should inherit ExceptionBase
 */

/**
 * Guildelines for exception handling
 * 1. How to throw
 *    AUTIL_LEGACY_THROW(the_exception_to_show, description_string)
 *    e.g. AUTIL_LEGACY_THROW(ParameterInvalidException, "Cannot be empty string");
 *    If you throw not using AUTIL_LEGACY_THROW, the location of throw will not be recorded.
 *
 * 2. Define an new autil exception:
 *    a. SHOULD inherit from autil::ExceptionBase or its sub-classes.
 *    b. SHOULD use macro AUTIL_LEGACY_DEFINE_EXCEPTION inside public: region of the class definition.
 *
 * 3. GetClassName(): exception class name.
 *    GetMessage(): (virtual) One can customize it to return some specific details. By default, it returns the
 *        description string when you construct the exception.
 *    GetStackTrace(): call stack when the exception is thrown.
 *    ToString(): return a string describing:
 *        a) where the exception is thown (if available)
 *        b) the exception class name
 *        c) the content of GetMessage
 *        d) stack trace
 *        e) cause, if available
 *    what(): Same as ToString(). But the return type is const char *.
 *    GetExceptionChain(): Same as ToString(). Kept for backward compatibility.
 */


#include <stddef.h>
#include <exception> //IWYU pragma: export
#include <string> //IWYU pragma: export
#include <memory>

namespace autil {
namespace legacy {
class AlreadyExistException;
class AuthenticationFailureException;
class DataUnavailableException;
class DirectoryExistException;
class DirectoryNotExistException;
class ExceptionBase;
class FileAppendingException;
class FileExistException;
class FileNotExistException;
class FileOverwriteException;
class InternalServerErrorException;
class InvalidOperation;
class LogicError;
class NotExistException;
class NotImplementedException;
class OverflowError;
class PangunNotEnoughChunkserverExcepion;
class ParameterInvalidException;
class RuntimeError;
class SameNameEntryExistException;
class ServiceExceptionBase;
class ServiceUnavailableException;
class SessionExpireException;
class StorageExceptionBase;
class StreamCorruptedException;
class TimeoutError;
class UnexpectedEndOfStreamException;
class UnimplementedException;
}  // namespace legacy
}  // namespace autil

#define AUTIL_LEGACY_THROW(ExClass, args...)                                  \
    do                                                                  \
    {                                                                   \
        ExClass tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0(args);         \
        tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.Init(__FILE__, __PRETTY_FUNCTION__, __LINE__); \
        throw tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0;                 \
    } while (false)

#define AUTIL_LEGACY_THROW_CHAIN(ExClass, cause, args...)                     \
    do                                                                  \
    {                                                                   \
        ExClass tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0(args);         \
        tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.Init(__FILE__, __PRETTY_FUNCTION__, __LINE__); \
        tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0.SetCause(cause);       \
        throw tmp_3d3079a0_61ec_48e6_abd6_8a33b0eb91f0;                 \
    } while (false)


#define AUTIL_LEGACY_DEFINE_EXCEPTION(ExClass, Base)          \
    ExClass(const std::string& strMsg="") throw()       \
        : Base(strMsg)                                  \
    {}                                                  \
                                                        \
    ~ExClass() throw(){}                                      \
                                                              \
    std::string GetClassName() const override                 \
    {                                                         \
        return #ExClass;                                      \
    }                                                         \
                                                              \
    std::shared_ptr<ExceptionBase> Clone() const   override             \
    {                                                                   \
        return std::shared_ptr<ExceptionBase>(new ExClass(*this));      \
    }

namespace autil{ namespace legacy
{

class ExceptionBase : public std::exception
{
public:
    ExceptionBase(const std::string& message = "") throw();

    virtual ~ExceptionBase() throw();

    virtual std::shared_ptr<ExceptionBase> Clone() const;

    void Init(const char* file, const char* function, int line);

    virtual void SetCause(const ExceptionBase& cause);

    virtual void SetCause(std::shared_ptr<ExceptionBase> cause);

    virtual std::shared_ptr<ExceptionBase> GetCause() const;
   
    // Return the root cause, if the exception has the root cause; else return itself 
    virtual std::shared_ptr<ExceptionBase> GetRootCause() const;

    virtual std::string GetClassName() const;

    virtual std::string GetMessage() const;

    /**
     * With a) detailed throw location (file + lineno) (if availabe), b) Exception class name, and
     * c) content of GetMessage() (if not empty)
     */
    const char* what() const throw() override;

    /**
     * Synonym of what(), except for the return type.
     */
    const std::string& ToString() const;

    const std::string& GetExceptionChain() const;

    std::string GetStackTrace() const;

public:
    enum { MAX_STACK_TRACE_SIZE = 50 };
    std::shared_ptr<ExceptionBase> mNestedException;
    std::string mMessage;
    const char* mFile;
    const char* mFunction;
    int mLine;

private:
    void* mStackTrace[MAX_STACK_TRACE_SIZE];
    size_t mStackTraceSize;

    mutable std::string mWhat;

    // friend Any ToJson(const ExceptionBase& e);
    // friend void FromJson(ExceptionBase& e, const Any& a);
    // friend void serializeToWriter(rapidjson::Writer<rapidjson::StringBuffer> *writer,
    //                               const ExceptionBase& t);
    // friend void FromRapidValue(ExceptionBase& t, RapidValue& value);
};

class InvalidOperation : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(InvalidOperation, ExceptionBase);
};

class RuntimeError : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(RuntimeError, ExceptionBase);
};

class TimeoutError : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(TimeoutError, ExceptionBase);
};

class LogicError : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(LogicError, ExceptionBase);
};

class OverflowError : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(OverflowError, ExceptionBase);
};

class AlreadyExistException: public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(AlreadyExistException, ExceptionBase);
};

class NotExistException: public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(NotExistException, ExceptionBase);
};

class NotImplementedException: public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(NotImplementedException, ExceptionBase);
};


// parameter invalid
class ParameterInvalidException : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(ParameterInvalidException, ExceptionBase);
};

// operation is denied
class AuthenticationFailureException : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(AuthenticationFailureException, ExceptionBase);
};

// base class for all exceptions in storage system
class StorageExceptionBase : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(StorageExceptionBase, ExceptionBase);
};

// when create an exist file
class FileExistException : public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(FileExistException, StorageExceptionBase);
};

// when open/delete/rename/... an non-exist file
class FileNotExistException : public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(FileNotExistException, StorageExceptionBase);
};

class DirectoryExistException : public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(DirectoryExistException, StorageExceptionBase);
};

class DirectoryNotExistException : public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(DirectoryNotExistException, StorageExceptionBase);
};

class SameNameEntryExistException : public StorageExceptionBase
{
    public:
        AUTIL_LEGACY_DEFINE_EXCEPTION(SameNameEntryExistException, StorageExceptionBase);
};

// when append/delete a file that being appended
class FileAppendingException : public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(FileAppendingException, StorageExceptionBase);
};

// when opening a file that cannot be overwritten
class FileOverwriteException: public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(FileOverwriteException, StorageExceptionBase);
};

// when append/delete a file that being appended
class PangunNotEnoughChunkserverExcepion : public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(PangunNotEnoughChunkserverExcepion, StorageExceptionBase);
};

// when read, data is unavailable due to internal error
class DataUnavailableException : public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(DataUnavailableException, StorageExceptionBase);
};

// when append/commit, data stream is corrupted due to internal error
class StreamCorruptedException : public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(StreamCorruptedException, StorageExceptionBase);
};

// when end of stream comes unexpected
class UnexpectedEndOfStreamException: public StorageExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(UnexpectedEndOfStreamException, StorageExceptionBase);
};

// base class for all exceptions in service
class ServiceExceptionBase : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(ServiceExceptionBase, ExceptionBase);
};

// can't get service connection
class ServiceUnavailableException : public ServiceExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(ServiceUnavailableException, ServiceExceptionBase);
};

// internal server error
class InternalServerErrorException : public ServiceExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(InternalServerErrorException, ServiceExceptionBase);
};

// session expired
class SessionExpireException : public ServiceExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(SessionExpireException, ServiceExceptionBase);
};

// running into unimplemented code
class UnimplementedException : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(UnimplementedException, ExceptionBase);
};

}}//namespace autil{ namespace legacy


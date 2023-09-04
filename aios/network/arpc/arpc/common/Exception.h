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
#ifndef ARPC_COMMON_EXCEPTION_H
#define ARPC_COMMON_EXCEPTION_H

#include <exception>
#include <iostream>
#include <memory>
#include <stddef.h>
#include <string>

#define ARPC_DEFINE_EXCEPTION(ExClass, Base)                                                                           \
    class ExClass : public Base {                                                                                      \
    public:                                                                                                            \
        ExClass(const std::string &message = "") throw() : Base(message) {}                                            \
                                                                                                                       \
        ExClass(const std::string &message, const Exception &cause) throw() : Base(message, cause) {}                  \
                                                                                                                       \
        ~ExClass() throw() {}                                                                                          \
                                                                                                                       \
        /* override */ const char *GetClassName() const { return #ExClass; }                                           \
                                                                                                                       \
        /* override */ ExceptionPtr Clone() const { return ExceptionPtr(new ExClass(*this)); }                         \
    };

namespace arpc {
namespace common {

class Exception;

typedef std::shared_ptr<Exception> ExceptionPtr;

class Exception : public std::exception {
public:
    Exception(const std::string &message = "") throw();
    Exception(const std::string &message, const Exception &cause) throw();
    virtual ~Exception() throw();

    void Init(const char *file, const char *function, int line);

    virtual ExceptionPtr Clone() const;
    virtual ExceptionPtr GetCause() const;
    virtual const char *GetClassName() const;
    virtual const std::string &GetMessage() const;

    /**
     * With a) detailed throw location (file + lineno) (if availabe), b) Exception class name, and
     * c) content of GetMessage() (if not empty)
     */
    /* override */ const char *what() const throw();

    /**
     * Synonym of what(), except for the return type.
     */
    const std::string &ToString() const;

private:
    std::string GetStackTrace() const;

protected:
    ExceptionPtr mNestedException;
    std::string mMessage;
    const char *mFile;
    const char *mFunction;
    int mLine;

private:
    enum { MAX_STACK_TRACE_SIZE = 50 };
    void *mStackTrace[MAX_STACK_TRACE_SIZE];
    size_t mStackTraceSize;

    mutable std::string mWhat;
};

ARPC_DEFINE_EXCEPTION(RPCTimeoutException, Exception);
ARPC_DEFINE_EXCEPTION(RPCTimeoutInFutureException, RPCTimeoutException);
ARPC_DEFINE_EXCEPTION(RPCServerStoppedException, Exception);
ARPC_DEFINE_EXCEPTION(RPCConnectionClosedException, Exception);
ARPC_DEFINE_EXCEPTION(RPCDecodePacketException, Exception);
ARPC_DEFINE_EXCEPTION(RPCEncodePacketException, Exception);
ARPC_DEFINE_EXCEPTION(RPCRpcCallMismatchException, Exception);
ARPC_DEFINE_EXCEPTION(RPCPostPacketException, Exception);
ARPC_DEFINE_EXCEPTION(RPCPushServerWorkItemException, Exception);
ARPC_DEFINE_EXCEPTION(RPCRequestTooLargeException, Exception);
ARPC_DEFINE_EXCEPTION(RPCResponseTooLargeException, Exception);
ARPC_DEFINE_EXCEPTION(RPCResponseBadPacketException, Exception);

} // namespace common
} // namespace arpc
#endif // ARPC_COMMON_EXCEPTION_H

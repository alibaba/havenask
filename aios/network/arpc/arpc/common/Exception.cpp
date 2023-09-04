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
#include "Exception.h"

#include <cxxabi.h>
#include <execinfo.h>
#include <iostream>
#include <sstream>
#include <stdlib.h>

namespace arpc {
namespace common {

Exception::Exception(const std::string &message) throw()
    : mMessage(message), mFile("<unknown file>"), mFunction("<unknown function>"), mLine(-1), mStackTraceSize(0) {}

Exception::Exception(const std::string &message, const Exception &cause) throw()
    : mNestedException(cause.Clone())
    , mMessage(message)
    , mFile("<unknown file>")
    , mFunction("<unknown function>")
    , mLine(-1)
    , mStackTraceSize(0) {}

Exception::~Exception() throw() {}

ExceptionPtr Exception::Clone() const { return ExceptionPtr(new Exception(*this)); }

void Exception::Init(const char *file, const char *function, int line) {
    mFile = file;
    mFunction = function;
    mLine = line;
    mStackTraceSize = backtrace(mStackTrace, MAX_STACK_TRACE_SIZE);
}

ExceptionPtr Exception::GetCause() const { return mNestedException; }

const char *Exception::GetClassName() const { return "Exception"; }

const std::string &Exception::GetMessage() const { return mMessage; }

const char *Exception::what() const throw() { return ToString().c_str(); }

const std::string &Exception::ToString() const {
    if (mWhat.empty()) {
        std::stringstream result;

        if (mLine > 0) {
            result << mFile << "(" << mLine << ")";
        } else {
            result << "<unknown throw location>";
        }

        result << ": " << GetClassName();

        std::string customizedString = GetMessage();

        if (!customizedString.empty()) {
            result << ": " << customizedString;
        }

        result << "\nStack trace:\n";
        result << GetStackTrace();

        if (mNestedException != NULL) {
            result << "Caused by:\n" << mNestedException->ToString();
        }

        mWhat = result.str();
    }

    return mWhat;
}

std::string Exception::GetStackTrace() const {
    if (mStackTraceSize == 0) {
        return "<No stack trace>\n";
    }

    char **strings = backtrace_symbols(mStackTrace, mStackTraceSize);

    if (strings == NULL) {
        return "<Unknown error: backtrace_symbols returned NULL>\n";
    }

    std::stringstream result;

    for (size_t i = 0; i < mStackTraceSize; ++i) {
        std::string mangledName = strings[i];
        std::string::size_type begin = mangledName.find('(');
        std::string::size_type end = mangledName.find('+', begin);

        if (begin == std::string::npos || end == std::string::npos) {
            result << mangledName;
            result << '\n';
            continue;
        }

        ++begin;
        int status;
        char *s = abi::__cxa_demangle(mangledName.substr(begin, end - begin).c_str(), NULL, 0, &status);

        if (status != 0) {
            result << mangledName;
            result << '\n';
            continue;
        }

        std::string demangledName(s);
        free(s);

        // Ignore Exception::Init so the top frame is the
        // user's frame where this exception is thrown.
        //
        // Can't just ignore frame#0 because the compiler might
        // inline Exception::Init.
        if (i == 0 && demangledName == "apsara::common2::Exception::Init(char const*, char const*, int)")
            continue;

        result << mangledName.substr(0, begin);
        result << demangledName;
        result << mangledName.substr(end);
        result << '\n';
    }

    free(strings);

    return result.str();
}

} // namespace common
} // namespace arpc

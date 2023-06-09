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
#include "autil/legacy/exception.h"
#include "autil/Backtrace.h"
#include <execinfo.h>
#include <cxxabi.h>
#include <cstdlib>

namespace autil{ namespace legacy
{

static inline int GetBackTraceDepth() {
    static int depth = -1;
    if (depth >= 0) {
        return depth;
    }
    auto env = std::getenv("AUTIL_BACKTRACE_DEPTH");
    if (env) {
        auto num = std::strtol(env, nullptr, 10);
        if (num >= 0 && num < ExceptionBase::MAX_STACK_TRACE_SIZE) {
            depth = num;
            return depth;
        }
    }
    depth = ExceptionBase::MAX_STACK_TRACE_SIZE;
    return depth;
}

ExceptionBase::ExceptionBase(const std::string& message) throw()
    : mMessage(message),
      mFile("<unknown file>"),
      mFunction("<unknown function>"),
      mLine(-1),
      mStackTraceSize(0)
{}

ExceptionBase::~ExceptionBase() throw()
{}

std::shared_ptr<ExceptionBase> ExceptionBase::Clone() const
{
    return std::shared_ptr<ExceptionBase>(new ExceptionBase(*this));
}

void ExceptionBase::Init(const char* file, const char* function, int line)
{
    mFile = file;
    mFunction = function;
    mLine = line;
    mStackTraceSize = backtrace(mStackTrace, GetBackTraceDepth());
}

void ExceptionBase::SetCause(const ExceptionBase& cause)
{
    SetCause(cause.Clone());
}

void ExceptionBase::SetCause(std::shared_ptr<ExceptionBase> cause)
{
    mNestedException = cause;
}

std::shared_ptr<ExceptionBase> ExceptionBase::GetCause() const
{
    return mNestedException;
}

std::shared_ptr<ExceptionBase> ExceptionBase::GetRootCause() const
{
    if (mNestedException.get())
    {
        std::shared_ptr<ExceptionBase> rootCause =  mNestedException->GetRootCause();
        if (rootCause.get())
            return rootCause;
    }
    return mNestedException;
}

std::string ExceptionBase::GetClassName() const
{
    return "ExceptionBase";
}

std::string ExceptionBase::GetMessage() const
{
    return mMessage;
}

const char* ExceptionBase::what() const throw()
{
    return ToString().c_str();
}

const std::string& ExceptionBase::ToString() const
{
    if (mWhat.empty())
    {
        if (mLine > 0)
            mWhat = std::string(mFile) + "(" + std::to_string(mLine) + ")";
        else
            mWhat = "<unknown throw location>";
        mWhat += ": " + GetClassName();
        std::string customizedString = GetMessage();
        if (!customizedString.empty())
        {
            mWhat += ": " + customizedString;
        }
        mWhat += "\nStack trace:\n";
        mWhat += GetStackTrace();
        if (mNestedException.get())
        {
            mWhat += "Caused by:\n" + mNestedException->ToString();
        }
    }
    return mWhat;
}

const std::string& ExceptionBase::GetExceptionChain() const
{
    return ToString();
}

std::string ExceptionBase::GetStackTrace() const
{
    if (mStackTraceSize == 0)
        return "<No stack trace>\n";
    char** strings = backtrace_symbols(mStackTrace, mStackTraceSize);
    if (strings == NULL) // Since this is for debug only thus
                         // non-critical, don't throw an exception.
        return "<Unknown error: backtrace_symbols returned NULL>\n";

    std::string result;
    for (size_t i = 0; i < mStackTraceSize; ++i)
    {
        std::string s = strings[i];
        std::string::size_type begin = s.find('(');
        std::string::size_type end = s.find('+', begin);
        if (begin == std::string::npos || end == std::string::npos) {
            result += s;
            result += '\n';
            continue;
        }
        ++begin;
        auto mangledName = s.substr(begin, end-begin);
        auto demangledName = Backtrace::GetDemangledName(mangledName);
        // Ignore ExceptionBase::Init so the top frame is the
        // user's frame where this exception is thrown.
        //
        // Can't just ignore frame#0 because the compiler might
        // inline ExceptionBase::Init.
        if (i == 0
            && demangledName == "autil::legacy::ExceptionBase::Init(char const*, char const*, int)")
            continue;
        result += s.substr(0, begin);
        result += demangledName;
        result += s.substr(end);
        result += '\n';
    }
    free(strings);
    return result;
}

}}//namespace autil::legacy

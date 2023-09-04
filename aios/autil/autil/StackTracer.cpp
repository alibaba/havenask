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
#include "autil/StackTracer.h"

#include <cstdint>
#include <cxxabi.h>
#include <errno.h>
#include <execinfo.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utility>

#include "autil/Log.h"

using namespace std;

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, StackTracer);

bool StackTracer::mUseStackTracerLog = false;
StackTracer *StackTracer::ptr = NULL;
RecursiveThreadMutex StackTracer::gLock;

bool StackTracer::initFile(const string &fileName) {
    ScopedLock lock(_lock);
    if (_fp != NULL) {
        cerr << "already init file for StackTracer!" << endl;
        return true;
    }
    FILE *fp = fopen(fileName.c_str(), "w");
    if (fp == NULL) {
        cerr << "init file for StackTracer fail, file :" << fileName << endl;
        cerr << "error:" << strerror(errno) << endl;
        return false;
    }
    setbuf(fp, NULL);
    _fp = fp;
    cout << "init file for StackTracer : " << fileName << endl;
    return true;
}

void StackTracer::setMaxDepth(size_t maxDepth) {
    cout << "set max depth for StackTracer : " << maxDepth << endl;
    _maxDepth = maxDepth;
}

size_t StackTracer::getTraceId() const {
    void *stack_addrs[_maxDepth];
    size_t stack_depth = backtrace(stack_addrs, _maxDepth);
    char **stack_strings = backtrace_symbols(stack_addrs, _maxDepth);

    std::string value;
    for (size_t i = 0; i < stack_depth; i++) {
        std::string stackStr = stack_strings[stack_depth - i - 1];

        size_t start = stackStr.find('(');
        std::string prefixStr = stackStr.substr(0, start);
        std::string funcSym = getSubString(stackStr, '(', '+');
        std::string moveNumStr = getSubString(stackStr, '+', ')');
        std::string addressStr = getSubString(stackStr, '[', ']');
        char *name = abi::__cxa_demangle(funcSym.c_str(), NULL, NULL, NULL);
        std::string funcName = (name == NULL) ? "" : std::string(name);
        std::string lineStr = prefixStr + "(" + funcName + "):[" + funcSym + "]" + addressStr + ":" + moveNumStr;
        if (i == 0) {
            value = lineStr;
        } else {
            value += std::string(" -> ") + lineStr + std::string("\n");
        }
        free(name);
    }
    free(stack_strings);

    ScopedLock lock(_lock);
    auto iter = _traceIdMap.find(value);
    if (iter != _traceIdMap.end()) {
        return iter->second;
    }
    size_t id = _traceIdMap.size();
    if (_fp) {
        fprintf(_fp, "traceId[%lu]:%s\n", id, value.c_str());
    } else {
        std::cerr << "traceId[" << id << "]:" << value << std::endl;
    }
    _traceIdMap[value] = id;
    return id;
}

} // namespace autil

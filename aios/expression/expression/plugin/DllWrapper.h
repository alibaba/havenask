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
#ifndef ISEARCH_EXPRESSION_DLLWRAPPER_H
#define ISEARCH_EXPRESSION_DLLWRAPPER_H

#include "expression/common.h"

namespace expression {

class DllWrapper
{
public:
    DllWrapper(const std::string &dllPath);
    ~DllWrapper();
public:
    bool dlopen();
    bool dlclose();
    void* dlsym(const std::string &symName);
    std::string dlerror();
    std::string getLocalLibPath();
    bool initLibFile();

private:
    std::string _dllPath;
    void *_handle;
    std::string _lastError;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif

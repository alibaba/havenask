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
#ifndef FSLIB_DLLWRAPPER_H
#define FSLIB_DLLWRAPPER_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include <dlfcn.h>
#include <string>

FSLIB_BEGIN_NAMESPACE(fs);
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
};

FSLIB_END_NAMESPACE(fs);
#endif //FSLIB_DLLWRAPPER_H


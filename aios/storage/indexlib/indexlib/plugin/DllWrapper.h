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
#ifndef __INDEXLIB_PLUGIN_DLLWRAPPER_H
#define __INDEXLIB_PLUGIN_DLLWRAPPER_H

#include <dlfcn.h>
#include <memory>
#include <string>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace plugin {
class DllWrapper
{
public:
    DllWrapper(const std::string& dllPath);
    ~DllWrapper();

public:
    bool dlopen();
    bool dlclose();
    void* dlsym(const std::string& symName);
    std::string dlerror();
    std::string getLocalLibPath();
    bool initLibFile();

private:
    std::string _dllPath;
    void* _handle;
    std::string _lastError;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DllWrapper);
}} // namespace indexlib::plugin

#endif

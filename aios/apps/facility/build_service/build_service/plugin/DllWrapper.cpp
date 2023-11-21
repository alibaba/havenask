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
#include "build_service/plugin/DllWrapper.h"

#include <cstddef>

using namespace std;

namespace build_service { namespace plugin {
AUTIL_LOG_SETUP(plugin, DllWrapper);

DllWrapper::DllWrapper(const std::string& dllPath)
{
    AUTIL_LOG(TRACE3, "dllPath[%s]", dllPath.c_str());
    _dllPath = dllPath;
    _handle = NULL;
}

bool DllWrapper::initLibFile() { return true; }

DllWrapper::~DllWrapper() { dlclose(); }

bool DllWrapper::dlopen(int mode)
{
    if (!initLibFile()) {
        string errorMsg = "DllWrapper::dlopen faile, dllPath [" + _dllPath + "]";
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    AUTIL_LOG(DEBUG, "dlopen(%s)", _dllPath.c_str());
    _handle = ::dlopen(_dllPath.c_str(), mode);
    if (!_handle) {
        string errorMsg = "open lib fail: [" + dlerror() + "]";
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
    }
    AUTIL_LOG(DEBUG, "handle(%p)", _handle);
    return _handle != NULL;
}

bool DllWrapper::dlclose()
{
    int ret = 0;
    if (_handle) {
        ret = ::dlclose(_handle);
        _handle = NULL;
    }

    return ret == 0;
}

void* DllWrapper::dlsym(const string& symName)
{
    if (!_handle) {
        return NULL;
    }

    return ::dlsym(_handle, symName.c_str());
}

string DllWrapper::dlerror()
{
    const char* errMsg = ::dlerror();
    if (errMsg) {
        _lastError = errMsg;
    } else {
        return _lastError;
    }
    return _lastError;
}

string DllWrapper::getLocalLibPath() { return _dllPath; }

}} // namespace build_service::plugin

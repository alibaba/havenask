#include "indexlib/plugin/DllWrapper.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(plugin);
IE_LOG_SETUP(plugin, DllWrapper);

DllWrapper::DllWrapper(const std::string &dllPath ) {
    IE_LOG(TRACE3, "dllPath[%s]", dllPath.c_str());
    _dllPath = dllPath;
    _handle = NULL;
}

bool DllWrapper::initLibFile(){
    return true;
}

DllWrapper::~DllWrapper() {
    dlclose();
}

bool DllWrapper::dlopen() {
    if (!initLibFile()) {
        string errorMsg = "DllWrapper::dlopen failed, dllPath [" + _dllPath + "]";
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    IE_LOG(DEBUG, "dlopen(%s)", _dllPath.c_str());
    _handle = ::dlopen(_dllPath.c_str(), RTLD_NOW);
    if (!_handle) {
        string errorMsg = "open lib fail: [" + dlerror() + "]";
        IE_LOG(ERROR, "%s", errorMsg.c_str());
    }
    IE_LOG(DEBUG, "handle(%p)", _handle);
    return _handle != NULL;
}

bool DllWrapper::dlclose() {
    int ret = 0;
    if (_handle) {
        ret = ::dlclose(_handle);
        _handle = NULL;
    }

    return ret == 0 ;
}

void* DllWrapper::dlsym(const string &symName) {
    if (!_handle) {
        return NULL;
    }

    return ::dlsym(_handle, symName.c_str());
}

string DllWrapper::dlerror() {
    const char *errMsg = ::dlerror();
    if (errMsg) {
        _lastError = errMsg;
    } else {
        return _lastError;
    }
    return _lastError;
}

string DllWrapper::getLocalLibPath(){
    return _dllPath;
}

IE_NAMESPACE_END(plugin);


#include "build_service/plugin/DllWrapper.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

using namespace std;

namespace build_service {
namespace plugin {
BS_LOG_SETUP(plugin, DllWrapper);

DllWrapper::DllWrapper(const std::string &dllPath ) {
    BS_LOG(TRACE3, "dllPath[%s]", dllPath.c_str());
    _dllPath = dllPath;
    _handle = NULL;
}

bool DllWrapper::initLibFile(){
    return true;
}

DllWrapper::~DllWrapper() {
    dlclose();
}

bool DllWrapper::dlopen(int mode) {
    if (!initLibFile()) {
        string errorMsg = "DllWrapper::dlopen faile, dllPath [" + _dllPath + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    BS_LOG(DEBUG, "dlopen(%s)", _dllPath.c_str());
    _handle = ::dlopen(_dllPath.c_str(), mode);
    if (!_handle) {
        string errorMsg = "open lib fail: [" + dlerror() + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
    }
    BS_LOG(DEBUG, "handle(%p)", _handle);
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

}
}

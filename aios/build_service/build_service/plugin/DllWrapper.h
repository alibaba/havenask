#ifndef ISEARCH_BS_DLLWRAPPER_H
#define ISEARCH_BS_DLLWRAPPER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <dlfcn.h>
#include <string>

namespace build_service {
namespace plugin {
class DllWrapper
{
public:
    DllWrapper(const std::string &dllPath);
    ~DllWrapper();
public:
    bool dlopen(int mode = RTLD_NOW);
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
    BS_LOG_DECLARE();
};

}
}
#endif

#ifndef __INDEXLIB_PLUGIN_DLLWRAPPER_H
#define __INDEXLIB_PLUGIN_DLLWRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <dlfcn.h>
#include <string>

IE_NAMESPACE_BEGIN(plugin);
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
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DllWrapper);
IE_NAMESPACE_END(plugin);

#endif

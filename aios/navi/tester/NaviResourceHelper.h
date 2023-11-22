#pragma once

#include <list>
#include <memory>
#include <string>

#include "navi/config/NaviRegistryConfigMap.h"
#include "navi/engine/ResourceMap.h"

namespace navi {

class Resource;
class KernelTester;

class NaviResourceHelper {
public:
    NaviResourceHelper();
    ~NaviResourceHelper();
    NaviResourceHelper(const NaviResourceHelper &) = delete;
    NaviResourceHelper &operator=(const NaviResourceHelper &) = delete;

public:
    bool config(const std::string &name, const std::string &config);
    bool snapshotConfig(const std::string &name, const std::string &config);
    bool config(const std::string &configStr);
    bool loadConfig(const std::string &configFile);
    void kernelConfig(const std::string &configStr); // used for kernel resource
    void namedData(const std::string &name, std::shared_ptr<Data> data);
    std::shared_ptr<Resource> getOrCreateResPtr(const std::string &name);
    Resource *getOrCreateRes(const std::string &name);
    template <typename ResT>
    std::shared_ptr<ResT> getOrCreateResPtr();
    template <typename ResT>
    ResT *getOrCreateRes();
    template <typename ResT>
    bool getOrCreateRes(ResT *&ptr);
    bool addExternalRes(std::shared_ptr<Resource> resource);
    const ResourceMap &getResourceMap() const { return _resourceMap; }
    void setModules(const std::vector<std::string> &modules);
    void logLevel(const std::string &level);

private:
    void initBaseRes();
    std::shared_ptr<Resource> createResPtr(const std::string &name);
    void collectDependRes(const std::shared_ptr<Resource> &resource);
    std::string registerResKernel(const std::string &name);

private:
    std::list<autil::legacy::RapidDocument> _document;
    NaviRegistryConfigMap _snapshotConfigMap;
    NaviRegistryConfigMap _configMap;
    ResourceMap _resourceMap;
    std::string _kernelConfig;
    std::map<std::string, std::shared_ptr<Data>> _namedDatas;
    std::vector<std::string> _modules;
    std::vector<std::shared_ptr<KernelTester>> _testers;
    std::string _logLevel;
};

template <typename ResT>
std::shared_ptr<ResT> NaviResourceHelper::getOrCreateResPtr() {
    return std::dynamic_pointer_cast<ResT>(getOrCreateResPtr(ResT::RESOURCE_ID));
}

template <typename ResT>
ResT *NaviResourceHelper::getOrCreateRes() {
    return getOrCreateResPtr<ResT>().get();
}

template <typename ResT>
bool NaviResourceHelper::getOrCreateRes(ResT *&ptr) {
    ptr = getOrCreateRes<ResT>();
    return ptr != nullptr;
}

} // namespace navi

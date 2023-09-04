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
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "autil/Log.h"
#include "swift/admin/AdminStatusManager.h"
#include "swift/common/Common.h"

namespace swift {
namespace config {
class AdminConfig;
}
namespace admin {
class ModuleManager;
class SysController;

class BaseModule {
public:
    using ModuleMap = std::unordered_map<std::string, std::shared_ptr<BaseModule>>;
    using ModuleBindFunc = std::function<void(std::shared_ptr<BaseModule>)>;
    using BindFuncMap = std::unordered_map<std::string, ModuleBindFunc>;

public:
    BaseModule();
    virtual ~BaseModule();
    BaseModule(const BaseModule &) = delete;
    BaseModule &operator=(const BaseModule &) = delete;

public:
    void setLoadStatus(common::LoadStatus loadStatus);
    void setSysModule();
    bool isSysModule() const;
    void setName(const std::string &name);
    const std::string &getName() const;

public:
    bool init(ModuleManager *manager, admin::SysController *sysController, config::AdminConfig *adminConfig);
    bool load();
    bool unload();
    void clear();
    bool update(Status status);
    bool needLoad(Status status) const;
    bool isLoad() const;
    static common::LoadStatus calcLoadStatus(const std::string &masterTag, const std::string &leaderTag);
    void beDepended(const std::string &name, const std::shared_ptr<BaseModule> &module);
    ModuleMap &getDependMap();
    ModuleMap &getBeDependedMap();
    BindFuncMap getBindFuncMap() const;

public:
    virtual const std::set<std::string> &getModuleDeps() { return _moduleDeps; }
    void initDepends() { defineDependModule(); };

protected:
    virtual bool doInit() { return true; };
    virtual bool doLoad() = 0;
    virtual bool doUnload() = 0;
    virtual bool doUpdate(Status status) { return true; }
    virtual void defineDependModule(){};

protected:
    bool isMaster() const;
    bool isLeader() const;

protected:
    ModuleManager *_moduleManager = nullptr;
    admin::SysController *_sysController = nullptr;
    config::AdminConfig *_adminConfig = nullptr;
    std::atomic_bool _load;
    common::LoadStatus _loadStatus;
    const std::set<std::string> _moduleDeps;
    std::string _name;
    bool _sysModule = false;
    ModuleMap _dependMap;
    ModuleMap _beDependedMap;
    BindFuncMap _bindFuncMap;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(BaseModule);

} // namespace admin
} // namespace swift

using ModuleCreatorFunc = std::function<std::shared_ptr<swift::admin::BaseModule>()>;

#define PP_CONCAT(a, b) a##b
#define INIT_ONCE_FUNC(n) PP_CONCAT(INIT_ONCE_FUNC_, n)
#define INIT_ONCE_FUNC_0                                                                                               \
    void defineDependModule() override {                                                                               \
        ::swift::common::DependMatcher<0> *matcher = nullptr;                                                          \
        dependModuleHelperFunc(matcher);                                                                               \
    }                                                                                                                  \
    template <int T>                                                                                                   \
    void dependModuleHelperFunc(::swift::common::DependMatcher<T> *matcher) {}

#define DEPEND_ON_MODULE(Module, member) DEPEND_ON_HELPER(Module, member, __COUNTER__)
#define DEPEND_ON_HELPER(Module, member, count) DEPEND_ON_IMPL(Module, member, count)
#define DEPEND_ON_IMPL(Module, member, count) INIT_ONCE_FUNC(count) DEPEND_ON_IMPL_REPEATED(Module, member, count)
#define DEPEND_ON_IMPL_REPEATED(Module, member, count)                                                                 \
private:                                                                                                               \
    std::shared_ptr<Module> member;                                                                                    \
                                                                                                                       \
public:                                                                                                                \
    void dependModuleHelperFunc(::swift::common::DependMatcher<count> *matcher) {                                      \
        ::swift::common::DependMatcher<count + 1> *next = nullptr;                                                     \
        _bindFuncMap[typeid(Module).name()] = [this](std::shared_ptr<::swift::admin::BaseModule> BaseModule) {         \
            this->_dependMap.emplace(std::make_pair(typeid(Module).name(), BaseModule));                               \
            this->member = std::dynamic_pointer_cast<Module>(BaseModule);                                              \
        };                                                                                                             \
        dependModuleHelperFunc(next);                                                                                  \
    }

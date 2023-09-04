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

#include "autil/Log.h"
#include "autil/Singleton.h"
#include "swift/admin/modules/BaseModule.h"
#include "swift/common/Common.h"

namespace swift {
namespace common {

template <RegisterType Type>
struct RegisterTypeTraits {
    typedef void CreatorFuncType;
};

#define REGISTER_TYPE_HELPER(t, c)                                                                                     \
    template <>                                                                                                        \
    struct RegisterTypeTraits<t> {                                                                                     \
        typedef c CreatorFuncType;                                                                                     \
    };

REGISTER_TYPE_HELPER(RegisterType::RT_MODULE, ModuleCreatorFunc);

template <RegisterType Type>
class CreatorRegistry : public autil::Singleton<CreatorRegistry<Type>> {
    using CreatorFuncType = typename RegisterTypeTraits<Type>::CreatorFuncType;
    using CreatorFuncMap = std::unordered_map<std::string, CreatorFuncType>;

public:
    CreatorRegistry() {}
    ~CreatorRegistry() {}

public:
    void addCreatorFunc(const std::string &name, CreatorFuncType creatorFuncType) {
        _creatorFuncMap[name] = std::move(creatorFuncType);
    }

    CreatorFuncType getCreatorFunc(const std::string &name = "default") {
        auto iter = _creatorFuncMap.find(name);
        if (iter == _creatorFuncMap.end()) {
            return CreatorFuncType();
        }
        return iter->second;
    }

    const CreatorFuncMap &getCreatorFuncMap() { return _creatorFuncMap; }

private:
    CreatorFuncMap _creatorFuncMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace swift

#define REGISTER_MODULE(Module, masterTag, leaderTag)                                                                  \
    __attribute__((constructor)) static void Register##Module() {                                                      \
        swift::common::CreatorRegistry<swift::common::RegisterType::RT_MODULE>::getInstance()->addCreatorFunc(         \
            typeid(Module).name(), []() -> std::shared_ptr<swift::admin::BaseModule> {                                 \
                auto module = std::make_shared<Module>();                                                              \
                module->setName(typeid(Module).name());                                                                \
                module->setLoadStatus(swift::admin::BaseModule::calcLoadStatus(masterTag, leaderTag));                 \
                return module;                                                                                         \
            });                                                                                                        \
    }

#define REGISTER_SYS_MODULE(Module)                                                                                    \
    __attribute__((constructor)) static void Register##Module() {                                                      \
        swift::common::CreatorRegistry<swift::common::RegisterType::RT_MODULE>::getInstance()->addCreatorFunc(         \
            typeid(Module).name(), []() -> std::shared_ptr<swift::admin::BaseModule> {                                 \
                auto module = std::make_shared<Module>();                                                              \
                module->setSysModule();                                                                                \
                module->setName(typeid(Module).name());                                                                \
                return module;                                                                                         \
            });                                                                                                        \
    }

#define REGISTER_DATA(Module) REGISTER_SYS_MODULE(Module)

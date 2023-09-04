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
#ifndef NAVI_KERNEL_H
#define NAVI_KERNEL_H

#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/KernelCreator.h"
#include "navi/engine/KernelInitContext.h"
#include "navi/log/NaviLogger.h"
#include <map>

namespace navi {

class NodeDef;
class Biz;

class Kernel
{
public:
    Kernel();
    virtual ~Kernel();
private:
    Kernel(const Kernel &);
    Kernel& operator=(const Kernel &);
public:
    virtual void def(KernelDefBuilder &builder) const = 0;
    virtual bool config(KernelConfigContext &ctx) {
        return true;
    }
    virtual ErrorCode init(KernelInitContext &ctx) {
        return EC_NONE;
    }
    virtual ErrorCode compute(KernelComputeContext &ctx) = 0;
    virtual bool isExpensive() const {
        return true;
    }
    virtual void __navi__declare_depend(navi::KernelDefBuilder &builder) {
    }
    inline void __navi__declare_depend_t(navi::DependMatcher<-2> *matcher, navi::KernelDefBuilder &builder) {}
public:
    const std::string &getNodeName() const;
    const std::string &getKernelName() const;
    const std::string &getDeviceName() const;
private:
    void setNodeDef(const NodeDef *def);
    friend class Node;
private:
    const NodeDef *_def;
};

NAVI_TYPEDEF_PTR(Kernel);

#define REGISTER_KERNEL(NAME) REGISTER_KERNEL_HELPER(__COUNTER__, NAME)
#define REGISTER_KERNEL_HELPER(ctr, NAME) REGISTER_KERNEL_UNIQ(ctr, NAME)
#define REGISTER_KERNEL_UNIQ(ctr, NAME)                                                                                \
    __attribute__((constructor)) static void Register##NAME##Kernel##ctr() {                                           \
        auto ret = ::navi::CreatorRegistry::current(navi::RT_KERNEL)->addCreatorFunc([]() -> ::navi::Creator * {       \
            auto def = new navi::KernelDef();                                                                          \
            navi::KernelDefBuilder builder(def);                                                                       \
            NAME thisKernel;                                                                                           \
            thisKernel.def(builder);                                                                                   \
            thisKernel.__navi__declare_depend(builder);                                                                \
            return new ::navi::KernelCreator(__FILE__, builder, [](void *&baseAddr) {                                  \
                auto ptr = new NAME();                                                                                 \
                baseAddr = ptr;                                                                                        \
                return ptr;                                                                                            \
            });                                                                                                        \
        });                                                                                                            \
        if (!ret) {                                                                                                    \
            NAVI_KERNEL_LOG(ERROR, "register kernel [%s] failed", #NAME);                                              \
        }                                                                                                              \
    }

#define REGISTER_KERNEL_INIT(FUNC)                                             \
    REGISTER_KERNEL_INIT_HELPER(__COUNTER__, FUNC)
#define REGISTER_KERNEL_INIT_HELPER(ctr, FUNC)                                 \
    REGISTER_KERNEL_INIT_UNIQ(ctr, FUNC)
#define REGISTER_KERNEL_INIT_UNIQ(ctr, FUNC)                                                                           \
    __attribute__((constructor)) static void Register##KernelInit##ctr() {                                             \
        auto ret = ::navi::CreatorRegistry::current(navi::RT_KERNEL)->addModuleInitFunc([]() {                         \
            if (!FUNC()) {                                                                                             \
                NAVI_KERNEL_LOG(ERROR, "module init failed");                                                          \
                return false;                                                                                          \
            }                                                                                                          \
            return true;                                                                                               \
        });                                                                                                            \
        if (!ret) {                                                                                                    \
            NAVI_KERNEL_LOG(ERROR, "register ModuleInitFunc [%s] failed", #FUNC);                                      \
        }                                                                                                              \
    }
}

#define KERNEL_DEPEND_DECLARE()                                                                                        \
public:                                                                                                                \
    void __navi__declare_depend(navi::KernelDefBuilder &builder) override {                                            \
        navi::DependMatcher<512> *matcherEnd = nullptr;                                                                \
        __navi__declare_depend_t(matcherEnd, builder);                                                                 \
    }                                                                                                                  \
                                                                                                                       \
protected:                                                                                                             \
    template <int N>                                                                                                   \
    void __navi__declare_depend_t(navi::DependMatcher<N> *matcher, navi::KernelDefBuilder &builder) {                  \
        __navi__declare_depend_t((navi::DependMatcher<N - 1> *)0, builder);                                            \
    }                                                                                                                  \
    using navi::Kernel::__navi__declare_depend_t;

#define KERNEL_DEPEND_DECLARE_BASE(BASE)                                                                               \
public:                                                                                                                \
    void __navi__declare_depend(navi::KernelDefBuilder &builder) override {                                            \
        BASE::__navi__declare_depend(builder);                                                                         \
        navi::DependMatcher<512> *matcherEnd = nullptr;                                                                \
        __navi__declare_depend_t(matcherEnd, builder);                                                                 \
    }                                                                                                                  \
                                                                                                                       \
protected:                                                                                                             \
    template <int N>                                                                                                   \
    void __navi__declare_depend_t(navi::DependMatcher<N> *matcher, navi::KernelDefBuilder &builder) {                  \
        __navi__declare_depend_t((navi::DependMatcher<N - 1> *)0, builder);                                            \
    }                                                                                                                  \
                                                                                                                       \
protected:                                                                                                             \
    using navi::Kernel::__navi__declare_depend_t;

#define KERNEL_DEPEND_ON(CLASS, NAME) KERNEL_DEPEND_ON_ID(CLASS, NAME, CLASS::RESOURCE_ID, true)
#define KERNEL_DEPEND_ON_FALSE(CLASS, NAME) KERNEL_DEPEND_ON_ID(CLASS, NAME, CLASS::RESOURCE_ID, false)

#define KERNEL_DEPEND_ON_ID(CLASS, NAME, ID, REQUIRE)                                                                  \
    KERNEL_DECLARE_DEPEND_ON_HELPER(__COUNTER__, CLASS, NAME, ID, REQUIRE)
#define KERNEL_DECLARE_DEPEND_ON_HELPER(ctr, CLASS, NAME, ID, REQUIRE)                                                 \
    KERNEL_DECLARE_DEPEND_ON_UNIQ(ctr, CLASS, NAME, ID, REQUIRE)
#define KERNEL_DECLARE_DEPEND_ON_UNIQ(ctr, CLASS, NAME, ID, REQUIRE)                                                   \
    CLASS *NAME = nullptr;                                                                                             \
    void __navi__declare_depend_t(navi::DependMatcher<ctr> *matcher, navi::KernelDefBuilder &builder) {                \
        builder.dependOn(ID, REQUIRE, BIND_RESOURCE_TO(NAME));                                                         \
        navi::DependMatcher<ctr - 1> *nextMatcher = nullptr;                                                           \
        __navi__declare_depend_t(nextMatcher, builder);                                                                \
    }

#define KERNEL_NAMED_DATA(CLASS, NAME, ID) KERNEL_DECLARE_NAMED_DATA_HELPER(__COUNTER__, CLASS, NAME, ID, true)
#define KERNEL_NAMED_DATA_FALSE(CLASS, NAME, ID) KERNEL_DECLARE_NAMED_DATA_HELPER(__COUNTER__, CLASS, NAME, ID, false)

#define KERNEL_DECLARE_NAMED_DATA_HELPER(ctr, CLASS, NAME, ID, REQUIRE)                                                \
    KERNEL_DECLARE_NAMED_DATA_UNIQ(ctr, CLASS, NAME, ID, REQUIRE)
#define KERNEL_DECLARE_NAMED_DATA_UNIQ(ctr, CLASS, NAME, ID, REQUIRE)                                                  \
    CLASS *NAME = nullptr;                                                                                             \
    void __navi__declare_depend_t(navi::DependMatcher<ctr> *matcher, navi::KernelDefBuilder &builder) {                \
        builder.namedData(ID, REQUIRE, BIND_DATA_TO(NAME));                                                            \
        navi::DependMatcher<ctr - 1> *nextMatcher = nullptr;                                                           \
        __navi__declare_depend_t(nextMatcher, builder);                                                                \
    }

#endif //NAVI_KERNEL_H

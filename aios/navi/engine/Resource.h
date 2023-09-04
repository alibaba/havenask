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
#ifndef NAVI_RESOURCE_H
#define NAVI_RESOURCE_H

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/engine/ResourceCreator.h"
#include "navi/engine/ResourceInitContext.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/log/NaviLogger.h"
#include <list>

namespace navi {

class ResourceMap;

struct StaticGraphInfo {
    std::string name;
    std::string meta;
    GraphDef def;
};

NAVI_TYPEDEF_PTR(StaticGraphInfo);

class Resource : public std::enable_shared_from_this<Resource> {
public:
    Resource() {}
    virtual ~Resource() {}
public:
    virtual void def(ResourceDefBuilder &builder) const = 0;
    virtual bool config(ResourceConfigContext &ctx) {
        return true;
    }
    virtual ErrorCode init(ResourceInitContext &ctx) = 0;
    virtual bool getMeta(std::string &meta) const {
        return false;
    }
    virtual bool getStaticGraphInfo(std::vector<StaticGraphInfoPtr> &graphInfos) const {
        return false;
    }
    virtual void __navi__declare_depend(navi::ResourceDefBuilder &builder) {
    }
    inline void __navi__declare_depend_t(navi::DependMatcher<-2> *matcher, navi::ResourceDefBuilder &builder) {}
public:
    const std::string &getName();
private:
    void setDepend(const ResourceMapPtr &resourceMap);
    void setResourceDef(const std::shared_ptr<ResourceDef> &def);
protected:
    void initLogger(NaviObjectLogger &logger, const std::string &prefix);
    const ResourceMap &getDepends() const;
private:
    friend class ResourceCreator;
    friend class ResourceManager;
    friend class ResourceCreateKernel;
    friend class NaviResourceHelper;
private:
    std::shared_ptr<ResourceDef> _def;
    ResourceMap _dependMap;
};

NAVI_TYPEDEF_PTR(Resource);

class RootResource : public Resource {
public:
    RootResource(const std::string &name);
    void def(ResourceDefBuilder &builder) const override final;
    bool config(ResourceConfigContext &ctx) override final {
        return true;
    }
    ErrorCode init(ResourceInitContext &ctx) override final {
        return EC_NONE;
    }
private:
    std::string _name;
};

#define REGISTER_RESOURCE(NAME) REGISTER_RESOURCE_HELPER(__COUNTER__, NAME)
#define REGISTER_RESOURCE_HELPER(ctr, NAME) REGISTER_RESOURCE_UNIQ(ctr, NAME)
#define REGISTER_RESOURCE_UNIQ(ctr, NAME)                                                                              \
    __attribute__((constructor)) static void Register##NAME##Resource() {                                              \
        auto ret = ::navi::CreatorRegistry::current(navi::RT_RESOURCE)->addCreatorFunc([]() -> ::navi::Creator * {     \
            auto def = new navi::ResourceDef();                                                                        \
            navi::ResourceDefBuilder builder(def);                                                                     \
            NAME thisResource;                                                                                         \
            thisResource.def(builder);                                                                                 \
            thisResource.__navi__declare_depend(builder);                                                              \
            return new ::navi::ResourceCreator(                                                                        \
                __FILE__,                                                                                              \
                builder,                                                                                               \
                [](void *&baseAddr) {                                                                                  \
                    auto ptr = new NAME();                                                                             \
                    baseAddr = ptr;                                                                                    \
                    navi::ResourcePtr resource(ptr);                                                                   \
                    return resource;                                                                                   \
                },                                                                                                     \
                [](navi::Resource *other) { return (nullptr != dynamic_cast<NAME *>(other)); });                       \
        });                                                                                                            \
        if (!ret) {                                                                                                    \
            NAVI_KERNEL_LOG(ERROR, "register resource [%s] failed", #NAME);                                            \
        }                                                                                                              \
    }

#define RESOURCE_ASSERT(resource)                                              \
    if (!resource) {                                                           \
        return navi::EC_LACK_RESOURCE;                                         \
    }

template <typename T>
bool bindDynamicResourceTo(navi::Resource *depend, std::set<T *> &member) {
    static_assert(std::is_base_of<navi::Resource, T>::value,
                  "illegal member type, use sub class of navi::Resource");
    static_assert(!std::is_same<navi::Resource, T>::value,
                  "illegal member type, can't use navi::Resource for match");
    auto *typedDepend = dynamic_cast<T *>(depend);
    if (!typedDepend) {
        return false;
    }
    member.insert(typedDepend);
    return true;
}

#define BIND_RESOURCE_TO(member)                                                                                       \
    [](void *obj, navi::Resource *depend) -> bool {                                                                    \
        using T = std::remove_const_t<std::remove_pointer_t<decltype(this)>>;                                          \
        auto *typedObj = static_cast<T *>(obj);                                                                        \
        typedObj->member = nullptr;                                                                                    \
        if (!depend) {                                                                                                 \
            return true;                                                                                               \
        }                                                                                                              \
        auto *typedDepend = dynamic_cast<decltype(typedObj->member)>(depend);                                          \
        typedObj->member = typedDepend;                                                                                \
        auto ret = (typedDepend != nullptr);                                                                           \
        if (!ret) {                                                                                                    \
            NAVI_KERNEL_LOG(ERROR,                                                                                     \
                            "cast resource failed, expect [%s], got [%s], member [%s]",                                \
                            typeid(decltype(typedObj->member)).name(),                                                 \
                            typeid(*depend).name(),                                                                    \
                            #member);                                                                                  \
        }                                                                                                              \
        return ret;                                                                                                    \
    }

#define BIND_DYNAMIC_RESOURCE_TO(member)                                                                               \
    [](void *obj, navi::Resource *depend, void *not_used) -> bool {                                                    \
        if (!depend) {                                                                                                 \
            return true;                                                                                               \
        }                                                                                                              \
        using T = std::remove_const_t<std::remove_pointer_t<decltype(this)>>;                                          \
        auto *typedObj = static_cast<T *>(obj);                                                                        \
        return bindDynamicResourceTo(depend, typedObj->member);                                                        \
    }
}

#define RESOURCE_DEPEND_DECLARE()                                                                                      \
public:                                                                                                                \
    __attribute__((always_inline)) void __navi__declare_depend(navi::ResourceDefBuilder &builder) override {           \
        navi::DependMatcher<512> *matcherEnd = nullptr;                                                                \
        __navi__declare_depend_t(matcherEnd, builder);                                                                 \
    }                                                                                                                  \
                                                                                                                       \
protected:                                                                                                             \
    template <int N>                                                                                                   \
    __attribute__((always_inline)) void __navi__declare_depend_t(navi::DependMatcher<N> *matcher,                      \
                                                                 navi::ResourceDefBuilder &builder) {                  \
        __navi__declare_depend_t((navi::DependMatcher<N - 1> *)0, builder);                                            \
    }                                                                                                                  \
    using navi::Resource::__navi__declare_depend_t;

#define RESOURCE_DEPEND_DECLARE_BASE(BASE)                                                                             \
public:                                                                                                                \
    __attribute__((always_inline)) void __navi__declare_depend(navi::ResourceDefBuilder &builder) override {           \
        BASE::__navi__declare_depend(builder);                                                                         \
        navi::DependMatcher<512> *matcherEnd = nullptr;                                                                \
        __navi__declare_depend_t(matcherEnd, builder);                                                                 \
    }                                                                                                                  \
                                                                                                                       \
protected:                                                                                                             \
    template <int N>                                                                                                   \
    __attribute__((always_inline)) void __navi__declare_depend_t(navi::DependMatcher<N> *matcher,                      \
                                                                 navi::ResourceDefBuilder &builder) {                  \
        __navi__declare_depend_t((navi::DependMatcher<N - 1> *)0, builder);                                            \
    }                                                                                                                  \
    using navi::Resource::__navi__declare_depend_t;

#define RESOURCE_DEPEND_ON(CLASS, NAME) RESOURCE_DEPEND_ON_ID(CLASS, NAME, CLASS::RESOURCE_ID, true)
#define RESOURCE_DEPEND_ON_FALSE(CLASS, NAME) RESOURCE_DEPEND_ON_ID(CLASS, NAME, CLASS::RESOURCE_ID, false)

#define RESOURCE_DEPEND_ON_ID(CLASS, NAME, ID, REQUIRE)                                                                \
    RESOURCE_DECLARE_DEPEND_ON_HELPER(__COUNTER__, CLASS, NAME, ID, REQUIRE)
#define RESOURCE_DECLARE_DEPEND_ON_HELPER(ctr, CLASS, NAME, ID, REQUIRE)                                               \
    RESOURCE_DECLARE_DEPEND_ON_UNIQ(ctr, CLASS, NAME, ID, REQUIRE)
#define RESOURCE_DECLARE_DEPEND_ON_UNIQ(ctr, CLASS, NAME, ID, REQUIRE)                                                 \
    CLASS *NAME = nullptr;                                                                                             \
    template <typename T>                                                                                              \
    static bool __navi_resource_binder_func_##NAME(void *obj, navi::Resource *depend) {                                \
        auto *typedObj = static_cast<T *>(obj);                                                                        \
        typedObj->NAME = nullptr;                                                                                      \
        if (!depend) {                                                                                                 \
            return true;                                                                                               \
        }                                                                                                              \
        auto *typedDepend = dynamic_cast<decltype(typedObj->NAME)>(depend);                                            \
        typedObj->NAME = typedDepend;                                                                                  \
        auto ret = (typedDepend != nullptr);                                                                           \
        if (!ret) {                                                                                                    \
            NAVI_KERNEL_LOG(ERROR,                                                                                     \
                            "cast resource failed, expect [%s], got [%s], member [%s]",                                \
                            typeid(decltype(typedObj->NAME)).name(),                                                   \
                            typeid(*depend).name(),                                                                    \
                            #NAME);                                                                                    \
        }                                                                                                              \
        return ret;                                                                                                    \
    }                                                                                                                  \
    __attribute__((always_inline)) void __navi__declare_depend_t(navi::DependMatcher<ctr> *matcher,                    \
                                                                 navi::ResourceDefBuilder &builder) {                  \
        builder.dependOn(                                                                                              \
            ID,                                                                                                        \
            REQUIRE,                                                                                                   \
            __navi_resource_binder_func_##NAME<std::remove_const_t<std::remove_pointer_t<decltype(this)>>>);           \
        navi::DependMatcher<ctr - 1> *nextMatcher = nullptr;                                                           \
        __navi__declare_depend_t(nextMatcher, builder);                                                                \
    }

#define RESOURCE_NAMED_DATA(CLASS, NAME, ID) RESOURCE_DECLARE_NAMED_DATA_HELPER(__COUNTER__, CLASS, NAME, ID, true)
#define RESOURCE_NAMED_DATA_FALSE(CLASS, NAME, ID)                                                                     \
    RESOURCE_DECLARE_NAMED_DATA_HELPER(__COUNTER__, CLASS, NAME, ID, false)

#define RESOURCE_DECLARE_NAMED_DATA_HELPER(ctr, CLASS, NAME, ID, REQUIRE)                                              \
    RESOURCE_DECLARE_NAMED_DATA_UNIQ(ctr, CLASS, NAME, ID, REQUIRE)
#define RESOURCE_DECLARE_NAMED_DATA_UNIQ(ctr, CLASS, NAME, ID, REQUIRE)                                                \
    CLASS *NAME = nullptr;                                                                                             \
    template <typename T>                                                                                              \
    static bool __navi_named_data_binder_func_##NAME(void *obj, navi::Data *namedData) {                               \
        auto *typedObj = static_cast<T *>(obj);                                                                        \
        typedObj->NAME = nullptr;                                                                                      \
        if (!namedData) {                                                                                              \
            return true;                                                                                               \
        }                                                                                                              \
        auto *typedData = dynamic_cast<decltype(typedObj->NAME)>(namedData);                                           \
        typedObj->NAME = typedData;                                                                                    \
        auto ret = (typedData != nullptr);                                                                             \
        if (!ret) {                                                                                                    \
            NAVI_KERNEL_LOG(ERROR,                                                                                     \
                            "cast named data failed, expect [%s], got [%s], member [%s]",                              \
                            typeid(decltype(typedObj->NAME)).name(),                                                   \
                            typeid(*namedData).name(),                                                                 \
                            #NAME);                                                                                    \
        }                                                                                                              \
        return ret;                                                                                                    \
    }                                                                                                                  \
    __attribute__((always_inline)) void __navi__declare_depend_t(navi::DependMatcher<ctr> *matcher,                    \
                                                                 navi::ResourceDefBuilder &builder) {                  \
        builder.namedData(                                                                                             \
            ID,                                                                                                        \
            REQUIRE,                                                                                                   \
            __navi_named_data_binder_func_##NAME<std::remove_const_t<std::remove_pointer_t<decltype(this)>>>);         \
        navi::DependMatcher<ctr - 1> *nextMatcher = nullptr;                                                           \
        __navi__declare_depend_t(nextMatcher, builder);                                                                \
    }

#endif //NAVI_RESOURCE_H

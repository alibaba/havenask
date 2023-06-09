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

#include "autil/plugin/BaseInterface.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/engine/ResourceCreator.h"
#include "navi/engine/ResourceInitContext.h"
#include "navi/log/NaviLogger.h"
#include <list>

namespace navi {

class ResourceMap;

class Resource : public autil::BaseInterface {
public:
    Resource();
    virtual ~Resource();
public:
    virtual void def(ResourceDefBuilder &builder) const = 0;
    virtual bool config(ResourceConfigContext &ctx) {
        return true;
    }
    virtual ErrorCode init(ResourceInitContext &ctx) = 0;
public:
    const std::string &getName();
public:
    void addDepend(const std::shared_ptr<Resource> &depend);
    void addDepend(const ResourceMap &resourceMap);
    void setResourceDef(const std::shared_ptr<ResourceDef> &def);
protected:
    void initLogger(NaviObjectLogger &logger, const std::string &prefix);
private:
    std::shared_ptr<ResourceDef> _def;
    std::list<std::shared_ptr<Resource>> _depends;
};

NAVI_TYPEDEF_PTR(Resource);

class RootResource : public Resource {
public:
    RootResource(const std::string &name);
    void def(ResourceDefBuilder &builder) const override;
    ErrorCode init(ResourceInitContext &ctx) override {
        return EC_NONE;
    }
private:
    std::string _name;
};

#define REGISTER_RESOURCE(NAME) REGISTER_RESOURCE_HELPER(__COUNTER__, NAME)
#define REGISTER_RESOURCE_HELPER(ctr, NAME) REGISTER_RESOURCE_UNIQ(ctr, NAME)
#define REGISTER_RESOURCE_UNIQ(ctr, NAME)                                      \
    __attribute__((constructor)) static void Register##NAME##Resource() {      \
        ::navi::CreatorRegistry::current(navi::RT_RESOURCE)                    \
            ->addCreatorFunc([]() -> ::navi::Creator * {                       \
                auto def = new navi::ResourceDef();                            \
                navi::ResourceDefBuilder builder(def);                         \
                NAME().def(builder);                                           \
                return new ::navi::ResourceCreator(                            \
                    builder, [](autil::mem_pool::Pool *pool) {                 \
                        if (pool) {                                            \
                            navi::ResourcePtr resource(                        \
                                NAVI_POOL_NEW_CLASS(pool, NAME),               \
                                [](navi::Resource *obj) {                      \
                                    NAVI_POOL_DELETE_CLASS(obj);               \
                                });                                            \
                            return resource;                                   \
                        } else {                                               \
                            navi::ResourcePtr resource(new NAME());            \
                            return resource;                                   \
                        }                                                      \
                    });                                                        \
            });                                                                \
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

#define BIND_RESOURCE_TO(member)                                               \
    [](void *obj, navi::Resource *depend) -> bool {                            \
        using T = std::remove_const_t<std::remove_pointer_t<decltype(this)> >; \
        auto *typedObj = static_cast<T *>(obj);                                \
        typedObj->member = nullptr;                                            \
        if (!depend) {                                                         \
            return true;                                                       \
        }                                                                      \
        auto *typedDepend = dynamic_cast<decltype(typedObj->member)>(depend);  \
        typedObj->member = typedDepend;                                        \
        return typedDepend != nullptr;                                         \
    }

#define BIND_DYNAMIC_RESOURCE_TO(member)                                       \
    [](void *obj, navi::Resource *depend, void *not_used) -> bool {            \
        if (!depend) {                                                         \
            return true;                                                       \
        }                                                                      \
        using T = std::remove_const_t<std::remove_pointer_t<decltype(this)> >; \
        auto *typedObj = static_cast<T *>(obj);                                \
        return bindDynamicResourceTo(depend, typedObj->member);                \
    }

}

#endif //NAVI_RESOURCE_H

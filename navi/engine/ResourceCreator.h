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
#ifndef NAVI_RESOURCECREATOR_H
#define NAVI_RESOURCECREATOR_H

#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/proto/KernelDef.pb.h"

namespace autil {
namespace mem_pool {
class Pool;
}
}

namespace navi {

class Resource;


class ResourceCreator : public Creator
{
private:
    typedef std::shared_ptr<Resource> ResourcePtr;
    typedef std::function<ResourcePtr(autil::mem_pool::Pool *pool)>
        ResourceCreateFunc;
public:
    ResourceCreator(const ResourceDefBuilder &builder,
                    const ResourceCreateFunc &func);
    virtual ~ResourceCreator();
private:
    ResourceCreator(const ResourceCreator &);
    ResourceCreator &operator=(const ResourceCreator &);
public:
    bool init() override;
    const std::string &getName() const override;
    ResourceLoadType getLoadType() const;
    const std::shared_ptr<ResourceDef> &def() const;
    ResourcePtr create(autil::mem_pool::Pool *pool) const;
    const std::map<std::string, bool> &getDependResources() const;
    const ResourceBindInfos &getBinderInfos() const;
private:
    bool initDependResources();
private:
    std::shared_ptr<ResourceDef> _def;
    ResourceCreateFunc _func;
    std::map<std::string, bool> _dependResources;
    ResourceBindInfos _binderInfos;
};

}

#endif //NAVI_RESOURCECREATOR_H

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
#include "navi/engine/ResourceCreator.h"
#include "navi/engine/Resource.h"

namespace navi {

ResourceCreator::ResourceCreator(const ResourceDefBuilder &builder,
                                 const ResourceCreateFunc &func)
    : _def(builder.def())
    , _func(func)
    , _binderInfos(builder.getBinderInfos())
{
}

ResourceCreator::~ResourceCreator() {
}

bool ResourceCreator::init() {
    return initDependResources();
}

bool ResourceCreator::initDependResources() {
    auto resourceCount = _def->depend_resources_size();
    for (int i = 0; i < resourceCount; i++) {
        const auto &resource = _def->depend_resources(i);
        const auto &name = resource.name();
        if (_dependResources.end() != _dependResources.find(name)) {
            NAVI_KERNEL_LOG(
                ERROR,
                "invalid resource def [%s], repeated depend resource [%s]",
                _def->resource_name().c_str(), name.c_str());
            return false;
        }
        _dependResources.emplace(name, resource.require());
    }
    return true;
}

const std::map<std::string, bool> &ResourceCreator::getDependResources() const {
    return _dependResources;
}

const std::string &ResourceCreator::getName() const {
    return _def->resource_name();
}

ResourceLoadType ResourceCreator::getLoadType() const {
    return _def->load_type();
}

const std::shared_ptr<ResourceDef> &ResourceCreator::def() const {
    return _def;
}

ResourcePtr ResourceCreator::create(autil::mem_pool::Pool *pool) const {
    auto resource = _func(pool);
    assert(resource);
    resource->setResourceDef(_def);
    return resource;
}

const ResourceBindInfos &ResourceCreator::getBinderInfos() const {
    return _binderInfos;
}

}

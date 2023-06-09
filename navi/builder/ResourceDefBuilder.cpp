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
#include "navi/builder/ResourceDefBuilder.h"

namespace navi {

ResourceDefBuilder::ResourceDefBuilder(ResourceDef *def)
    : _def(def)
{
    loadType(RLT_DEFAULT);
}

ResourceDefBuilder::~ResourceDefBuilder() {
}

ResourceDefBuilder &ResourceDefBuilder::name(const std::string &name) {
    _def->set_resource_name(name);
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::depend(const std::string &name,
                                               bool require,
                                               const StaticResourceBindFunc &binder)
{
    auto depend = _def->add_depend_resources();
    depend->set_name(name);
    depend->set_require(require);
    StaticResourceBindInfo bindInfo;
    bindInfo.name = name;
    bindInfo.require = require;
    bindInfo.binder = binder;
    _binderInfos.staticBinderVec.emplace_back(std::move(bindInfo));
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::dynamicDepend(
        const DynamicResourceBindFunc &binder,
        const std::set<std::string> &resourceSet)
{
    for (const auto &name : resourceSet) {
        auto depend = _def->add_depend_resources();
        depend->set_name(name);
        depend->set_require(true);
    }
    DynamicResourceBindInfo bindInfo;
    bindInfo.binder = binder;
    _binderInfos.dynamicBinderVec.emplace_back(std::move(bindInfo));
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::loadType(ResourceLoadType type) {
    _def->set_load_type(type);
    return *this;
}

ResourceDef *ResourceDefBuilder::def() const {
    return _def;
}

const ResourceBindInfos &ResourceDefBuilder::getBinderInfos() const {
    return _binderInfos;
}

}

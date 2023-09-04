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
    if (_def) {
        _def->set_version(NAVI_VERSION);
    }
}

ResourceDefBuilder::~ResourceDefBuilder() {
}

ResourceDefBuilder &ResourceDefBuilder::name(const std::string &name,
                                             ResourceStage stage)
{
    _def->set_name(name);
    _def->set_stage(stage);
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::dependOn(
        const std::string &name,
        bool require,
        const StaticResourceBindFunc &binder)
{
    auto dependInfo = _def->mutable_resource_depend_info();
    auto depend = dependInfo->add_depend_resources();
    depend->set_name(name);
    depend->set_require(require);
    StaticResourceBindInfo bindInfo;
    bindInfo.name = name;
    bindInfo.require = require;
    bindInfo.binder = binder;
    _bindInfos.staticBindVec.emplace_back(std::move(bindInfo));
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::namedData(
        const std::string &name,
        bool require,
        const NamedDataBindFunc &binder)
{
    auto dependInfo = _def->mutable_resource_depend_info();
    auto dataDef = dependInfo->add_depend_named_data();
    dataDef->set_name(name);
    dataDef->set_require(require);
    NamedDataBindInfo bindInfo;
    bindInfo.name = name;
    bindInfo.require = require;
    bindInfo.binder = binder;
    _bindInfos.namedDataBindVec.emplace_back(std::move(bindInfo));
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::dependByResource(
        const std::string &resource,
        const std::string &dynamicGroup)
{
    auto dependBy = _def->add_depend_bys();
    dependBy->set_type(DBT_RESOURCE);
    dependBy->set_name(resource);
    dependBy->set_dynamic_group(dynamicGroup);
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::dependByKernel(
        const std::string &kernel,
        const std::string &dynamicGroup)
{
    auto dependBy = _def->add_depend_bys();
    dependBy->set_type(DBT_KERNEL);
    dependBy->set_name(kernel);
    dependBy->set_dynamic_group(dynamicGroup);
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::enableBuildR(const std::vector<std::string> &candidates) {
    auto dependInfo = _def->mutable_resource_depend_info();
    auto enableDef = dependInfo->mutable_enable_builds();
    for (const auto &candidate : candidates) {
        enableDef->add_candidates(candidate);
    }
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::dynamicResourceGroup(
    const std::string &groupName,
    const DynamicResourceBindFunc &binder)
{
    auto dependInfo = _def->mutable_resource_depend_info();
    auto groupDef = dependInfo->add_dynamic_resource_groups();
    groupDef->set_group_name(groupName);
    DynamicResourceBindInfo bindInfo;
    bindInfo.group = groupName;
    bindInfo.binder = binder;
    _bindInfos.dynamicBindVec.emplace_back(std::move(bindInfo));
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::produce(const std::string &name) {
    auto produceInfo = _def->mutable_produce_info();
    produceInfo->set_name(name);
    return *this;
}

ResourceDefBuilder &ResourceDefBuilder::enableReplaceR(const std::string &name) {
    auto replaceInfo = _def->add_replace_enables();
    replaceInfo->set_name(name);
    return *this;
}

ResourceDef *ResourceDefBuilder::def() const {
    return _def;
}

const BindInfos &ResourceDefBuilder::getBindInfos() const {
    return _bindInfos;
}

}

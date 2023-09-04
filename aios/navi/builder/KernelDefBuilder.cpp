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
#include "navi/builder/KernelDefBuilder.h"

namespace navi {

KernelDefBuilder::KernelDefBuilder(KernelDef *def)
    : _def(def)
{
    if (_def) {
        _def->set_version(NAVI_VERSION);
    }
}

KernelDefBuilder::~KernelDefBuilder() {
}

KernelDefBuilder &KernelDefBuilder::name(const std::string &name) {
    _def->set_kernel_name(name);
    return *this;
}

KernelDefBuilder &KernelDefBuilder::input(const std::string &name,
                                          const std::string &typeId,
                                          InputTypeDef inputType)
{
    auto input = _def->add_inputs();
    input->set_name(name);
    input->set_type(inputType);
    auto dataType = input->mutable_data_type();
    dataType->set_name(typeId);
    return *this;
}

KernelDefBuilder &KernelDefBuilder::inputGroup(const std::string &name,
                                               const std::string &typeId,
                                               InputTypeDef channelInputType,
                                               InputTypeDef groupInputType) {
    auto group = _def->add_input_groups();
    group->set_name(name);
    group->set_type(channelInputType);
    group->set_group_type(groupInputType);
    auto dataType = group->mutable_data_type();
    dataType->set_name(typeId);
    return *this;
}

KernelDefBuilder &KernelDefBuilder::output(const std::string &name,
                                           const std::string &typeId)
{
    auto output = _def->add_outputs();
    output->set_name(name);
    auto dataType = output->mutable_data_type();
    dataType->set_name(typeId);
    return *this;
}

KernelDefBuilder &KernelDefBuilder::outputGroup(const std::string &name,
                                               const std::string &typeId)
{
    auto group = _def->add_output_groups();
    group->set_name(name);
    auto dataType = group->mutable_data_type();
    dataType->set_name(typeId);
    return *this;
}

KernelDefBuilder &KernelDefBuilder::dependOn(
        const std::string &name,
        bool require,
        const StaticResourceBindFunc &binder)
{
    auto dependInfo = _def->mutable_resource_depend_info();
    auto resource = dependInfo->add_depend_resources();
    resource->set_name(name);
    resource->set_require(require);
    StaticResourceBindInfo bindInfo;
    bindInfo.name = name;
    bindInfo.require = require;
    bindInfo.binder = binder;
    _bindInfos.staticBindVec.emplace_back(std::move(bindInfo));
    return *this;
}

KernelDefBuilder &KernelDefBuilder::resourceConfigKey(const std::string &name, const std::string &configKey) {
    auto key = _def->add_config_keys();
    key->set_name(name);
    key->set_config_key(configKey);
    return *this;
}

KernelDefBuilder &KernelDefBuilder::selectR(const std::vector<std::string> &candidates,
                                            const ResourceSelectFunc &selectorFunc,
                                            bool require,
                                            const StaticResourceBindFunc &binder)
{
    auto dependInfo = _def->mutable_resource_depend_info();
    auto selector = dependInfo->add_selectors();
    for (const auto &candidate : candidates) {
        selector->add_candidates(candidate);
    }
    selector->set_require(require);
    SelectResourceBindInfo bindInfo;
    bindInfo.candidates = candidates;
    bindInfo.require = require;
    bindInfo.selector = selectorFunc;
    bindInfo.binder = binder;
    _bindInfos.selectBindVec.emplace_back(std::move(bindInfo));
    return *this;
}

KernelDefBuilder &KernelDefBuilder::enableBuildR(const std::vector<std::string> &candidates) {
    auto dependInfo = _def->mutable_resource_depend_info();
    auto enableDef = dependInfo->mutable_enable_builds();
    for (const auto &candidate : candidates) {
        enableDef->add_candidates(candidate);
    }
    return *this;
}

KernelDefBuilder &KernelDefBuilder::dynamicResourceGroup(
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

KernelDefBuilder &KernelDefBuilder::namedData(const std::string &name,
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

KernelDefBuilder &KernelDefBuilder::jsonAttrs(const std::string &attrs) {
    _def->set_json_attrs(attrs);
    return *this;
}

KernelDef *KernelDefBuilder::def() const {
    return _def;
}

const BindInfos &KernelDefBuilder::getBindInfos() const {
    return _bindInfos;
}

}

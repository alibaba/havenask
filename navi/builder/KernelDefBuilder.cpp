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
                                               InputTypeDef inputType)
{
    auto group = _def->add_input_groups();
    group->set_name(name);
    group->set_type(inputType);
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

KernelDefBuilder &KernelDefBuilder::resource(const std::string &name,
                                             bool require,
                                             const StaticResourceBindFunc &binder)
{
    auto resource = _def->add_depend_resources();
    resource->set_name(name);
    resource->set_require(require);
    StaticResourceBindInfo bindInfo;
    bindInfo.name = name;
    bindInfo.require = require;
    bindInfo.binder = binder;
    _binderInfos.staticBinderVec.emplace_back(std::move(bindInfo));
    return *this;
}

KernelDefBuilder &KernelDefBuilder::dynamicResource(
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

KernelDef *KernelDefBuilder::def() const {
    return _def;
}

const ResourceBindInfos &KernelDefBuilder::getBinderInfos() const {
    return _binderInfos;
}

}

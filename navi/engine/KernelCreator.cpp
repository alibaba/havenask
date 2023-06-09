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
#include "navi/engine/KernelCreator.h"
#include "navi/log/NaviLogger.h"
#include "navi/util/CommonUtil.h"
#include "navi/ops/ResourceData.h"

namespace navi {

KernelCreator::KernelCreator(const KernelDefBuilder &builder,
                             const KernelCreateFunc &func)

    : _def(builder.def())
    , _func(func)
    , _binderInfos(builder.getBinderInfos())
{}

KernelCreator::~KernelCreator() {
    DELETE_AND_SET_NULL(_def);
}

bool KernelCreator::init() {
    return initInputIndex()
        && initOutputIndex()
        && initInputGroupIndex()
        && initOutputGroupIndex()
        && initDependResources();
}

bool KernelCreator::initInputIndex() {
    for (int32_t i = 0; i < _def->inputs().size(); i++) {
        const auto &inputDef = _def->inputs(i);
        auto it = _inputMap.find(inputDef.name());
        if (_inputMap.end() != it) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid kernel def [%s], input port [%s] defined "
                            "more than once",
                            _def->kernel_name().c_str(),
                            inputDef.name().c_str());
            return false;
        }
        _inputMap.emplace(inputDef.name(),
                          PortInfo(PortIndex(i, INVALID_INDEX), &inputDef));
    }
    return true;
}

bool KernelCreator::initOutputIndex() {
    auto outputSize = _def->outputs().size();
    for (int32_t i = 0; i < outputSize; i++) {
        const auto &outputDef = _def->outputs(i);
        auto it = _outputMap.find(outputDef.name());
        if (_outputMap.end() != it) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid kernel def [%s], output port [%s] defined "
                            "more than once",
                            _def->kernel_name().c_str(),
                            outputDef.name().c_str());
            return false;
        }
        _outputMap.insert(std::make_pair(outputDef.name(),
                        PortInfo(PortIndex(i, INVALID_INDEX), &outputDef)));
    }
    _controlOutputMap.emplace(
        NODE_FINISH_PORT,
        PortInfo(PortIndex(0, INVALID_INDEX, true), &_finishPortDef));
    return true;
}

bool KernelCreator::initInputGroupIndex() {
    for (int32_t i = 0; i < _def->input_groups().size(); i++) {
        const auto &inputGroupDef = _def->input_groups(i);
        auto it = _inputGroupMap.find(inputGroupDef.name());
        if (_inputGroupMap.end() != it) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid kernel def [%s], input group port [%s] "
                            "defined more than once",
                            _def->kernel_name().c_str(),
                            inputGroupDef.name().c_str());
            return false;
        }
        _inputGroupMap.emplace(
            inputGroupDef.name(),
            PortInfo(PortIndex(INVALID_INDEX, i), &inputGroupDef));
    }
    return true;
}

bool KernelCreator::initOutputGroupIndex() {
    for (int32_t i = 0; i < _def->output_groups().size(); i++) {
        const auto &outputGroupDef = _def->output_groups(i);
        auto it = _outputGroupMap.find(outputGroupDef.name());
        if (_outputGroupMap.end() != it) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid kernel def [%s], output group port [%s] "
                            "defined more than once",
                            _def->kernel_name().c_str(),
                            outputGroupDef.name().c_str());
            return false;
        }
        _outputGroupMap.emplace(
            outputGroupDef.name(),
            PortInfo(PortIndex(INVALID_INDEX, i), &outputGroupDef));
    }
    return true;
}

bool KernelCreator::initDependResources() {
    auto resourceCount = _def->depend_resources_size();
    for (int i = 0; i < resourceCount; i++) {
        const auto &resource = _def->depend_resources(i);
        const auto &name = resource.name();
        if (_dependResources.end() != _dependResources.find(name)) {
            NAVI_KERNEL_LOG(
                ERROR, "invalid kernel def [%s], repeated depend resource [%s]",
                _def->kernel_name().c_str(), name.c_str());
            return false;
        }
        _dependResources.emplace(name, resource.require());
    }
    return true;
}

const std::map<std::string, bool> &KernelCreator::getDependResources() const {
    return _dependResources;
}

Kernel *KernelCreator::create(autil::mem_pool::Pool *pool) const {
    return _func(pool);
}

size_t KernelCreator::inputSize() const {
    return _inputMap.size();
}

size_t KernelCreator::outputSize() const {
    return _outputMap.size();
}

size_t KernelCreator::controlOutputSize() const {
    return _controlOutputMap.size();
}

size_t KernelCreator::inputGroupSize() const {
    return _inputGroupMap.size();
}

size_t KernelCreator::outputGroupSize() const {
    return _outputGroupMap.size();
}

PortInfo KernelCreator::getInputPortInfo(const std::string &name) const {
    std::string port;
    IndexType index = 0;
    if (!CommonUtil::parseGroupPort(name, port, index)) {
        return PortInfo();
    }
    return getInputPortInfo(port, index);
}

PortInfo KernelCreator::getInputPortInfo(const std::string &port, IndexType index) const {
    if (0 == index) {
        auto inputIt = _inputMap.find(port);
        if (_inputMap.end() != inputIt) {
            return inputIt->second;
        }
    }
    auto inputGroupIt = _inputGroupMap.find(port);
    if (_inputGroupMap.end() != inputGroupIt) {
        auto ret = inputGroupIt->second;
        ret.index.index = index;
        return ret;
    }
    return PortInfo(PortIndex(), nullptr);
}

PortInfo KernelCreator::getOutputPortInfo(const std::string &name) const {
    std::string port;
    IndexType index = 0;
    if (!CommonUtil::parseGroupPort(name, port, index)) {
        return PortInfo();
    }
    if (0 == index) {
        auto outputIt = _outputMap.find(port);
        if (_outputMap.end() != outputIt) {
            return outputIt->second;
        }
    }
    auto outputGroupIt = _outputGroupMap.find(port);
    if (_outputGroupMap.end() != outputGroupIt) {
        auto ret = outputGroupIt->second;
        ret.index.index = index;
        return ret;
    }
    auto controlIt = _controlOutputMap.find(port);
    if (_controlOutputMap.end() != controlIt) {
        return controlIt->second;
    }
    return PortInfo(PortIndex(), nullptr);
}

std::string KernelCreator::getInputPortName(PortIndex portIndex) const {
    if (!portIndex.isGroup()) {
        auto index = portIndex.index;
        if (index >= _def->inputs().size()) {
            return std::string();
        }
        return _def->inputs(index).name();
    } else {
        auto group = portIndex.group;
        if (group >= _def->input_groups().size()) {
            return std::string();
        }
        return _def->input_groups(group).name();
    }
}

std::string KernelCreator::getOutputPortName(PortIndex portIndex) const {
    if (!portIndex.isGroup()) {
        auto index = portIndex.index;
        if (index >= _def->outputs().size()) {
            return std::string();
        }
        return _def->outputs(index).name();
    } else {
        auto group = portIndex.group;
        if (group >= _def->output_groups().size()) {
            return std::string();
        }
        return _def->output_groups(group).name();
    }
}

bool KernelCreator::getPortTypeStr(const std::string &portName,
                                   IoType ioType,
                                   std::string &typeStr) const
{
    if (0 == portName.find(NODE_RESOURCE_INPUT_PORT)) {
        typeStr = ResourceDataType::RESOURCE_DATA_TYPE_ID;
        return true;
    }
    if (IOT_INPUT == ioType) {
        auto portInfo = getInputPortInfo(portName);
        if (!portInfo.isValid()) {
            NAVI_KERNEL_LOG(ERROR, "kernel [%s] has no input port [%s]",
                            getName().c_str(), portName.c_str());
            return false;
        }
        auto portDef = static_cast<const InputDef *>(portInfo.def);
        assert(portDef);
        typeStr = portDef->data_type().name();
    } else {
        const auto &portInfo = getOutputPortInfo(portName);
        if (!portInfo.isValid()) {
            NAVI_KERNEL_LOG(ERROR, "kernel [%s] has no output port [%s]",
                            getName().c_str(), portName.c_str());
            return false;
        }
        auto portDef = static_cast<const OutputDef *>(portInfo.def);
        assert(portDef);
        typeStr = portDef->data_type().name();
    }
    return true;
}

const KernelDef *KernelCreator::def() const {
    return _def;
}

const ResourceBindInfos &KernelCreator::getBinderInfos() const {
    return _binderInfos;
}

}

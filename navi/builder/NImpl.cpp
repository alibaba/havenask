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
#include "navi/builder/NImpl.h"
#include "autil/legacy/jsonizable.h"
#include "navi/builder/PImpl.h"
#include "navi/proto/GraphDef.pb.h"

namespace navi {

NImpl::NImpl()
    : _builder(nullptr)
    , _graphId(INVALID_GRAPH_ID)
    , _def(nullptr)
    , _nodeType(NT_NORMAL)
{
}

NImpl::~NImpl() {
    for (auto p : _ins) {
        delete p.second;
    }
    for (auto p : _outs) {
        delete p.second;
    }
}

void NImpl::init(GraphBuilder *builder, GraphId graphId, NodeDef *def, NodeType nodeType) {
    _builder = builder;
    _graphId = graphId;
    _def = def;
    _nodeType = nodeType;
}

void NImpl::kernel(const std::string &name) {
    _def->set_kernel_name(name);
}

void NImpl::device(const std::string &device) {
    _def->set_device(device);
}

void NImpl::jsonAttrs(const std::string &attrs) {
    setJsonAttr(attrs);
}

void NImpl::jsonAttrsFromMap(const std::map<std::string, std::string> &attrs) {
    setJsonAttr(autil::legacy::FastToJsonString(attrs));
}

void NImpl::jsonAttrsFromJsonMap(const autil::legacy::json::JsonMap &attrs) {
    setJsonAttr(autil::legacy::ToJsonString(attrs));
}

void NImpl::binaryAttrsFromMap(
    const std::map<std::string, std::string> &binaryAttrs)
{
    for (const auto &pair : binaryAttrs) {
        attr(pair.first, pair.second);
    }
}

void NImpl::attr(const std::string &key, const std::string &value) {
    auto binaryAttrMap = _def->mutable_binary_attrs();
    (*binaryAttrMap)[key] = value;
}

void NImpl::integerAttr(const std::string &key, int64_t value) {
    auto integerAttrMap = _def->mutable_integer_attrs();
    (*integerAttrMap)[key] = value;
}

void NImpl::setJsonAttr(const std::string &attrStr) {
    _def->set_json_attrs(attrStr);
}

void NImpl::skipConfig(bool skipConfig) {
    auto buildinAttr = _def->mutable_buildin_attrs();
    buildinAttr->set_skip_config(skipConfig);
}

void NImpl::skipInit(bool skipInit) {
    auto buildinAttr = _def->mutable_buildin_attrs();
    buildinAttr->set_skip_init(skipInit);
}

void NImpl::stopAfterInit(bool stopAfterInit) {
    auto buildinAttr = _def->mutable_buildin_attrs();
    buildinAttr->set_stop_after_init(stopAfterInit);
}

void NImpl::skipDeleteKernel(bool skipDeleteKernel) {
    auto buildinAttr = _def->mutable_buildin_attrs();
    buildinAttr->set_skip_delete_kernel(skipDeleteKernel);
}

P NImpl::in(const std::string &port) {
    auto it = _ins.find(port);
    if (it != _ins.end()) {
        return P(it->second, INVALID_INDEX);
    }
    auto impl = new PImpl(this, port, true);
    _ins.emplace(port, impl);
    return P(impl, INVALID_INDEX);
}

P NImpl::out(const std::string &port) {
    auto it = _outs.find(port);
    if (it != _outs.end()) {
        return P(it->second, INVALID_INDEX);
    }
    auto impl = new PImpl(this, port, false);
    _outs.emplace(port, impl);
    return P(impl, INVALID_INDEX);
}

void NImpl::dependOn(P p) {
    auto impl = p._impl;
    if (impl->getNode() == this) {
        throw autil::legacy::ExceptionBase("error, self dependency detected: " + autil::StringUtil::toString(*this));
    }
    auto it = _dependOns.find(impl);
    if (it != _dependOns.end()) {
        return;
    }
    auto controlInput = in(NODE_CONTROL_INPUT_PORT);
    auto e = controlInput.from(p);
    e.require(true);
    _dependOns.emplace(impl);
}

GraphBuilder *NImpl::builder() const {
    return _builder;
}

const std::string &NImpl::name() const {
    return _def->name();
}

const std::string &NImpl::kernel() const {
    return _def->kernel_name();
}

GraphId NImpl::graphId() const {
    return _graphId;
}

std::ostream &operator<<(std::ostream &os, const NImpl &impl) {
    os << "graphId " << impl.graphId() << " node " << impl.name() << " type " << NodeType_Name(impl._nodeType);
    return os;
}

} // namespace navi

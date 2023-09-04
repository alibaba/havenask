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
#include "navi/builder/SubGraphBuildInfo.h"

namespace navi {

NImpl::NImpl()
    : _subGraphBuildInfo(nullptr)
    , _def(nullptr)
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

void NImpl::init(NodeDef *def, SubGraphBuildInfo *subGraphBuildInfo) {
    _def = def;
    setNodeType(def->type());
    _subGraphBuildInfo = subGraphBuildInfo;
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
    auto e = controlInput.autoNext().from(p);
    e.require(true);
    _dependOns.emplace(impl);
}

GraphBuilder *NImpl::builder() const {
    return _subGraphBuildInfo->getBuilder();
}

const std::string &NImpl::name() const {
    return _def->name();
}

const std::string &NImpl::kernel() const {
    return _def->kernel_name();
}

GraphId NImpl::graphId() const {
    return _subGraphBuildInfo->getGraphId();
}

int32_t NImpl::getScope() const {
    return _def->scope();
}

void NImpl::initScopeBorder(IoType ioType) {
    auto nodeType = getNodeType();
    if (nodeType != NT_NORMAL && nodeType != NT_SPLIT && nodeType != NT_MERGE) {
        return;
    }
    auto isFork = _subGraphBuildInfo->isFork();
    if ((isFork && IOT_INPUT != ioType) || (!isFork && IOT_OUTPUT != ioType)) {
        return;
    }
    setScopeBorder();
}

bool NImpl::isScopeBorder() const {
    return _def->is_scope_border();
}

void NImpl::setScopeBorder() {
    _def->set_is_scope_border(true);
    if (!_scopeTerminatorInput._impl) {
        auto scopeTerminatorImpl = _subGraphBuildInfo->getScopeTerminator(getScope());
        if (!scopeTerminatorImpl) {
            return;
        }
        N scopeTerminator(scopeTerminatorImpl);
        _scopeTerminatorInput = scopeTerminator.in(SCOPE_TERMINATOR_INPUT_PORT);
        _scopeTerminatorInput.autoNext().from(out(NODE_FINISH_PORT));
    }
}

std::ostream &operator<<(std::ostream &os, const NImpl &impl) {
    os << "graphId " << impl.graphId() << " node " << impl.name() << " type " << NodeType_Name(impl.getNodeType());
    return os;
}

} // namespace navi

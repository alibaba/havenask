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
#include "aios/apps/facility/cm2/cm_sub/topo_node.h"

namespace cm_sub {

using namespace cm_basic;

TopoNode::TopoNode(const std::shared_ptr<TopoNode>& topoNode)
{
    _node = topoNode->_node;
    _hashWeightRatio = topoNode->_hashWeightRatio;
    _virtualNodeRatio = topoNode->_virtualNodeRatio;
    _version = topoNode->_version;
    _tWeight = topoNode->_tWeight;
}

bool TopoNode::hasValidLBInfoWeight()
{
    if (!_node) {
        return false;
    }
    return _node->hasLBInfo() && _node->getLBInfoWeight() > 0;
}

int32_t TopoNode::getLBInfoWeight()
{
    if (!_node) {
        return 0;
    }
    return _node->getLBInfoWeight();
}

void TopoNode::setLBInfoWeight(int32_t weight)
{
    if (!_node) {
        return;
    }
    _node->setLBInfoWeight(weight);
}

int TopoNode::getWeight()
{
    if (_tWeight >= 0) {
        return _tWeight;
    }
    if (_node && _node->hasLBInfo() && _node->getLBInfoWeight() > 0) {
        return _node->getLBInfoWeight();
    }
    return 0;
}

int TopoNode::getConhashReplica()
{
    int weight = getWeight();
    return _hashWeightRatio == 0 ? 1 : (weight > 0 ? weight / _hashWeightRatio : 0);
}

bool TopoNode::isMatch(ProtocolType protocol, IDCType idc)
{
    if (_node == nullptr) {
        return false;
    }
    if (idc != IDC_ANY && _node->getIDCType() != IDC_ANY && idc != _node->getIDCType()) {
        // 当前节点所在的机房不满足要求
        return false;
    }
    if (protocol != PT_ANY) {
        int32_t cnt = _node->getProtoPortSize();
        int32_t i = 0;
        for (i = 0; i < cnt; ++i) {
            if (_node->getProtoType(i) == protocol) {
                break;
            }
        }
        // 当前节点的协议类型不满足要求
        if (i == cnt) {
            return false;
        }
    }
    return true;
}

bool TopoNode::isMatchValid(ProtocolType protocol, IDCType idc)
{
    if (_node == nullptr) {
        return false;
    }
    if (_node->getCurStatus() != NS_NORMAL) {
        return false;
    }
    return isMatch(protocol, idc);
}

} // namespace cm_sub
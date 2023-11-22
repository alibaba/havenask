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
#include "navi/engine/GraphBorder.h"
#include "navi/util/CommonUtil.h"

namespace navi {

GraphBorder::GraphBorder(const NaviLoggerPtr &logger, GraphParam *param)
    : _logger(this, "border", logger)
    , _param(param)
{
}

GraphBorder::~GraphBorder() {
}

bool GraphBorder::collectSubGraph(const SubGraphDef &subGraphDef,
                                  const GraphDomainPtr &domain)
{
    auto graphId = subGraphDef.graph_id();
    auto borderCount = subGraphDef.borders_size();
    auto thisPartId = subGraphDef.location().this_part_id();
    auto &partInfoDef = subGraphDef.location().part_info();
    NAVI_LOG(SCHEDULE1, "collect sub graph, location [%s]",
             subGraphDef.location().ShortDebugString().c_str());
    if (0 == borderCount) {
        if (subGraphDef.has_option() && subGraphDef.option().ignore_isolate()) {
            return true;
        }
        if (0 == subGraphDef.nodes_size()) {
            NAVI_LOG(ERROR, "sub graph [%d] has no nodes, biz [%s]", graphId,
                     subGraphDef.location().biz_name().c_str());
            return false;
        } else {
            NAVI_LOG(ERROR,
                     "graph has isolated sub graph [%d], check graph topo or "
                     "graph output, biz [%s]",
                     graphId, subGraphDef.location().biz_name().c_str());
            return false;
        }
    }
    for (int32_t index = 0; index < borderCount; index++) {
        const auto &borderDef = subGraphDef.borders(index);
        auto subBorderPtr = addSubBorder(thisPartId, graphId, borderDef, domain);
        if (!subBorderPtr->init(partInfoDef, borderDef, domain.get())) {
            return false;
        }
        domain->addSubBorder(subBorderPtr);
    }
    return true;
}

SubGraphBorderPtr GraphBorder::addSubBorder(NaviPartId partId,
                                            GraphId graphId,
                                            const BorderDef &borderDef,
                                            const GraphDomainPtr &domain)
{
    BorderId borderId(graphId, borderDef);
    auto it = _borderMap.find(borderId);
    if (_borderMap.end() != it) {
        return it->second;
    } else {
        SubGraphBorderPtr border(new SubGraphBorder(domain->getLogger().logger,
                                                    _param, partId, borderId));
        _borderMap.emplace(borderId, border);
        return border;
    }
}

SubGraphBorderMapPtr GraphBorder::getBorderMap(GraphId graphId) const {
    auto borderMap = new SubGraphBorderMap();
    for (const auto &pair : _borderMap) {
        const auto &borderId = pair.first;
        if (borderId.id == graphId) {
            borderMap->emplace(pair);
        }
    }
    return SubGraphBorderMapPtr(borderMap);
}

std::ostream& operator<<(std::ostream& os, const GraphBorder& border) {
    os << &border << "{" << std::endl;
    for (const auto &pair : border._borderMap) {
        os << *pair.second;
    }
    os << std::endl <<"}";
    return os;
}

bool GraphBorder::weave() {
    for (const auto &thisPair : _borderMap) {
        const auto &thisBorderId = thisPair.first;
        for (const auto &otherPair : _borderMap) {
            const auto &otherBorderId = otherPair.first;
            if (!match(thisBorderId, otherBorderId)) {
                continue;
            }
            if (thisBorderId.ioType == IOT_INPUT) {
                continue;
            }
            if (!thisPair.second->linkTo(*otherPair.second)) {
                return false;
            }
            break;
        }
    }
    bool ret = true;
    for (const auto &pair : _borderMap) {
        const auto &subBorder = *pair.second;
        if (!subBorder.linked()) {
            NAVI_LOG(ERROR, "border peer sub graph [%d] not exist, border [%s]",
                     pair.first.peer, subBorder.getBorderIdStr().c_str());
            ret = false;
        }
    }
    return ret;
}

bool GraphBorder::match(const BorderId &thisBorder,
                        const BorderId &otherBorder)
{
    auto otherIoType = CommonUtil::getReverseIoType(thisBorder.ioType);
    return otherIoType == otherBorder.ioType
        && thisBorder.id == otherBorder.peer
        && thisBorder.peer == otherBorder.id;
}

}

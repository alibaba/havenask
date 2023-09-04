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
#ifndef NAVI_SUBGRAPHBORDER_H
#define NAVI_SUBGRAPHBORDER_H

#include "navi/common.h"
#include "navi/builder/BorderId.h"
#include "navi/engine/PartState.h"
#include "navi/engine/Port.h"
#include <unordered_map>
#include <vector>

namespace navi {

class GraphDomain;

class SubGraphBorder {
private:
    struct SamePortVec {
        uint32_t linkIndex = 0;
        std::vector<Port *> ports;
    };
public:
    SubGraphBorder(GraphParam *param, NaviPartId partId,
                   const BorderId &borderId);
    virtual ~SubGraphBorder();
public:
    bool init(const PartInfoDef &partInfo, const BorderDef &borderDef,
              GraphDomain *domain);
    GraphParam *getGraphParam() const;
    bool addPort(Port *port);
    Port *getPort(const std::string &node, const std::string &port);
    Port *getPort(PortId portId) const;
    bool linkTo(SubGraphBorder &toBorder);
    bool linked() const;
    const BorderId &getBorderId() const;
    std::string getBorderIdStr() const;
    const std::vector<Port *> &getPortVec() const;
public:
    void notifySend(NaviPartId partId);
    bool collectBorderMessage(NaviPartId partId, NaviBorderData &borderMessage);
    bool receive(NaviBorderData &borderData);
    void incMessageCount(NaviPartId partId);
    void decMessageCount(NaviPartId partId);
private:
    void initBorderInfo(const PartInfoDef &partInfo, GraphId peer,
                        GraphDomain *domain);
    void initPartInfo(PartInfoDef partInfoDef, GraphDomain *domain);
    Port *createPort(IoType ioType, const BorderEdgeDef &borderEdgeDef);
    Port *doCreateLocalPort(IoType ioType, const EdgeDef &def);
    Port *doCreateRemotePort(IoType ioType, const EdgeDef &def);
    void initPort(Port *port, const BorderEdgeDef &borderEdgeDef);
    void initPortIndexMap();
    bool initBorderState(NaviPartId partCount);
    bool doCollectBorderMessage(NaviPartId partId,
                                NaviBorderData &borderMessage);
    bool collect(NaviPartId partId, NaviBorderData &borderMessage);
    ErrorCode collectPortData(Port *port, NaviPartId partId,
                              NaviBorderData &borderMessage);
    void fillBorderId(NaviBorderData &borderData);
    bool getBorderConnectInfo(const SubGraphBorder &toBorder,
                              BorderConnectInfo &connectInfo) const;
    bool initBorderState(const BorderConnectInfo &connectInfo,
                         SubGraphBorder &toBorder);
    bool doInitBorderState(const PartInfo &partInfo);
    void initQueueType(const BorderConnectInfo &connectInfo,
                       SubGraphBorder &toBorder);
    bool doLink(const BorderConnectInfo &connectInfo, SubGraphBorder &toBorder);
    bool isOnClient(const SubGraphBorder &toBorder) const;
    bool setSendEof(NaviPartId from, NaviPartId to, size_t edgeId);
    bool setReceiveEof(NaviPartId from, NaviPartId to, size_t edgeId);
public:
    void setBorderDomainIndex(size_t index);
    size_t getBorderDomainIndex() const;
    const PartInfo &getPartInfo() const;
    NaviPartId getPartId() const;
    bool isLocalBorder() const;
    bool eof() const;
    bool eof(NaviPartId partId) const;
    ErrorCode setEof(NaviPartId fromPartId);
public:
    friend std::ostream& operator<<(std::ostream& os, const SubGraphBorder& subBorder);
private:
    DECLARE_LOGGER();
    GraphParam *_param;
    NaviPartId _partId;
    BorderId _borderId;
    bool _localBorder;
    size_t _domainIndex;
    PartInfo _partInfo;
    GraphDomain *_domain;
    std::unordered_map<std::string, SamePortVec> _portNameMap;
    std::unordered_map<PortId, Port *> _portIdMap;
    std::vector<Port *> _portVec;
    BorderState _borderState;
    PortQueueType _queueType;
    bool _linked;
};

NAVI_TYPEDEF_PTR(SubGraphBorder);

typedef std::unordered_map<BorderId, SubGraphBorderPtr, BorderIdHash> SubGraphBorderMap;
typedef std::shared_ptr<SubGraphBorderMap> SubGraphBorderMapPtr;

}

#endif //NAVI_SUBGRAPHBORDER_H

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
#include "navi/engine/SubGraphBorder.h"
#include "navi/engine/GraphDomain.h"
#include "navi/engine/GraphDomainClient.h"
#include "navi/engine/LocalInputPort.h"
#include "navi/engine/LocalOutputPort.h"
#include "navi/engine/RemoteInputPort.h"
#include "navi/engine/RemoteOutputPort.h"
#include "navi/log/NaviLogger.h"
#include "navi/util/CommonUtil.h"

using namespace std;
namespace navi {

// TODO
static PartInfo createSinglePartInfo() {
    PartInfo partInfo;
    partInfo.init(1);
    return partInfo;
}


SubGraphBorder::SubGraphBorder(const NaviLoggerPtr &logger,
                               GraphParam *param,
                               NaviPartId partId,
                               const BorderId &borderId)
    : _logger(this, "SubBorder", logger)
    , _param(param)
    , _partId(partId)
    , _borderId(borderId)
    , _localBorder(true)
    , _domainIndex(0)
    , _domain(nullptr)
    , _queueType(PQT_UNKNOWN)
    , _linked(false)
{
}

SubGraphBorder::~SubGraphBorder() {
    for (const auto &port : _portVec) {
        delete port;
    }
}

bool SubGraphBorder::init(const PartInfoDef &partInfoDef,
                          const BorderDef &borderDef,
                          GraphDomain *domain)
{
    initBorderInfo(partInfoDef, borderDef.peer().graph_id(), domain);
    auto edgeCount = borderDef.edges_size();
    IoType ioType = (IoType)borderDef.io_type();
    for (int32_t edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
        const auto &borderEdgeDef = borderDef.edges(edgeIndex);
        auto port = createPort(ioType, borderEdgeDef);
        if (!port) {
            NAVI_LOG(ERROR, "get port failed, edge def: %s",
                     borderEdgeDef.DebugString().c_str());
            return false;
        }
        if (!addPort(port)) {
            delete port;
            return false;
        }
    }
    return true;
}

void SubGraphBorder::initBorderInfo(const PartInfoDef &partInfoDef, GraphId peer,
                                    GraphDomain *domain)
{
    NAVI_LOG(SCHEDULE1, "start init border info, partInfoDef [%s]",
             partInfoDef.ShortDebugString().c_str());
    _localBorder = domain->isLocalBorder(peer);
    initPartInfo(partInfoDef, domain);
    _domain = domain;
    NAVI_LOG(SCHEDULE1, "end init border info, partInfo [%s] localBorder [%d] "
             "GraphId [%d] GraphDomain [%s] ",
             _partInfo.ShortDebugString().c_str(), _localBorder, peer,
             autil::StringUtil::toString(*domain).c_str());
}

void SubGraphBorder::initPartInfo(PartInfoDef partInfoDef, GraphDomain *domain) {
    auto domainType = domain->getType();
    switch (domainType) {
    case GDT_USER:
    case GDT_LOCAL:
    case GDT_FORK:
        _partInfo.init(1);
        break;
    case GDT_SERVER:
        _partInfo.init(partInfoDef.part_count(), partInfoDef);
        break;
    case GDT_CLIENT:
    {
        auto clientDomain = dynamic_cast<GraphDomainClient *>(domain);
        assert(clientDomain);
        _partInfo = clientDomain->getPartInfo();
        break;
    }
    default:
        assert(false);
        break;
    }

    NAVI_LOG(SCHEDULE1, "init part info, partInfo [%s] domain [%s]",
             _partInfo.ShortDebugString().c_str(),
             autil::StringUtil::toString(*domain).c_str());
}

void SubGraphBorder::setBorderDomainIndex(size_t index) {
    _domainIndex = index;
}

size_t SubGraphBorder::getBorderDomainIndex() const {
    return _domainIndex;
}

const PartInfo &SubGraphBorder::getPartInfo() const {
    return _partInfo;
}

NaviPartId SubGraphBorder::getPartId() const {
    return _partId;
}

bool SubGraphBorder::isLocalBorder() const {
    return _localBorder;
}

bool SubGraphBorder::isOnClient(const SubGraphBorder &toBorder) const {
    auto domain = _domain;
    if (!toBorder.isLocalBorder()) {
        domain = toBorder._domain;
    }
    return GDT_CLIENT == domain->getType();
}

Port *SubGraphBorder::createPort(IoType ioType,
                                 const BorderEdgeDef &borderEdgeDef)
{
    Port *port = nullptr;
    const auto &edgeDef = borderEdgeDef.edge();
    if (isLocalBorder()) {
        port = doCreateLocalPort(ioType, edgeDef);
    } else {
        port = doCreateRemotePort(ioType, edgeDef);
    }
    initPort(port, borderEdgeDef);
    return port;
}

Port *SubGraphBorder::doCreateLocalPort(IoType ioType, const EdgeDef &def) {
    if (IOT_INPUT == ioType) {
        return new LocalInputPort(def, this);
    } else {
        return new LocalOutputPort(def, this);
    }
}

Port *SubGraphBorder::doCreateRemotePort(IoType ioType, const EdgeDef &def) {
    if (IOT_INPUT == ioType) {
        return new RemoteInputPort(def, this);
    } else {
        return new RemoteOutputPort(def, this);
    }
}

void SubGraphBorder::initPort(Port *port, const BorderEdgeDef &borderEdgeDef) {
    NAVI_LOG(SCHEDULE2, "init port [%p] type [%s] def [%s]",
             port, typeid(*port).name(),
             borderEdgeDef.ShortDebugString().c_str());
    port->setLogger(_logger);
    port->setPortInfo(borderEdgeDef.edge_id(), borderEdgeDef.peer_edge_id(), borderEdgeDef.node());
}

GraphParam *SubGraphBorder::getGraphParam() const {
    return _param;
}

bool SubGraphBorder::addPort(Port *port) {
    const auto &portKey = port->getPortKey();
    auto it = _portNameMap.find(portKey);
    if (_portNameMap.end() == it) {
        _portNameMap[portKey].ports.push_back(port);
    } else {
        // if (IOT_INPUT == port->getIoType()) {
        //     auto prev = it->second.ports[0];
        //     auto graphOutput = port->getPort().empty();
        //     NAVI_LOG(
        //         ERROR, "%s [%s] has multiple input [%s] and [%s]",
        //         graphOutput ? "graph output" : "port", portKey.c_str(),
        //         CommonUtil::getPortKey(port->getPeerNode(), port->getPeerPort())
        //             .c_str(),
        //         CommonUtil::getPortKey(prev->getPeerNode(), prev->getPeerPort())
        //             .c_str());
        //     return false;
        // }
        it->second.ports.push_back(port);
    }
    port->setPortBorderIndex(_portVec.size());
    _portVec.push_back(port);
    _portIdMap.emplace(port->getPortId(), port);
    return true;
}

Port *SubGraphBorder::getPort(const std::string &node,
                              const std::string &port)
{
    auto portKey = CommonUtil::getPortKey(node, port);
    auto it = _portNameMap.find(portKey);
    if (_portNameMap.end() == it) {
        return nullptr;
    } else {
        auto &portVec = it->second;
        if (portVec.linkIndex >= portVec.ports.size()) {
            return nullptr;
        }
        return portVec.ports[portVec.linkIndex++];
    }
}

Port *SubGraphBorder::getPort(PortId portId) const {
    auto it = _portIdMap.find(portId);
    if (_portIdMap.end() != it) {
        return it->second;
    } else {
        return nullptr;
    }
}

bool SubGraphBorder::linkTo(SubGraphBorder &toBorder) {
    if (_linked) {
        NAVI_LOG(ERROR, "border double link [%s]", getBorderIdStr().c_str());
        return false;
    }
    BorderConnectInfo connectInfo;
    if (!getBorderConnectInfo(toBorder, connectInfo)) {
        return false;
    }
    if (!initBorderState(connectInfo, toBorder)) {
        return false;
    }
    initQueueType(connectInfo, toBorder);
    if (!doLink(connectInfo, toBorder)) {
        return false;
    }
    _linked = true;
    toBorder._linked = true;
    return true;
}

bool SubGraphBorder::linked() const {
    return _linked;
}

bool SubGraphBorder::getBorderConnectInfo(const SubGraphBorder &toBorder,
                                          BorderConnectInfo &connectInfo) const
{
    auto fromPartInfo = getPartInfo();
    auto toPartInfo = toBorder.getPartInfo();
    connectInfo.fromPartInfo = fromPartInfo;
    connectInfo.toPartInfo = toPartInfo;
    connectInfo.eofPartInfo = createSinglePartInfo();
    connectInfo.storeType = PST_SERIALIZE_DATA;

    auto onClient = isOnClient(toBorder);
    auto fromLocal = isLocalBorder();
    auto toLocal = toBorder.isLocalBorder();
    auto sum = (fromLocal << 2) | (toLocal << 1) | (onClient << 0);
    switch (sum) {
    case 0b000:
        assert(false);
        return false;
    case 0b001:
        connectInfo.queueType = PQT_REMOTE_REMOTE_CLIENT_6;
        connectInfo.eofPartInfo = fromPartInfo;
        break;
    case 0b010:
        connectInfo.queueType = PQT_REMOTE_LOCAL_SERVER_4;
        connectInfo.toPartInfo = fromPartInfo;
        break;
    case 0b011:
        connectInfo.queueType = PQT_REMOTE_LOCAL_CLIENT_5;
        connectInfo.toPartInfo = fromPartInfo;
        break;
    case 0b100:
        connectInfo.queueType = PQT_LOCAL_REMOTE_SERVER_1;
        connectInfo.fromPartInfo = toPartInfo;
        break;
    case 0b101:
        connectInfo.queueType = PQT_LOCAL_REMOTE_CLIENT_2;
        connectInfo.fromPartInfo = toPartInfo;
        break;
    case 0b110:
        connectInfo.queueType = PQT_LOCAL_LOCAL_3;
        connectInfo.storeType = PST_DATA;
        break;
    case 0b111:
        connectInfo.queueType = PQT_LOCAL_LOCAL_3;
        connectInfo.storeType = PST_DATA;
        break;
    default:
        assert(false);
        return false;
    }

    NAVI_LOG(SCHEDULE1, "got border connect info [%s] with mode [%x]",
             autil::StringUtil::toString(connectInfo).c_str(), sum);
    return true;
}

bool SubGraphBorder::initBorderState(const BorderConnectInfo &connectInfo,
                                     SubGraphBorder &toBorder)
{

    if (!doInitBorderState(connectInfo.toPartInfo) ||
        !toBorder.doInitBorderState(connectInfo.toPartInfo))
    {
        NAVI_LOG(ERROR, "init border state failed, maybe double init");
        return false;
    }
    return true;
}

bool SubGraphBorder::doInitBorderState(const PartInfo &partInfo) {
    return _borderState.init(partInfo, _portVec.size());
}

void SubGraphBorder::initQueueType(const BorderConnectInfo &connectInfo,
                                   SubGraphBorder &toBorder)
{
    _queueType = connectInfo.queueType;
    toBorder._queueType = connectInfo.queueType;
}

bool SubGraphBorder::doLink(const BorderConnectInfo &connectInfo,
                            SubGraphBorder &toBorder)
{
    NAVI_LOG(SCHEDULE1, "do link from [%p] to [%p], connectInfo [%s]",
             this, &toBorder, autil::StringUtil::toString(connectInfo).c_str());
    for (auto fromPort : _portVec) {
        const auto &peerNode = fromPort->getPeerNode();
        const auto &peerPort = fromPort->getPeerPort();
        auto toPort = toBorder.getPort(peerNode, peerPort);
        if (!toPort) {
            NAVI_LOG(ERROR,
                     "get border port failed, fromBorder: %s, fromNode: "
                     "%s, fromPort: %s, toBorder: %s, toNode: %s, "
                     "toPort: %s",
                     getBorderIdStr().c_str(), fromPort->getNode().c_str(),
                     fromPort->getPort().c_str(),
                     toBorder.getBorderIdStr().c_str(), peerNode.c_str(),
                     peerPort.c_str());
            return false;
        }
        if (!fromPort->setDownStream(toPort, connectInfo)) {
            return false;
        }
    }
    return true;
}

const BorderId &SubGraphBorder::getBorderId() const {
    return _borderId;
}

std::string SubGraphBorder::getBorderIdStr() const {
    return autil::legacy::FastToJsonString(BorderIdJsonize(_borderId));
}

const std::vector<Port *> &SubGraphBorder::getPortVec() const {
    return _portVec;
}

void SubGraphBorder::incMessageCount(NaviPartId partId) {
    if (isLocalBorder()) {
        return;
    }
    _borderState.incMessageCount(partId);
    _domain->incMessageCount(partId);
}

void SubGraphBorder::decMessageCount(NaviPartId partId) {
    if (isLocalBorder()) {
        return;
    }
    _borderState.decMessageCount(partId);
    _domain->decMessageCount(partId);
}

bool SubGraphBorder::eof() const {
    return _borderState.eof();
}

bool SubGraphBorder::eof(NaviPartId partId) const {
    return _borderState.eof(partId);
}

void SubGraphBorder::notifySend(NaviPartId partId) {
    _domain->notifySend(partId);
}

bool SubGraphBorder::collectBorderMessage(NaviPartId partId,
                                          NaviBorderData &borderMessage)
{
    if (GDT_CLIENT == _domain->getType()) {
        if (!doCollectBorderMessage(partId, borderMessage)) {
            return false;
        }
    } else {
        for (auto partId : _partInfo) {
            if (!doCollectBorderMessage(partId, borderMessage)) {
                return false;
            }
        }
    }
    if (borderMessage.port_datas_size() > 0) {
        fillBorderId(borderMessage);
    }
    return true;
}

bool SubGraphBorder::doCollectBorderMessage(NaviPartId partId,
                                            NaviBorderData &borderMessage)
{
    assert(!isLocalBorder());
    assert(IOT_INPUT == _borderId.ioType);
    NAVI_LOG(SCHEDULE2, "remote domain trigger write, partId[%d]", partId);
    while (_borderState.messageCount(partId) > 0) {
        if (!collect(partId, borderMessage)) {
            return false;
        }
    }
    return true;
}

bool SubGraphBorder::collect(NaviPartId partId, NaviBorderData &borderMessage) {
    for (auto port : _portVec) {
        auto ec = collectPortData(port, partId, borderMessage);
        if (EC_NONE != ec) {
            return false;
        }
    }
    return true;
}

ErrorCode SubGraphBorder::collectPortData(Port *port, NaviPartId partId,
                                          NaviBorderData &borderMessage)
{
    assert(port);
    ErrorCode ec = EC_NONE;
    auto portId = port->getPortId();
    while (EC_NO_DATA != ec) {
        NaviPortData data;
        bool eof = false;
        ec = port->getData(partId, data, eof);
        if (EC_NONE != ec && EC_NO_DATA != ec) {
            NAVI_LOG(ERROR, "get data error, portId: %d, portKey: %s, port "
                            "partInfo: [%s], partId: %d, ec: %s",
                     port->getPortId(), port->getPortKey().c_str(),
                     port->getPartInfo().ShortDebugString().c_str(), partId,
                     CommonUtil::getErrorString(ec));
            return ec;
        }
        if (EC_NO_DATA == ec) {
            break;
        }
        if (eof) {
            if (!setSendEof(data.from_part_id(), data.to_part_id(),
                            port->getPortBorderIndex()))
            {
                return EC_UNKNOWN;
            }
        }
        auto dataPb = borderMessage.add_port_datas();
        dataPb->Swap(&data);
        dataPb->set_port_id(portId);
    }
    return EC_NONE;
}

void SubGraphBorder::fillBorderId(NaviBorderData &borderData) {
    auto borderIdDef = borderData.mutable_border_id();
    borderIdDef->set_graph_id(_borderId.peer);
    borderIdDef->set_peer(_borderId.id);
    borderIdDef->set_io_type(CommonUtil::getReverseIoType(_borderId.ioType));
}

bool SubGraphBorder::receive(NaviBorderData &borderData) {
    auto dataCount = borderData.port_datas_size();
    for (int32_t index = 0; index < dataCount; index++) {
        auto &portData = *borderData.mutable_port_datas(index);
        auto portId = portData.port_id();
        auto port = getPort(portId);
        if (!port) {
            NAVI_LOG(ERROR, "port not found, portId: %d", portId);
            return false;
        }
        bool retEof = true;
        auto from = portData.from_part_id();
        auto to = portData.to_part_id();
        auto ec = port->setData(portData, retEof);
        // todo report error
        if (EC_NONE != ec && EC_BUFFER_FULL != ec && EC_DATA_AFTER_EOF != ec) {
            NAVI_LOG(ERROR,
                     "set data failed, portId[%d] portKey[%s], ec[%s]",
                     portId,
                     port->getPortKey().c_str(),
                     CommonUtil::getErrorString(ec));
            return false;
        }
        if (retEof) {
            if (!setReceiveEof(from, to, port->getPortBorderIndex())) {
                return false;
            }
        }
    }
    return true;
}

bool SubGraphBorder::setSendEof(NaviPartId from, NaviPartId to, size_t edgeId) {
    if (isLocalBorder()) {
        assert(false);
        return false;
    }
    switch (_queueType) {
    case PQT_LOCAL_REMOTE_SERVER_1:
    case PQT_LOCAL_REMOTE_CLIENT_2:
    case PQT_REMOTE_REMOTE_CLIENT_6:
    {
        auto partId = to;
        _borderState.setEof(partId, edgeId);
        NAVI_LOG(SCHEDULE2, "borderId: %s, port eof, partId: %d, edgeIndex: %ld",
                 getBorderIdStr().c_str(), partId, edgeId);
        return true;
    }
    default:
        return true;
    }
}

bool SubGraphBorder::setReceiveEof(NaviPartId from, NaviPartId to,
                                   size_t edgeId)
{
    if (isLocalBorder()) {
        assert(false);
        return false;
    }
    switch (_queueType) {
    case PQT_REMOTE_LOCAL_CLIENT_5:
    {
        auto partId = from;
        _borderState.setEof(partId, edgeId);
        NAVI_LOG(SCHEDULE2,
                 "borderId: %s, port receive eof, partId: %d, edgeIndex: %ld",
                 getBorderIdStr().c_str(), partId, edgeId);
        return true;
    }
    case PQT_REMOTE_REMOTE_CLIENT_6:
    {
        auto partId = to;
        _borderState.setEof(partId, edgeId);
        NAVI_LOG(SCHEDULE2,
                 "borderId: %s, port receive eof, partId: %d, edgeIndex: %ld",
                 getBorderIdStr().c_str(), partId, edgeId);
        return true;
    }
    default:
        return true;
    }
}

ErrorCode SubGraphBorder::setEof(NaviPartId fromPartId) {
    for (auto port : _portVec) {
        auto ec = port->setEof(fromPartId);
        if (EC_NONE != ec && EC_DATA_AFTER_EOF != ec) {
            return ec;
        }
    }
    return EC_NONE;
}

std::ostream& operator<<(std::ostream& os, const SubGraphBorder& subBorder)
{
    os << std::endl;
    os << "(" << &subBorder << ","
       << "borderId[" << subBorder._borderId << "],"
       << "isLocal[" << subBorder._localBorder << "],"
       << "partId[" << subBorder._partId << "],"
       << "partInfo[" << subBorder._partInfo.ShortDebugString() << "],"
       << "domain[";
    autil::StringUtil::toStream(os, *subBorder._domain);
    os << "],"
       << "port[";
    for (const auto *port : subBorder._portVec) {
        os << *port;
    }
    os << "])";
    return os;
}
}

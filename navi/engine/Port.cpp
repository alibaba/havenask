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
#include "navi/engine/Port.h"
#include "navi/engine/Node.h"
#include "navi/engine/SubGraphBorder.h"
#include "navi/log/NaviLogger.h"
#include "navi/util/CommonUtil.h"
#include "autil/StringUtil.h"

using namespace std;
namespace navi {

Port::Port(const std::string &node,
           const std::string &port,
           const std::string &peerNode,
           const std::string &peerPort,
           IoType ioType,
           PortStoreType storeType,
           SubGraphBorder *border)
    : _node(node)
    , _port(port)
    , _peerNode(peerNode)
    , _peerPort(peerPort)
    , _portKey(CommonUtil::getPortKey(node, port))
    , _ioType(ioType)
    , _upStream(nullptr)
    , _downStream(nullptr)
    , _dataType(nullptr)
    , _graphMemoryPoolResource(nullptr)
    , _storeType(storeType)
    , _portId(INVALID_PORT_ID)
    , _peerPortId(INVALID_PORT_ID)
    , _borderIndex(0)
    , _dataQueues(nullptr)
    , _border(border)
    , _param(border->getGraphParam())
{
    setPartInfo(border->getPartInfo());
}

Port::Port(const NodePortDef &nodePort,
           const NodePortDef &peerPort,
           IoType ioType,
           PortStoreType storeType,
           SubGraphBorder *border)
    : Port(nodePort.node_name(), nodePort.port_name(),
           peerPort.node_name(), peerPort.port_name(),
           ioType, storeType, border)
{
}

Port::~Port() {
    if (_downStream) {
        DELETE_AND_SET_NULL(_dataQueues);
    }
}

const string &Port::getNode() const {
    return _node;
}

const string &Port::getPort() const {
    return _port;
}

const string &Port::getPortKey() const {
    return _portKey;
}

const std::string &Port::getPeerNode() const {
    return _peerNode;
}

const std::string &Port::getPeerPort() const {
    return _peerPort;
}

IoType Port::getIoType() const {
    return _ioType;
}

bool Port::isLocalPort() const {
    return PST_DATA == _storeType;
}

void Port::setPortInfo(PortId portId, PortId peerPortId, const std::string &portNodeName) {
    _portId = portId;
    _peerPortId = peerPortId;
    _portNodeName = portNodeName;
}

PortId Port::getPortId() const {
    return _portId;
}

const std::string &Port::getPortNodeName() const {
    return _portNodeName;
}

bool Port::hasUpStream() const {
    return _upStream != nullptr;
}

bool Port::hasDownStream() const {
    return _downStream != nullptr;
}

bool Port::setDownStream(Port *port, const BorderConnectInfo &connectInfo) {
    if (hasDownStream()) {
        NAVI_LOG(ERROR,
                 "double connect, fromNode: %s, fromPort: %s, old toNode: %s, "
                 "old toPort: %s, new toNode: %s, new toPort: %s",
                 getNode().c_str(), getPort().c_str(),
                 _downStream->getNode().c_str(), _downStream->getPort().c_str(),
                 port->getNode().c_str(), port->getPort().c_str());
        return false;
    }
    if (port->hasUpStream()) {
        NAVI_LOG(ERROR,
                 "double connect, toNode: %s, toPort: %s, old fromNode: %s, "
                 "old fromPort: %s, new fromNode: %s, new fromPort: %s",
                 port->getNode().c_str(), port->getPort().c_str(),
                 port->_upStream->getNode().c_str(),
                 port->_upStream->getPort().c_str(), getNode().c_str(),
                 getPort().c_str());
        return false;
    }
    return link(port, connectInfo);
}

bool Port::link(Port *downStream, const BorderConnectInfo &connectInfo) {
    NAVI_LOG(SCHEDULE2, "port link to [%p,%s] connectInfo [%s]",
             downStream, downStream->getPortKey().c_str(),
             autil::StringUtil::toString(connectInfo).c_str());
    _downStream = downStream;
    setPartInfo(connectInfo.fromPartInfo);
    downStream->_upStream = this;
    downStream->setPartInfo(connectInfo.toPartInfo);

    if (PQT_REMOTE_REMOTE_CLIENT_6 == connectInfo.queueType) {
        _dataQueues = new ShufflePortQueue();
    } else {
        _dataQueues = new NormalPortQueue();
    }
    if (!_dataQueues->init(_logger.logger, _param->pool.get(),
                           connectInfo.toPartInfo,
                           connectInfo.eofPartInfo,
                           this, downStream, connectInfo.storeType,
                           connectInfo.queueType))
    {
        DELETE_AND_SET_NULL(_dataQueues);
        return false;
    }
    _downStream->_dataQueues = _dataQueues;
    return true;
}

void Port::resetLink() {
    if (_downStream) {
        auto downStream = _downStream;
        _downStream = nullptr;
        downStream->resetLink();
    }
    if (_upStream) {
        auto upStream = _upStream;
        _upStream = nullptr;
        upStream->resetLink();
    }
}

const PartInfo &Port::getPartInfo() const {
    return _partInfo;
}

void Port::setPartInfo(const PartInfo &partInfo) {
    NAVI_LOG(SCHEDULE2, "port set part info [%s]", partInfo.ShortDebugString().c_str());
    _partInfo = partInfo;
}

SubGraphBorder *Port::getPeerBorder() const {
    if (_upStream) {
        return _upStream->_border;
    } else {
        return _downStream->_border;
    }
}

void Port::setPortBorderIndex(size_t index) {
    _borderIndex = index;
}

size_t Port::getPortBorderIndex() const {
    return _borderIndex;
}

SubGraphBorder *Port::getBorder() const {
    return _border;
}

void Port::setLogger(const NaviObjectLogger &logger) {
    _logger.object = this;
    _logger.logger = logger.logger;
    _logger.addPrefix("%s", _portKey.c_str());
}

ostream& operator<<(std::ostream& os, const Port& port) {
    std::ostringstream queueStream;
    if (port._dataQueues) {
        queueStream << *port._dataQueues;
    }
    return os << "(" << &port << ","
              << "key[" << port.getPortKey() << ","
              << typeid(port).name() << "],"
              << "partInfo[" << port.getPartInfo().ShortDebugString() << "],"
              << "isLocal[" << port.isLocalPort() << "],"
              << "queue[" << queueStream.str()  << "],"
              << "up[" << port._upStream << "],"
              << "down[" << port._downStream << "]"
              <<")";
}

void Port::bindDataType(const Type *dataType) {
    if (!_dataType) {
        _dataType = dataType;
    }
    if (_upStream) {
        _upStream->bindDataType(dataType);
    }
}

const Type *Port::getDataType() const {
    return _dataType;
}

void Port::setPoolResource(GraphMemoryPoolResource *resource) {
    _graphMemoryPoolResource = resource;
}

std::shared_ptr<autil::mem_pool::Pool> Port::getPool() {
    assert(_graphMemoryPoolResource);
    if (!_poolCache) {
        _poolCache = _graphMemoryPoolResource->getPool();
    }
    return _poolCache;
}

void Port::releasePool() {
    _poolCache.reset();
}

autil::mem_pool::Pool *Port::getQueryPool() const {
    return _param->pool.get();
}


ErrorCode Port::setData(std::vector<DataPtr> &dataVec, bool eof) {
    NAVI_LOG(SCHEDULE2, "set vec data, size [%lu] eof [%d]",
             dataVec.size(), eof);
    assert(dataVec.size() == _partInfo.getUsedPartCount());

    ErrorCode ec = EC_NONE;
    NaviPartId dataSize = dataVec.size();
    auto fromPartId = _border->getPartId();
    for (NaviPartId index = 0; index < dataSize; index++) {
        auto &data = dataVec[index];
        NaviPartId partId = _partInfo.getUsedPartId(index);
        if (data == nullptr && eof == false) {
            continue;
        }

        if (PST_DATA == _dataQueues->getStoreType()) {
            bool retEof;
            ec = setDataToData(partId, fromPartId, data, eof, retEof);
        } else {
            ec = setDataToSerialize(partId, fromPartId, data, eof);
        }
        if (EC_NONE != ec) {
            return ec;
        }
        data.reset();
    }
    return ec;
}

ErrorCode Port::setData(NaviPortData &data, bool &eof) {
    NAVI_LOG(SCHEDULE2, "set port data, id [%d] from [%d] to [%d] eof [%d] "
             "type [%s] dataLen [%lu]",
             data.port_id(), data.from_part_id(), data.to_part_id(), data.eof(),
             data.type().c_str(), data.data().length());

    if (PST_DATA == _dataQueues->getStoreType()) {
        return setSerializeToData(data, eof);
    } else {
        return setSerializeToSerialize(data.to_part_id(), data.from_part_id(),
                                       data, eof);
    }
}

ErrorCode Port::setEof(NaviPartId fromPartId) {
    return _dataQueues->setEof(fromPartId);
}

ErrorCode Port::setDataToData(NaviPartId toPartId, NaviPartId fromPartId,
                              const DataPtr &data, bool eof, bool &retEof)
{
    NAVI_LOG(SCHEDULE2, "set data, from [%d] to [%d] data [%p] eof [%d]",
             fromPartId, toPartId, data.get(), eof);
    return _dataQueues->push(toPartId, fromPartId, data, eof, retEof);
}

ErrorCode Port::setSerializeToSerialize(NaviPartId toPartId,
                                        NaviPartId fromPartId,
                                        NaviPortData &data,
                                        bool &retEof)
{
    return _dataQueues->pushSerialize(toPartId, fromPartId, &data, data.eof(),
                                      retEof);
}

ErrorCode Port::setDataToSerialize(NaviPartId toPartId, NaviPartId fromPartId,
                                   const DataPtr &data, bool eof)
{
    NAVI_LOG(SCHEDULE2, "set data, from [%d] to [%d] data [%p] eof [%d]",
             fromPartId, toPartId, data.get(), eof);

    if (!_dataType && !initDataType(data)) {
        NAVI_LOG(ERROR, "null data type");
        return EC_SERIALIZE;
    }
    NaviPortData serializeData;
    if (!SerializeData::serialize(_logger, toPartId, fromPartId, data, eof,
                                  _dataType, serializeData))
    {
        NAVI_LOG(ERROR, "serialize failed");
        return EC_SERIALIZE;
    }
    bool retEof;
    return setSerializeToSerialize(toPartId, fromPartId, serializeData, retEof);
}

ErrorCode Port::setSerializeToData(const NaviPortData &data, bool &retEof) {
    DataPtr dataPtr;
    if (!_dataType && !initDataType(data)) {
        return EC_DESERIALIZE;
    }
    if (!checkPoolUsage()) {
        NAVI_LOG(ERROR, "pool usage exceed");
        return EC_DESERIALIZE;
    }
    if (!SerializeData::deserialize(_logger, data, _dataType,
                                    getQueryPool(), getPool(), dataPtr))
    {
        NAVI_LOG(ERROR, "deserialize failed");
        return EC_DESERIALIZE;
    }
    auto fromPartId = data.from_part_id();
    auto toPartId = fromPartId;
    return setDataToData(toPartId, fromPartId, dataPtr, data.eof(), retEof);
}

ErrorCode Port::getData(NaviPartId toPartId, DataPtr &data, bool &eof) {
    if (PST_DATA == _dataQueues->getStoreType()) {
        return getDataToData(toPartId, data, eof);
    } else {
        return getSerializeToData(toPartId, data, eof);
    }
}

ErrorCode Port::getData(NaviPartId toPartId, NaviPortData &data, bool &eof) {
    if (PST_DATA == _dataQueues->getStoreType()) {
        return getDataToSerialize(toPartId, data, eof);
    } else {
        return getSerializeToSerialize(toPartId, data, eof);
    }
}

ErrorCode Port::getDataToData(NaviPartId toPartId, DataPtr &data, bool &eof) {
    return _dataQueues->pop(toPartId, data, eof);
}

ErrorCode Port::getSerializeToData(NaviPartId toPartId, DataPtr &data,
                                   bool &eof)
{
    NaviPortData serializeData;
    auto ec = getSerializeToSerialize(toPartId, serializeData, eof);
    if (EC_NONE != ec) {
        return ec;
    }
    if (!_dataType && !initDataType(serializeData)) {
        return EC_DESERIALIZE;
    }
    if (!checkPoolUsage()) {
        NAVI_LOG(ERROR, "pool usage exceed");
        return EC_DESERIALIZE;
    }
    if (!SerializeData::deserialize(_logger, serializeData, _dataType,
                                    getQueryPool(), getPool(), data))
    {
        NAVI_LOG(ERROR, "deserialize failed");
        return EC_DESERIALIZE;
    }
    return EC_NONE;
}

ErrorCode Port::getSerializeToSerialize(NaviPartId toPartId, NaviPortData &data,
                                        bool &eof)
{
    return _dataQueues->popSerialize(toPartId, &data, eof);
}

ErrorCode Port::getDataToSerialize(NaviPartId toPartId,
                                   NaviPortData &serializeData, bool &eof)
{
    DataPtr data;
    auto ec = getDataToData(toPartId, data, eof);
    if (EC_NONE != ec) {
        return ec;
    }
    if (!_dataType && !initDataType(data)) {
        NAVI_LOG(ERROR, "null data type");
        return EC_SERIALIZE;
    }
    assert(_upStream);
    auto fromPartId = _upStream->_border->getPartId();
    if (!SerializeData::serialize(_logger, toPartId, fromPartId, data, eof,
                                  _dataType, serializeData))
    {
        NAVI_LOG(ERROR, "serialize failed");
        return EC_SERIALIZE;
    }
    return EC_NONE;
}

int64_t Port::getQueueSize(NaviPartId toPartId) {
    return _dataQueues->size(toPartId);
}

void Port::incBorderMessageCount(NaviPartId partId) {
    _border->incMessageCount(partId);
}

void Port::decBorderMessageCount(NaviPartId partId) {
    _border->decMessageCount(partId);
}

bool Port::initDataType(const DataPtr &data) {
    if (!data) {
        return true;
    }
    const auto &typeStr = data->getTypeId();
    return initDataTypeByTypeStr(typeStr);
}

bool Port::initDataType(const NaviPortData &data) {
    if (data.data().empty()) {
        return true;
    }
    return initDataTypeByTypeStr(data.type());
}

bool Port::initDataTypeByTypeStr(const std::string &typeStr) {
    if (typeStr.empty()) {
        NAVI_LOG(ERROR, "lack data type");
        return false;
    }
    auto type = _param->creatorManager->getType(typeStr);
    if (!type) {
        NAVI_LOG(ERROR, "data type [%s] not supported", typeStr.c_str());
        return false;
    }
    _dataType = type;
    return true;
}

bool Port::checkPoolUsage() {
    size_t limit = _param->worker->getRunParams().getSinglePoolUsageLimit();
    if (limit > 0) {
        size_t poolSize = getPool()->getAllocatedSize();
        if (poolSize > limit) {
            NAVI_LOG(ERROR, "pool size exceed, limit [%lu]B, current [%lu]B",
                     limit, poolSize);
            return false;
        }
        size_t queryPoolSize = getQueryPool()->getAllocatedSize();
        if (queryPoolSize > limit) {
            NAVI_LOG(ERROR, "query pool size exceed, limit [%lu]B, current [%lu]B",
                     limit, queryPoolSize);
            return false;
        }
    }
    return true;
}

}

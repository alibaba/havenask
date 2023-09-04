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
#include "build_service_tasks/batch_control/BatchReporter.h"

#include <unistd.h>

#include "aios/network/anet/httppacket.h"
#include "aios/network/anet/httpstreamer.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/md5.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace anet;

using namespace build_service::proto;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, BatchReporter);
BS_LOG_SETUP(task_base, BatchReporter::PacketHandler);

BatchReporter::RequestBody::RequestBody(const BuildId& buildId) : _buildId(buildId) {}

void BatchReporter::BuildIdWrapper::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        string appName = _buildId.appname();
        json.Jsonize("appName", appName);
        string dataTable = _buildId.datatable();
        json.Jsonize("dataTable", dataTable);
        uint32_t generationId = _buildId.generationid();
        json.Jsonize("generationId", generationId);
    } else {
        JsonMap jsonMap = json.GetMap();
        auto iter = jsonMap.find("appName");
        if (iter != jsonMap.end()) {
            _buildId.set_appname(AnyCast<string>(iter->second));
        }
        iter = jsonMap.find("dataTable");
        if (iter != jsonMap.end()) {
            _buildId.set_datatable(AnyCast<string>(iter->second));
        }
        iter = jsonMap.find("generationId");
        if (iter != jsonMap.end()) {
            _buildId.set_generationid(JsonNumberCast<uint32_t>(iter->second));
        }
    }
}

void BatchReporter::RequestBody::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("graphFileName", _graphFileName, _graphFileName);
    json.Jsonize("graphIdentifier", _graphId, _graphId);
    json.Jsonize("buildId", _buildId, _buildId);
    if (json.GetMode() == TO_JSON) {
        string paramStr = ToJsonString(_params);
        json.Jsonize("jsonParameterString", paramStr);
    } else {
        JsonMap jsonMap = json.GetMap();
        auto iter = jsonMap.find("jsonParameterString");
        string jsonParam = AnyCast<string>(iter->second);
        FromJsonString(_params, jsonParam);
    }
}

BatchReporter::BatchReporter() : _connectTimeout(1000) {}

BatchReporter::~BatchReporter() { close(); }

void BatchReporter::close()
{
    _transport.stop();
    _transport.wait();
}

bool BatchReporter::init(const proto::BuildId& buildId, const vector<string>& clusterNames)
{
    _buildId = buildId;
    if (clusterNames.empty()) {
        BS_LOG(ERROR, "lack of clusterNames");
        return false;
    }
    _clusterNames = ToJsonString(clusterNames);
    map<string, string> schemaIdMap;
    for (auto cluster : clusterNames) {
        schemaIdMap.insert(make_pair(cluster, "0"));
    }
    _schemaIdMap = ToJsonString(schemaIdMap);
    _transport.start();
    return true;
}

bool BatchReporter::resetHost(const std::string& host, int32_t port)
{
    ScopedLock lock(_lock);
    if (host.empty()) {
        BS_LOG(ERROR, "lack of admin address");
        return false;
    }
    _host = host;
    _endpoint = "tcp:" + host + ":" + StringUtil::toString(port);
    BS_LOG(INFO, "reset address, new address is [%s]", _endpoint.c_str());
    return true;
}

void BatchReporter::setUseV2Graph(bool useV2)
{
    ScopedLock lock(_lock);
    _useV2Graph = useV2;
}

void BatchReporter::prepareParams(const BatchControlWorker::BatchInfo& batch, int64_t globalId, int64_t lastGlobalId,
                                  const vector<int64_t>& skipGlobalIds, const vector<int64_t>& combineIds,
                                  BatchReporter::RequestBody& body)
{
    body._params["batchMask"] = StringUtil::toString(batch.batchId);
    body._params["clusterNames"] = _clusterNames;
    body._params["dataDescriptions"] = _dataDesc;
    body._params["startDataDescriptionId"] = "0";
    if (batch.operation == BatchControlWorker::BatchOp::begin) {
        body._graphId = StringUtil::toString(globalId) + "-begin";
        body._graphFileName = _useV2Graph ? "BatchBuildV2/AddBatch.graph" : "BatchBuild/AddBatch.graph";
        body._params["beginTime"] = StringUtil::toString(batch.beginTime);
        body._params["schemaIdMap"] = _schemaIdMap;
    } else if (batch.operation == BatchControlWorker::BatchOp::end) {
        body._graphId = StringUtil::toString(globalId) + "-end";
        body._graphFileName = _useV2Graph ? "BatchBuildV2/EndBatch.graph" : "BatchBuild/EndBatch.graph";
        body._params["endTime"] = StringUtil::toString(batch.endTime);
    }
    body._params["batchId"] = StringUtil::toString(globalId);
    if (lastGlobalId != -1) {
        body._params["lastBatchId"] = StringUtil::toString(lastGlobalId);
    }

    auto convertIdsToStr = [](const vector<int64_t>& ids) -> string {
        vector<string> strs;
        strs.reserve(ids.size());
        for (auto id : ids) {
            strs.push_back(StringUtil::toString(id));
        }
        return ToJsonString(strs);
    };

    if (!skipGlobalIds.empty()) {
        // "skipMergeBatchIds" is legacy for old admin, "skipBatchIds" is better
        body._params["skipMergeBatchIds"] = convertIdsToStr(skipGlobalIds);
    }
    if (!combineIds.empty()) {
        body._params["combinedBatchIds"] = convertIdsToStr(combineIds);
    }
}

bool BatchReporter::report(const BatchControlWorker::BatchInfo& batch, int64_t globalId, int64_t lastGlobalId)
{
    return innerReport(batch, globalId, lastGlobalId, /*skipIds*/ {}, /*combinedIds*/ {});
}

bool BatchReporter::report(const BatchControlWorker::BatchInfo& batch, int64_t globalId, int64_t lastGlobalId,
                           int64_t skipStepCount, int64_t& commitVersionGlobalId)
{
    vector<int64_t> skipIds;
    vector<int64_t> combinedIds;
    int64_t newCommitVersionGlobalId = commitVersionGlobalId;
    prepareSkipMergeBatchIds(globalId, commitVersionGlobalId, skipStepCount, &newCommitVersionGlobalId, &skipIds,
                             &combinedIds);
    if (!innerReport(batch, globalId, lastGlobalId, skipIds, combinedIds)) {
        return false;
    }
    commitVersionGlobalId = newCommitVersionGlobalId;
    return true;
}

void BatchReporter::prepareSkipMergeBatchIds(int64_t globalId, int64_t commitVersionGlobalId, int64_t skipStepCount,
                                             int64_t* newCommitVersionGlobalId, vector<int64_t>* skipIds,
                                             vector<int64_t>* combinedIds)
{
    assert(skipStepCount > 1);
    *newCommitVersionGlobalId = commitVersionGlobalId;
    if (globalId <= skipStepCount) {
        return;
    }

    if (commitVersionGlobalId == -1) {
        // no valid marked commit version
        *newCommitVersionGlobalId = globalId - skipStepCount;
        int64_t num = 0;
        for (int64_t id = *newCommitVersionGlobalId; id > 0; id--, num++) {
            if ((num % skipStepCount) == 0) {
                combinedIds->push_back(id);
            } else {
                skipIds->push_back(id);
            }
        }
        sort(skipIds->begin(), skipIds->end());
        sort(combinedIds->begin(), combinedIds->end());
        return;
    }

    int64_t nextCommitVersion = commitVersionGlobalId + skipStepCount;
    int64_t boundaryBatchId = globalId - skipStepCount;
    if (nextCommitVersion <= boundaryBatchId) {
        *newCommitVersionGlobalId = nextCommitVersion;
        for (int64_t i = 1; i <= skipStepCount - 1; i++) {
            skipIds->push_back(commitVersionGlobalId + i);
        }
        combinedIds->push_back(commitVersionGlobalId + skipStepCount);
    }
}

bool BatchReporter::innerReport(const BatchControlWorker::BatchInfo& batch, int64_t globalId, int64_t lastGlobalId,
                                const vector<int64_t>& skipGlobalIds, const vector<int64_t>& combinedIds)
{
    ScopedLock lock(_lock);
    if (_host.empty()) {
        BS_LOG(ERROR, "lack of host");
        return false;
    }
    RequestBody body(_buildId);
    prepareParams(batch, globalId, lastGlobalId, skipGlobalIds, combinedIds, body);
    string bodyStr = ToJsonString(body);
    HTTPPacket* packet = new HTTPPacket();
    packet->addHeader("Content-Type", "application/json");
    packet->addHeader("Accept", "*/*");
    packet->addHeader("Host", _host.c_str());
    packet->setURI("/AdminService/callGraph");
    packet->setBody(bodyStr.c_str(), bodyStr.size());
    packet->setPacketType(HTTPPacket::PT_REQUEST);
    packet->setMethod(HTTPPacket::HM_POST);
    BS_LOG(DEBUG, "callGraph: %s", bodyStr.c_str());

    HTTPStreamer streamer(&_packFactory);
    Connection* conn = _transport.connect(_endpoint.c_str(), &streamer, false);
    if (conn == NULL) {
        BS_LOG(ERROR, "call graph failed, connection error, event[%s].", bodyStr.c_str());
        return false;
    }

    PacketHandler handler(batch);
    if (!conn->postPacket(packet, &handler, NULL)) {
        BS_LOG(ERROR, "call graph failed, event[%s].", bodyStr.c_str());
        return false;
    }
    size_t time = 0;
    while (!handler.isDone() && time < _connectTimeout) {
        usleep(50000); // 50ms
        time += 50;
    }
    if (conn) {
        conn->close();
        conn->subRef();
    }
    if (handler.hasError()) {
        return false;
    }
    if (time >= _connectTimeout) {
        BS_LOG(ERROR, "call graph timeout, address is [%s]", _endpoint.c_str());
        return false;
    }
    return true;
}

BatchReporter::PacketHandler::PacketHandler(const BatchControlWorker::BatchInfo& batch)
    : _batch(batch)
    , _hasError(false)
    , _isDone(false)
{
}

BatchReporter::PacketHandler::~PacketHandler() {}

IPacketHandler::HPRetCode BatchReporter::PacketHandler::handlePacket(anet::Packet* packet, void* args)
{
    if (!packet->isRegularPacket()) {
        packet->free();
        return IPacketHandler::FREE_CHANNEL;
    }
    HTTPPacket* httpPacket = dynamic_cast<HTTPPacket*>(packet);
    if (httpPacket == NULL) {
        packet->free();
        return IPacketHandler::FREE_CHANNEL;
    }

    if (httpPacket->getStatusCode() != 200) {
        string body = string(httpPacket->getBody(), httpPacket->getBodyLen());
        if (!body.empty()) {
            BS_LOG(ERROR, "call graph fail, batch[%s], errorCode[%d] response[%s]", _batch.toString().c_str(),
                   httpPacket->getStatusCode(), body.c_str());
        }
        _isDone = true;
        _hasError = true;
        packet->free();
        return IPacketHandler::FREE_CHANNEL;
    }

    string response(httpPacket->getBody(), httpPacket->getBodyLen());
    BS_LOG(DEBUG, "admin response: %s", response.c_str());
    if (response.empty()) {
        packet->free();
        _isDone = true;
        return IPacketHandler::FREE_CHANNEL;
    }
    try {
        JsonMap jsonMap;
        FromJsonString(jsonMap, response);
        auto iter = jsonMap.find("errorMessage");
        if (iter != jsonMap.end()) {
            BS_LOG(ERROR, "call graph failed, batch[%s], response[%s]", _batch.toString().c_str(), response.c_str());
            _hasError = true;
        }
    } catch (const autil::legacy::ExceptionBase& ex) {
        BS_LOG(ERROR, "handle response[%s] failed, batch[%s], exception[%s]", response.c_str(),
               _batch.toString().c_str(), ex.GetMessage().c_str());
        _hasError = true;
    } catch (...) {
        BS_LOG(ERROR, "call graph failed, batch[%s], unknownError[%s]", _batch.toString().c_str(), response.c_str());
        _hasError = true;
    }

    packet->free();
    _isDone = true;
    return IPacketHandler::FREE_CHANNEL;
}

bool BatchReporter::PacketHandler::isDone() const { return _isDone; }

bool BatchReporter::PacketHandler::hasError() const { return _hasError; }

void BatchReporter::setDataDescription(const proto::DataDescription& dataDesc)
{
    vector<DataDescription> ds;
    ds.push_back(dataDesc);
    _dataDesc = ToJsonString(ds);
}

}} // namespace build_service::task_base

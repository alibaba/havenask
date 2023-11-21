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
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "aios/network/anet/httppacketfactory.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/transport.h"
#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/batch_control/BatchControlWorker.h"

namespace build_service { namespace task_base {

class BatchReporter
{
public:
    BatchReporter();
    virtual ~BatchReporter();

private:
    BatchReporter(const BatchReporter&);
    BatchReporter& operator=(const BatchReporter&);

public:
    class BuildIdWrapper : public autil::legacy::Jsonizable
    {
    public:
        BuildIdWrapper(const proto::BuildId& buildId) : _buildId(buildId) {}
        BuildIdWrapper() {}

    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    private:
        proto::BuildId _buildId;
    };

    class RequestBody : public autil::legacy::Jsonizable
    {
    public:
        RequestBody(const proto::BuildId& buildId);
        ~RequestBody() {}

    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    private:
        BatchReporter::BuildIdWrapper _buildId;
        std::string _graphFileName;
        std::string _graphId;
        KeyValueMap _params;

    private:
        friend class BatchReporter;
    };
    class PacketHandler : public anet::IPacketHandler
    {
    public:
        PacketHandler(const BatchControlWorker::BatchInfo& batch);
        ~PacketHandler();

    public:
        anet::IPacketHandler::HPRetCode handlePacket(anet::Packet* packet, void* args) override;
        bool isDone() const;
        bool hasError() const;

    private:
        BatchControlWorker::BatchInfo _batch;
        bool _hasError;
        bool _isDone;

    private:
        BS_LOG_DECLARE();
    };

public:
    bool init(const proto::BuildId& buildId, const std::vector<std::string>& clusterNames);
    bool resetHost(const std::string& host, int32_t port);
    void setUseV2Graph(bool useV2);
    // virtual for test
    virtual bool report(const BatchControlWorker::BatchInfo& batch, int64_t globalId, int64_t lastGlobalId);

    virtual bool report(const BatchControlWorker::BatchInfo& batch, int64_t globalId, int64_t lastGlobalId,
                        int64_t skipStepCount, int64_t& commitVersionGlobalId);

    void setDataDescription(const proto::DataDescription& dataDesc);

private:
    void prepareParams(const BatchControlWorker::BatchInfo& batch, int64_t globalId, int64_t lastGlobalId,
                       const std::vector<int64_t>& skipGlobalIds, const std::vector<int64_t>& combinedIds,
                       BatchReporter::RequestBody& body);

    bool innerReport(const BatchControlWorker::BatchInfo& batch, int64_t globalId, int64_t lastGlobalId,
                     const std::vector<int64_t>& skipGlobalIds, const std::vector<int64_t>& combinedIds);

    static void prepareSkipMergeBatchIds(int64_t globalId, int64_t commitVersionGlobalId, int64_t skipStepCount,
                                         int64_t* newCommitVersionGlobalId, std::vector<int64_t>* skipMergeIds,
                                         std::vector<int64_t>* combinedIds);

private:
    void close();

private:
    mutable autil::RecursiveThreadMutex _lock;
    std::string _endpoint;
    std::string _host;
    size_t _connectTimeout;
    anet::Transport _transport;
    anet::HTTPPacketFactory _packFactory;
    proto::BuildId _buildId;
    std::string _clusterNames;
    std::string _schemaIdMap;
    std::string _dataDesc;
    bool _useV2Graph = false;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BatchReporter);

}} // namespace build_service::task_base

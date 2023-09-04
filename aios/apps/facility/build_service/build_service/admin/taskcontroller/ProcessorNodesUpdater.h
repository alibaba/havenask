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
#ifndef ISEARCH_BS_PROCESSORNODESUPDATER_H
#define ISEARCH_BS_PROCESSORNODESUPDATER_H

#include "autil/StringUtil.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/admin/taskcontroller/ProcessorInput.h"
#include "build_service/common/ProcessorOutput.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

struct ProcessorTargetInfo;
class ProcessorTargetInfos;
class ProcessorWriterVersion;
struct ProcessorBasicInfo;

class ProcessorNodesUpdater
{
protected:
    static const int64_t UNLIMIT_STOP_TIME;

public:
    struct ProcessorIO {
        ProcessorInput input;
        common::ProcessorOutput output;
    };

public:
    ProcessorNodesUpdater(const config::ResourceReaderPtr configReader,
                          taskcontroller::NodeStatusManagerPtr nodeStatusManager, std::vector<std::string> clusterNames,
                          const ProcessorIO& io, int64_t stopTimestamp, const std::string& configName, bool isTablet,
                          bool needSafeWrite);
    ~ProcessorNodesUpdater() {}

private:
    ProcessorNodesUpdater& operator=(const ProcessorNodesUpdater&);
    void checkAndSetNodeReady(proto::ProcessorNodes& nodes);
    void generateAndSetTarget(proto::ProcessorNodes& nodes, const ProcessorWriterVersion& writerVersion,
                              const ProcessorBasicInfo& basicInfo, ProcessorTargetInfos& lastTargetInfo) const;
    proto::ProcessorTarget generateTargetStatus(const ProcessorTargetInfo& targetInfo, uint32_t majorVersion,
                                                uint32_t minorVersion) const;

public:
    bool isAllProcessorFinished() const { return _finished; }
    void resetStopTimestamp(int64_t stopTimestamp) { _stopTimestamp = stopTimestamp; };

    int64_t getCheckPoint() const { return _input.offset; }
    void update(proto::ProcessorNodes& processorNodes, const ProcessorWriterVersion& writerVersion,
                const ProcessorBasicInfo& basicInfo, ProcessorTargetInfos& lastTargetInfo);
    ProcessorNodesUpdater* clone() const;

protected:
    int64_t getMinCheckpoint(const proto::ProcessorNodes& processorNodes);

protected:
    config::ResourceReaderPtr _configReader;
    taskcontroller::NodeStatusManagerPtr _nodeStatusManager;
    std::vector<std::string> _clusterNames;
    ProcessorInput _input;
    common::ProcessorOutput _output;
    int64_t _stopTimestamp;
    bool _finished;
    int64_t _lastSyncTime;
    int64_t _syncCheckpointInterval;
    std::string _configName;
    bool _isTablet;
    bool _needSafeWrite;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorNodesUpdater);

}} // namespace build_service::admin

#endif // ISEARCH_BS_PROCESSORNODESUPDATER_H

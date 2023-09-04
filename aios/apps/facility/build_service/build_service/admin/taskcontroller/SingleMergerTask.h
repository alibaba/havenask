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
#ifndef ISEARCH_BS_SINGLEMERGERTASK_H
#define ISEARCH_BS_SINGLEMERGERTASK_H

#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common_define.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/JsonizableProtobuf.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace admin {

class SingleMergerTask : public AdminTaskBase
{
public:
    SingleMergerTask(const proto::BuildId& buildId, const std::string& cluserName,
                     const TaskResourceManagerPtr& resMgr);
    ~SingleMergerTask();

private:
    SingleMergerTask& operator=(const SingleMergerTask&);

private:
    typedef proto::JsonizableProtobuf<proto::IndexInfo> JsonizableIndexInfo;

private:
    bool loadFromConfig(const config::ResourceReaderPtr& resourceReader) override;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    bool init(proto::BuildStep buildStep, const std::string& mergeConfigName = "", int32_t batchMask = -1);
    /**
     * return true when merge finished
     */
    bool run(proto::WorkerNodes& workerNodes) override;
    bool start(const KeyValueMap& kvMap) override;
    bool finish(const KeyValueMap& kvMap) override;

public:
    void waitSuspended(proto::WorkerNodes& workerNodes) override;
    bool run(proto::MergerNodes& activeNodes);
    bool updateConfig() override;
    void fillClusterInfo(proto::SingleClusterInfo* clusterInfo, const proto::MergerNodes& nodes,
                         const CounterCollector::RoleCounterMap& roleCounterMap) const;
    void doSupplementLableInfo(KeyValueMap& info) const override;
    std::string getTaskPhaseIdentifier() const override;
    void clearFullWorkerZkNode(const std::string& generationDir) const;

protected:
    virtual std::string getTaskIdentifier() const { return ""; }
    virtual proto::MergerTarget generateTargetStatus() const;
    virtual void endMerge(::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos);
    bool switchToInc();

private:
    void prepareMerge(int32_t workerPathVersion);
    bool prepareSlowNodeDetector(const std::string& clusterName) override
    {
        return doPrepareSlowNodeDetector<proto::MergerNodes>(clusterName);
    }

    bool getLatestCheckpoint(proto::BuilderCheckpoint& builderCheckpoint);
    bool startFromCheckpoint(int64_t schemaId, bool skipBuildCheckpoint);
    static bool enableAlterFieldVersionFromEnv();
    bool enableAlignedVersion() const { return _alignVersion; }
    versionid_t getAlignedVersionId() const { return _alignedVersionId; }

    proto::BuildStep getStep() const;
    bool updateKeepCheckpointCount(config::ResourceReaderPtr resourceReader);

    int64_t getTotalRemainIndexSize() const;

    void initMergeNodes(proto::MergerNodes& activeNodes);
    // bool currentStepFinished(proto::MergerNodes &activeNodes);
    void generateAndSetTarget(proto::MergerNodes& nodes) const;
    uint32_t getMergeParallelNum(const std::string& mergeConfigName) const;
    void moveToNextStep();
    void collectAlignVersion(proto::MergerNodes& activeNodes);
    void collectIndexInfo(proto::MergerNodes& activeNodes,
                          ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos);
    std::string getMergeConfigName() { return _mergeConfigName; }
    bool needWaitAlterField(bool needWait);
    bool getMergeConfigNeedWaitAlterField(bool& needWait) const;
    bool initOpIds(uint32_t maxOpId);
    bool checkConfigChanged(proto::MergerNodes& activeNodes) const;
    bool doWaitSuspended(proto::MergerNodes& mergerNodes);

public:
    static int32_t DEFAULT_CHECKPOINT_KEEP_COUNT;
    static std::string MERGE_CONFIG_SYNC_VERSION;
    // for legacy
    // void registMsgProvider(MultiClusterMsgDeliverer& deliver);
protected:
    bool _optimizeMerge;
    proto::MergeStep _mergeStep;
    std::string _clusterName;
    uint32_t _partitionCount;
    uint32_t _buildParallelNum;
    uint32_t _mergeParallelNum;
    int32_t _workerPathVersion;
    std::string _mergeConfigName;
    std::string _finishStepString;
    int64_t _timestamp;
    versionid_t _alignedVersionId;
    bool _alignVersion;
    int64_t _schemaVersion;
    int32_t _checkpointKeepCount;
    proto::BuildStep _buildStep;
    std::string _builderCheckpoint;
    bool _forceStop;
    int64_t _remainIndexSize;
    bool _needWaitAlterField;
    bool _disableFillIndexInfo;
    schema_opid_t _maxOpId;
    std::string _opIds;
    int32_t _workerProtocolVersion;
    int32_t _batchMask;

    common::IndexCheckpointAccessorPtr _indexCkpAccessor;
    common::BuilderCheckpointAccessorPtr _builderCkpAccessor;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleMergerTask);

}} // namespace build_service::admin

#endif // ISEARCH_BS_SINGLEMERGERTASK_H

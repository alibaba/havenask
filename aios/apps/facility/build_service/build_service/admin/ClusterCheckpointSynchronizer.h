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

#include <map>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/CheckpointMetricReporter.h"
#include "build_service/common/Checkpoint.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionMeta.h"

namespace build_service { namespace admin {

class ClusterCheckpointSynchronizer
{
public:
    struct Options {
        Options() = default;
        Options(int64_t syncIntervalMs, uint32_t reserveCheckpointCount, uint32_t reserveDailyCheckpointCount)
            : syncIntervalMs(syncIntervalMs)
            , reserveCheckpointCount(reserveCheckpointCount)
            , reserveDailyCheckpointCount(reserveDailyCheckpointCount)
        {
        }
        int64_t syncIntervalMs = 900 * 1000;
        uint32_t reserveCheckpointCount = 2;
        uint32_t reserveDailyCheckpointCount = 0;
    };

    struct SingleGenerationLevelCheckpointResult : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("is_savepoint", isSavepoint, isSavepoint);
            json.Jsonize("is_daily_savepoint", isDailySavepoint, isDailySavepoint);
            json.Jsonize("max_timestamp", generationLevelCheckpoint.maxTimestamp,
                         generationLevelCheckpoint.maxTimestamp);
            json.Jsonize("min_timestamp", generationLevelCheckpoint.minTimestamp,
                         generationLevelCheckpoint.minTimestamp);
            json.Jsonize("checkpoint_id", generationLevelCheckpoint.checkpointId,
                         generationLevelCheckpoint.checkpointId);
            json.Jsonize("create_time", generationLevelCheckpoint.createTime, generationLevelCheckpoint.createTime);
            json.Jsonize("read_schema_id", generationLevelCheckpoint.readSchemaId,
                         generationLevelCheckpoint.readSchemaId);
            json.Jsonize("is_obsoleted", generationLevelCheckpoint.isObsoleted, generationLevelCheckpoint.isObsoleted);
        }
        bool isSavepoint = false;
        bool isDailySavepoint = false;
        common::GenerationLevelCheckpoint generationLevelCheckpoint;
    };
    struct GenerationLevelCheckpointGetResult : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            generationLevelCheckpoint.Jsonize(json);
            json.Jsonize("checkpoint_info_file", checkpointInfoFile, checkpointInfoFile);
            json.Jsonize("is_savepoint", isSavepoint, isSavepoint);
            json.Jsonize("comment", comment, comment);
            json.Jsonize("is_obsoleted", generationLevelCheckpoint.isObsoleted, generationLevelCheckpoint.isObsoleted);
        }
        std::string comment;
        std::string checkpointInfoFile;
        bool isSavepoint = false;
        common::GenerationLevelCheckpoint generationLevelCheckpoint;
    };

    struct IndexInfoParam {
        int64_t startTimestamp = 0;
        int64_t finishTimestamp = 0;
        std::map<proto::Range, int64_t> totalRemainIndexSizes;
    };
    struct SyncOptions {
        bool isDaily = false;
        bool isInstant = false;
        bool isOffline = false;
        bool addIndexInfos = true;
        IndexInfoParam indexInfoParam;
    };

    ClusterCheckpointSynchronizer(const proto::BuildId& buildId, const std::string& clusterName);
    virtual ~ClusterCheckpointSynchronizer();

    virtual bool init(const std::string& root, const std::vector<proto::Range>& ranges,
                      const std::shared_ptr<common::CheckpointAccessor>& checkpointAccessor,
                      const std::shared_ptr<CheckpointMetricReporter>& metricsReporter, const Options& options);

    virtual bool sync();
    bool publishPartitionLevelCheckpoint(const proto::Range& range, const std::string& versionMetaStr,
                                         bool checkVersionLine, std::string& errMsg);
    bool createSavepoint(checkpointid_t checkpointId, const std::string& comment,
                         GenerationLevelCheckpointGetResult* checkpoint, std::string& errMsg);
    bool removeSavepoint(checkpointid_t checkpointId, std::string& errMsg);
    bool getReservedVersions(const proto::Range& range,
                             std::set<indexlibv2::framework::VersionCoord>* reservedVersionList) const;
    bool listCheckpoint(bool savepointOnly, uint32_t offset, uint32_t limit,
                        std::vector<SingleGenerationLevelCheckpointResult>* result) const;
    bool getCheckpoint(checkpointid_t checkpointId, GenerationLevelCheckpointGetResult* result) const;
    bool getCommittedVersions(const proto::Range& range, uint32_t limit, std::vector<indexlibv2::versionid_t>& versions,
                              std::string& errorMsg);

    bool rollback(checkpointid_t checkpointId, const ::google::protobuf::RepeatedPtrField<proto::Range>& rangeVec,
                  bool needAddIndexInfo, checkpointid_t* resId, std::string& errorMsg);

    bool sync(const SyncOptions& options);

    const std::vector<proto::Range>& getRanges() const { return _ranges; }
    const Options& getOptions() const { return _options; }
    std::shared_ptr<CheckpointMetricReporter> getMetricsReporter() const { return _metricsReporter; }
    std::shared_ptr<common::CheckpointAccessor> getCheckpointAccessor() const { return _checkpointAccessor; }

    static bool generationCheckpointfromStr(const std::string& str, common::GenerationLevelCheckpoint* checkpoint);
    std::pair<bool, common::PartitionLevelCheckpoint> getPartitionCheckpoint(const proto::Range& range) const;

protected:
    void addIndexInfoToCheckpoint(const ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos);

    virtual void addCheckpointToIndexInfo(indexlibv2::schemaid_t schemaVersion,
                                          const std::vector<common::PartitionLevelCheckpoint>& partitionCheckpoints,
                                          const IndexInfoParam& indexInfoParam, bool isFullIndex = false);

    std::string getCheckpointFileName(const checkpointid_t checkpointId) const;
    std::string getCheckpointDirectory() const;
    std::string getCheckpointFilePath(const checkpointid_t checkpointId) const;
    std::string getGenerationUserName(const checkpointid_t checkpointId) const;

    bool getReservedVersions(const proto::Range& range,
                             std::set<indexlibv2::framework::VersionCoord>* reservedVersionList, bool isDaily) const;
    bool createSavepoint(checkpointid_t checkpointId, const std::string& comment,
                         GenerationLevelCheckpointGetResult* checkpoint, bool isDaily, std::string& errMsg);
    bool getCheckpoint(checkpointid_t checkpointId, GenerationLevelCheckpointGetResult* result, bool isDaily) const;

    bool saveCheckpointInRoot(checkpointid_t checkpointId, const std::string& checkpointContent);
    bool deleteCheckpointInRoot();
    bool checkPublishParam(const proto::Range& range, const indexlibv2::framework::VersionMeta& versionMeta,
                           bool checkVersionLine) const;
    bool createPartitionSavepointByGeneration(const common::GenerationLevelCheckpoint& checkpoint);
    bool removePartitionSavepointByGeneration(const common::GenerationLevelCheckpoint& checkpoint,
                                              const std::vector<proto::Range>& ranges, bool cleanPartitionCheckpoint);
    bool cleanPartitionCheckpoint(const std::vector<proto::Range>& ranges,
                                  const std::vector<common::GenerationLevelCheckpoint>& removeCheckpoints);
    checkpointid_t getUniqueCheckpointId() const;
    bool checkRollbackLegal(checkpointid_t checkpointId);
    bool checkNeedSync(bool isDaily, bool instant, common::GenerationLevelCheckpoint* latestCheckpoint, bool* needSync);
    bool generateGenerationCheckpoint(common::GenerationLevelCheckpoint* checkpoint,
                                      std::vector<common::PartitionLevelCheckpoint>* partitionCheckpoints);
    bool generateCheckpointId(checkpointid_t* checkpointId);
    bool publishGenerationCheckpoint(const common::GenerationLevelCheckpoint& checkpoint,
                                     const std::vector<common::PartitionLevelCheckpoint>& partitionCheckpoints,
                                     const ClusterCheckpointSynchronizer::SyncOptions& options);
    bool getRemoveGenerationLevelCheckpoints(checkpointid_t checkpointId,
                                             std::vector<common::GenerationLevelCheckpoint>* removeCheckpoints);
    bool updateRollbackMark(const std::vector<proto::Range>& ranges);
    std::vector<proto::Range> convertRanges(const ::google::protobuf::RepeatedPtrField<proto::Range>& rangeVec);

    void reportPartitionLevelCheckpointMetric();

    // virtual for test
    virtual int64_t getCurrentTime() const;

protected:
    static constexpr const char* GENERATION_USER_PREFIX = "generation_user";
    static constexpr const char* CHECKPOINT_DIRECTORY_NAME = "checkpoint";
    static constexpr const char* CHECKPOINT_FILE_PREFIX = "checkpoint";
    static constexpr const int64_t DAYTIME_IN_MS = 24 * 60 * 60 * 1000;
    static constexpr const int64_t PARTITION_RESERVE_COUNT = 1;

    std::string _root;
    std::vector<proto::Range> _ranges;
    Options _options;

    proto::BuildId _buildId;
    std::string _clusterName;
    std::shared_ptr<common::CheckpointAccessor> _checkpointAccessor;
    std::shared_ptr<CheckpointMetricReporter> _metricsReporter;

    autil::ThreadMutex _mutex;

    BS_LOG_DECLARE();
};
}} // namespace build_service::admin

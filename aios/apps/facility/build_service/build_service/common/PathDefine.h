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
#ifndef ISEARCH_BS_PATHDEFINE_H
#define ISEARCH_BS_PATHDEFINE_H

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace common {

/* admin:
 *  zfs://zkroot/admin
 *  zfs://zkroot/admin/leader_elector
 *  zfs://zkroot/admin/generation.tableName.0/
 *  zfs://zkroot/admin/generation.tableName.0/generation_status
 *  zfs://zkroot/control_flow/
 *  zfs://zkroot/admin_schedule_timestamp
 *
 * workers:
 *  zfs://zkroot/generation.tableName.0/processor.0.65535/
 *  zfs://zkroot/generation.tableName.0/processor.0.65535/leader_elector
 *  zfs://zkroot/generation.tableName.0/processor.0.65535/current
 *  zfs://zkroot/generation.tableName.0/processor.0.65535/target
 *  zfs://zkroot/generation.tableName.0/processor.0.65535/reader1.checkpoint
 *  zfs://zkroot/generation.tableName.0/processor.0.65535/reader2.checkpoint
 *  zfs://zkroot/generation.tableName.0/builder.cluster1.0.65535/
 *  zfs://zkroot/generation.tableName.0/builder.cluster2.0.65535/
 *
 * partition lock:
 *  zfs://zkroot/generation.tableName.0.clusterName.0.32767.__lock__
 *  zfs://zkroot/generation.tableName.0.clusterName.32768.65535.__lock__
 *
 * cluster check point
 *  zfs://zkroot/generation.tableName.0/checkpoints/cluster0/v1
 *  zfs://zkroot/generation.tableName.0/checkpoints/cluster0/v2
 *  zfs://zkroot/generation.tableName.0/checkpoints/cluster1/v1
 *
 */

class PathDefine
{
public:
    // static std::string getPartitionCurrentStatusZkPath(
    //         const std::string &zkRoot, const proto::PartitionId &pid);
    // static std::string getPartitionTargetStatusZkPath(
    //         const std::string &zkRoot, const proto::PartitionId &pid);

    static std::string getAdminZkRoot(const std::string& zkRoot);
    static std::string getAgentNodeRoot(const std::string& zkRoot);
    static std::string getAdminControlFlowRoot(const std::string& zkRoot);
    static std::string getAdminScheduleTimestampFilePath(const std::string& zkRoot);
    static std::string getGenerationZkRoot(const std::string& zkRoot, const proto::BuildId& buildId);
    static std::string getGenerationStatusFile(const std::string& zkRoot, const proto::BuildId& buildId);
    static std::string getGenerationStopFile(const std::string& zkRoot, const proto::BuildId& buildId);
    static std::string getPartitionZkRoot(const std::string& zkRoot, const proto::PartitionId& pid,
                                          bool ignoreBackupId = false);
    static std::string getRoleZkPath(const std::string& generationRoot, const proto::PartitionId& pid,
                                     bool ignoreBackupId);

    static bool getAllGenerations(const std::string& adminZkRoot, std::vector<proto::BuildId>& buildIds);
    static std::string getLocalConfigPath();
    static std::string getLocalDataPath();
    static std::string getLocalLuaScriptRootPath();
    static std::string getAmonitorNodePath(const proto::PartitionId& pid);

    static std::string getCheckpointFilePath(const std::string& zkRoot, const proto::PartitionId& pid);

    static std::string getIndexPartitionLockPath(const std::string& zkRoot, const proto::PartitionId& pid);

    static bool partitionIdFromHeartbeatPath(const std::string& path, proto::PartitionId& pid);
    static std::string parseZkHostFromZkPath(const std::string& zkPath);

    static std::string getClusterCheckpointFileName(const std::string& generationDir, const std::string& clusterName,
                                                    const versionid_t versiondId);

    static std::string getClusterCheckpointDir(const std::string& generationDir, const std::string& clusterName);

    static std::string getReservedCheckpointFileName(const std::string& generationDir, const std::string& clusterName);

    static std::string getSnapshotCheckpointFileName(const std::string& generationDir, const std::string& clusterName);

public:
    static const std::string ZK_GENERATION_DIR_PREFIX;
    static const std::string ZK_GENERATION_STATUS_FILE;
    static const std::string ZK_GENERATION_STATUS_STOP_FILE;
    static const std::string ZK_CURRENT_STATUS_FILE;
    static const std::string ZK_TARGET_STATUS_FILE;
    static const std::string ZK_ADMIN_DIR;
    static const std::string ZK_AGENT_NODE_DIR;
    static const std::string ZK_AGENT_BLACK_LIST_FILE;
    static const std::string ZK_ADMIN_CONTROL_FLOW_DIR;
    static const std::string ZK_GENERATION_STATUS_FILE_SPLIT;
    static const std::string INDEX_PARTITION_LOCK_DIR_NAME;
    static const std::string ZK_GENERATION_CHECKPOINT_DIR;
    static const std::string CLUSTER_CHECKPOINT_PREFIX;
    static const std::string RESERVED_CHECKPOINT_FILENAME;
    static const std::string SNAPSHOT_CHECKPOINT_FILENAME;
    static const std::string CHECK_POINT_STATUS_FILE;
    static const std::string ZK_GENERATION_INDEX_INFOS;
    static const std::string ZK_SERVICE_INFO_TEMPLATE;
    static const std::string ZK_GLOBAL_AGENT_CONFIG;
    static const std::string ZK_GLOBAL_AGENT_TARGET_INFO;
    static const std::string WORKER_RUNTIMEDATA_PATH;
    static const std::string ZK_SCHEDULE_TIMESTAMP_FILE;

private:
    PathDefine();
    ~PathDefine();

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PathDefine);

}} // namespace build_service::common

#endif // ISEARCH_BS_PATHDEFINE_H

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
#ifndef ISEARCH_BS_INDEXCHECKPOINTACCESSOR_H
#define ISEARCH_BS_INDEXCHECKPOINTACCESSOR_H

#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace common {

class CheckpointAccessor;
BS_TYPEDEF_PTR(CheckpointAccessor);

class IndexCheckpointAccessor;
BS_TYPEDEF_PTR(IndexCheckpointAccessor);

class BuilderCheckpointAccessor;
BS_TYPEDEF_PTR(BuilderCheckpointAccessor);

class IndexCheckpointAccessor
{
public:
    IndexCheckpointAccessor(const CheckpointAccessorPtr& accessor);
    ~IndexCheckpointAccessor();

private:
    IndexCheckpointAccessor(const IndexCheckpointAccessor&);
    IndexCheckpointAccessor& operator=(const IndexCheckpointAccessor&);

public:
    //-----------index info operation-----------------
    bool hasFullIndexInfo(const std::string& clusterName);
    void updateIndexInfo(bool isFullIndex, const std::string& clusterName,
                         const ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos);
    bool getIndexInfo(bool isFull, const std::string& clusterName,
                      ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos);

    //-----------index checkpoint operation-----------------
    bool getIndexCheckpoint(const std::string& clusterName, versionid_t versionId,
                            proto::CheckpointInfo& checkpointInfo);

    bool getLatestIndexCheckpoint(const std::string& clusterName, proto::CheckpointInfo& checkpointInfo,
                                  std::string& checkpointName);

    bool getLatestIndexCheckpoint(const std::string& clusterName, int64_t schemaId,
                                  proto::CheckpointInfo& checkpointInfo, std::string& checkpointName);

    void addIndexCheckpoint(const std::string& clusterName, versionid_t targetVersion, int32_t keepCount,
                            proto::CheckpointInfo& checkpointInfo);

    void clearIndexCheckpoint(const std::string& clusterName, versionid_t targetVersion);
    std::set<versionid_t> getReservedVersions(const std::string& clusterName);

    //------------index savepoint operation-----------------
    bool createIndexSavepoint(const std::string& applyRole, const std::string& clusterName, versionid_t targetVersion);
    bool createIndexSavepoint(const std::string& applyRole, const std::string& clusterName, versionid_t targetVersion,
                              const std::string& comment, proto::CheckpointInfo& checkpointInfo, std::string& errorMsg);

    bool removeIndexSavepoint(const std::string& applyRole, const std::string& clusterName, versionid_t targetVersion);
    bool removeIndexSavepoint(const std::string& applyRole, const std::string& clusterName, versionid_t targetVersion,
                              std::string& errorMsg);

    bool isSavepoint(const std::string& clusterName, versionid_t version);

    bool listCheckpoint(const std::string& clusterName, bool savepointOnly, uint32_t limit,
                        std::vector<proto::CheckpointInfo>* checkpointInfos) const;

    bool getSchemaChangedStandard(const std::string& clusterName, std::string& changedSchema);

public:
    static std::set<versionid_t> getReservedVersions(const std::string& clusterName,
                                                     const IndexCheckpointAccessorPtr& indexCkpAccessor,
                                                     const BuilderCheckpointAccessorPtr& builderCkpAccessor);

private:
    CheckpointAccessorPtr _accessor;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::common

#endif // ISEARCH_BS_INDEXCHECKPOINTACCESSOR_H

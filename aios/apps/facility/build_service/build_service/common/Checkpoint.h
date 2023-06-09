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
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BuildTaskCurrentInfo.h"
#include "build_service/util/Log.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/framework/VersionMeta.h"
#include "indexlib/util/PathUtil.h"

namespace build_service { namespace common {

struct GenerationLevelCheckpoint : public autil::legacy::Jsonizable {
public:
    static std::string genGenerationLevelCheckpointKey(const proto::BuildId& buildId, const std::string& clusterName,
                                                       bool isDaily)
    {
        auto key = buildId.appname() + "_" + buildId.datatable() + "_" +
                   autil::StringUtil::toString(buildId.generationid()) + "_" + clusterName;
        key = key + (isDaily ? CHECKPOINT_DAILY_SUFFIX : CHECKPOINT_SUFFIX);
        return key;
    }

    static constexpr const char* CHECKPOINT_DAILY_SUFFIX = "_daily";
    static constexpr const char* CHECKPOINT_SUFFIX = "";

    GenerationLevelCheckpoint() = default;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("max_timestamp", maxTimestamp, maxTimestamp);
        json.Jsonize("min_timestamp", minTimestamp, minTimestamp);
        json.Jsonize("checkpoint_id", checkpointId, checkpointId);
        json.Jsonize("version_id_mapping", versionIdMapping, versionIdMapping);
        json.Jsonize("create_time", createTime, createTime);
        json.Jsonize("read_schema_id", readSchemaId, readSchemaId);
        json.Jsonize("is_obsoleted", isObsoleted, isObsoleted);
    }
    bool isObsoleted = false;
    checkpointid_t checkpointId = 0;
    int64_t maxTimestamp = indexlibv2::INVALID_TIMESTAMP;
    int64_t minTimestamp = std::numeric_limits<int64_t>::max();
    int64_t createTime = indexlibv2::INVALID_TIMESTAMP;
    indexlibv2::schemaid_t readSchemaId = indexlibv2::INVALID_SCHEMAID;
    std::map<std::string, indexlibv2::versionid_t> versionIdMapping;

    static constexpr const int64_t INVALID_CHECKPOINT_ID = -1;
};

struct PartitionLevelCheckpoint : public autil::legacy::Jsonizable {
public:
    static std::string genPartitionLevelCheckpointKey(const proto::BuildId& buildId, const std::string& clusterName,
                                                      const proto::Range& range)
    {
        return GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(buildId, clusterName,
                                                                          /*isDaily=*/false) +
               "_" + genRangeKey(range);
    }

    static std::string genRangeKey(const proto::Range& range)
    {
        return autil::StringUtil::toString(range.from()) + "_" + autil::StringUtil::toString(range.to());
    }

    PartitionLevelCheckpoint() = default;
    PartitionLevelCheckpoint(const indexlibv2::framework::VersionMeta& versionMeta, const proto::Range& range,
                             int64_t createTime)
        : versionId(versionMeta.GetVersionId())
        , readSchemaId(versionMeta.GetReadSchemaId())
        , maxTimestamp(versionMeta.GetMaxLocatorTs())
        , minTimestamp(versionMeta.GetMinLocatorTs())
        , from(range.from())
        , to(range.to())
        , createTime(createTime)
        , indexSize(versionMeta.GetIndexSize())
        , fenceName(versionMeta.GetFenceName())
        , versionLine(versionMeta.GetVersionLine())
        , isRollbackVersion(false)
    {
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("version_id", versionId, versionId);
        json.Jsonize("read_schema_id", readSchemaId, readSchemaId);
        json.Jsonize("max_timestamp", maxTimestamp, maxTimestamp);
        json.Jsonize("min_timestamp", minTimestamp, minTimestamp);
        json.Jsonize("from", from, from);
        json.Jsonize("to", to, to);
        json.Jsonize("create_time", createTime, createTime);
        json.Jsonize("index_size", indexSize, indexSize);
        json.Jsonize("fence_name", fenceName, fenceName);
        json.Jsonize("version_line", versionLine, versionLine);
        json.Jsonize("is_rollback_version", isRollbackVersion, isRollbackVersion);
    }
    indexlibv2::versionid_t versionId = indexlibv2::INVALID_VERSIONID;
    indexlibv2::schemaid_t readSchemaId = indexlibv2::INVALID_SCHEMAID;
    int64_t maxTimestamp = indexlibv2::INVALID_TIMESTAMP;
    int64_t minTimestamp = std::numeric_limits<int64_t>::max();
    uint32_t from = 0;
    uint32_t to = 65535;
    int64_t createTime = 0;
    int64_t indexSize = 0;
    std::string fenceName;
    indexlibv2::framework::VersionLine versionLine;
    bool isRollbackVersion = false;
};

class VersionMetaWrapper : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("version_meta", _versionMeta, _versionMeta);
    }

    const indexlibv2::framework::VersionMeta& getVersionMeta() const { return _versionMeta; }

private:
    indexlibv2::framework::VersionMeta _versionMeta;
};

struct PathInfo {
    std::string root;
    std::string clusterName;
    proto::BuildId buildId;
    proto::Range range;
    std::string path;
    PathInfo() = default;
    PathInfo(const std::string& root, const std::string& clusterName, const proto::BuildId& buildId,
             const proto::Range& range)
        : root(root)
        , clusterName(clusterName)
        , buildId(buildId)
        , range(range)
    {
        std::string fileName = clusterName + "_" + autil::StringUtil::toString(buildId.generationid()) + "_" +
                               autil::StringUtil::toString(range.from()) + "_" +
                               autil::StringUtil::toString(range.to()) + ".version";
        path = indexlib::util::PathUtil::JoinPath(root, fileName);
        std::string tmpPath = fslib::util::FileUtil::getPathFromZkPath(path);
        if (tmpPath != "") {
            path = tmpPath;
        }
    }
};

// offline v2 builder checkpoint
struct BuildCheckpoint : public autil::legacy::Jsonizable {
    bool buildFinished = false;
    proto::BuildStep buildStep;
    proto::BuildTaskCurrentInfo buildInfo;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("build_step", buildStep, buildStep);
        json.Jsonize("build_task_info", buildInfo, buildInfo);
        json.Jsonize("build_finished", buildFinished, buildFinished);
    }
};

}} // namespace build_service::common

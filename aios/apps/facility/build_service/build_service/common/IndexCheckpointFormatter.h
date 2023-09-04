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
#ifndef ISEARCH_BS_INDEXCHECKPOINTFORMATTER_H
#define ISEARCH_BS_INDEXCHECKPOINTFORMATTER_H

#include "autil/StringUtil.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/JsonizableProtobuf.h"
#include "build_service/util/Log.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace common {

class IndexCheckpointFormatter
{
public:
    IndexCheckpointFormatter();
    ~IndexCheckpointFormatter();

private:
    typedef proto::JsonizableProtobuf<proto::BuilderCheckpoint> JsonizableBuilderCheckpoint;

private:
    IndexCheckpointFormatter(const IndexCheckpointFormatter&);
    IndexCheckpointFormatter& operator=(const IndexCheckpointFormatter&);

public:
    static std::string getClusterCheckpointVisiableId(const std::string& clusterName)
    {
        std::string visiableId = BS_CHECKPOINT_VISIABLE_ID + "_" + clusterName;
        return visiableId;
    }
    static void convertToCheckpoint(const proto::IndexInfo& indexInfo, proto::CheckpointInfo& checkpoint);
    static std::string getBuilderCheckpointId(const std::string& clusterName)
    {
        std::string builderCheckpointId = BS_BUILDER_CHECKPOINT_ID + "_" + clusterName;
        return builderCheckpointId;
    }
    static std::string encodeBuilderCheckpoint(const proto::BuilderCheckpoint& checkpoint)
    {
        JsonizableBuilderCheckpoint jsonCheckpoint(checkpoint);
        return ToJsonString(jsonCheckpoint);
    }

    static void decodeBuilderCheckpoint(const std::string& checkpointValue, proto::BuilderCheckpoint& checkpoint)
    {
        JsonizableBuilderCheckpoint jsonCheckpoint;
        FromJsonString(jsonCheckpoint, checkpointValue);
        checkpoint = jsonCheckpoint.get();
    }

    static std::string getIndexTotalSizeCheckpointId(const std::string& clusterName)
    {
        return BS_INDEX_TOTAL_SIZE_ID + "_" + clusterName;
    }

    static std::string getBuilderCheckpointName(int64_t schemaId, versionid_t versionId)
    {
        std::stringstream ss;
        ss << schemaId << "_" << versionId;
        return ss.str();
    }

    static std::string getSchemaPatch(const std::string& clusterName)
    {
        return BS_CKP_SCHEMA_PATCH + "_" + clusterName;
    }

    static bool decodeBuilderCheckpointName(const std::string& builderCheckpointName, int64_t& schemaId,
                                            versionid_t& versionId);

    static std::string getIndexInfoId(bool isFullIndex, const std::string& clusterName)
    {
        std::string fullId = isFullIndex ? "_full" : "";
        return BS_INDEX_INFO_ID + "_" + clusterName + fullId;
    }

    static std::string getIndexCheckpointId(bool isMainIndex, const std::string& clusterName)
    {
        if (isMainIndex) {
            return BS_INDEX_CHECKPOINT_ID + "_" + clusterName;
        } else {
            return BS_BRANCH_INDEX_CHECKPOINT_ID + "_" + clusterName;
        }
    }

    static versionid_t getCheckpointVersion(const std::string& checkpointName)
    {
        versionid_t version;
        if (!autil::StringUtil::strToInt32(checkpointName.c_str(), version)) {
            assert(false);
            return -1;
        }
        return version;
    }
    static std::string encodeCheckpointName(versionid_t version) { return autil::StringUtil::toString(version); }

    static std::string encodeIndexInfo(std::vector<proto::JsonizableProtobuf<proto::IndexInfo>>& indexInfos)
    {
        return ToJsonString(indexInfos);
    }

    static void decodeIndexInfo(const std::string& checkpoint,
                                ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos);

    static std::string encodeCheckpoint(const proto::CheckpointInfo& checkpointInfo)
    {
        proto::JsonizableProtobuf<proto::CheckpointInfo> checkpoint(checkpointInfo);
        return ToJsonString(checkpoint);
    }

    static bool decodeCheckpoint(const std::string& checkpoint, proto::CheckpointInfo& checkpointInfo);

    static std::string getMasterBuilderCheckpointId(const std::string& clusterName, const proto::Range& range);
    static std::string getMasterBuilderCheckpointName(versionid_t versionId);
    static std::string getSlaveBuilderCheckpointId(const std::string& clusterName, const proto::Range& range);
    static std::string getSlaveBuilderCheckpointName(versionid_t versionId);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexCheckpointFormatter);

}} // namespace build_service::common

#endif // ISEARCH_BS_INDEXCHECKPOINTFORMATTER_H

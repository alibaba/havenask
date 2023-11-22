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
#include "build_service/common/IndexCheckpointFormatter.h"

#include <memory>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"

using namespace std;
using namespace autil;

namespace build_service { namespace common {
BS_LOG_SETUP(common, IndexCheckpointFormatter);

IndexCheckpointFormatter::IndexCheckpointFormatter() {}

IndexCheckpointFormatter::~IndexCheckpointFormatter() {}

void IndexCheckpointFormatter::decodeIndexInfo(const string& checkpoint,
                                               ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos)
{
    std::vector<proto::JsonizableProtobuf<proto::IndexInfo>> jsonizableIndexInfos;
    try {
        FromJsonString(jsonizableIndexInfos, checkpoint);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "invalid checkpoint %s", checkpoint.c_str());
        return;
    }
    for (auto indexInfo : jsonizableIndexInfos) {
        *indexInfos.Add() = indexInfo.get();
    }
}

void IndexCheckpointFormatter::convertToCheckpoint(const proto::IndexInfo& indexInfo,
                                                   proto::CheckpointInfo& checkpointInfo)
{
    checkpointInfo.set_versionid(indexInfo.indexversion());
    checkpointInfo.set_versiontimestamp(indexInfo.versiontimestamp());
    checkpointInfo.set_schemaid(indexInfo.schemaversion());
    checkpointInfo.set_processorcheckpoint(indexInfo.processorcheckpoint());
    checkpointInfo.set_processordatasourceidx(indexInfo.processordatasourceidx());
    checkpointInfo.set_ongoingmodifyopids(indexInfo.ongoingmodifyopids());
}

bool IndexCheckpointFormatter::decodeBuilderCheckpointName(const std::string& builderCheckpointName, int64_t& schemaId,
                                                           versionid_t& versionId)
{
    vector<string> infos = StringUtil::split(builderCheckpointName, "_");
    if (infos.size() != 2) {
        return false;
    }
    if (!StringUtil::fromString(infos[0], schemaId)) {
        return false;
    }
    if (!StringUtil::fromString(infos[1], versionId)) {
        return false;
    }
    return true;
}

bool IndexCheckpointFormatter::decodeCheckpoint(const std::string& checkpointStr, proto::CheckpointInfo& checkpointInfo)
{
    proto::JsonizableProtobuf<proto::CheckpointInfo> checkpoint;
    try {
        FromJsonString(checkpoint, checkpointStr);
        checkpointInfo = checkpoint.get();
    } catch (const autil::legacy::ExceptionBase& e) {
        // legacy code for old code
        try {
            std::vector<proto::JsonizableProtobuf<proto::IndexInfo>> jsonizableIndexInfos;
            FromJsonString(jsonizableIndexInfos, checkpointStr);
            if (jsonizableIndexInfos.size() <= 0) {
                BS_LOG(ERROR, "invalid checkpoint %s", checkpointStr.c_str());
                return false;
            }
            convertToCheckpoint(jsonizableIndexInfos[0].get(), checkpointInfo);
        } catch (const autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "invalid checkpoint %s", checkpointStr.c_str());
            return false;
        }
    }
    return true;
}

std::string IndexCheckpointFormatter::getMasterBuilderCheckpointId(const std::string& clusterName,
                                                                   const proto::Range& range)
{
    std::stringstream ss;
    ss << clusterName << "_master_builder_" << range.from() << "_" << range.to();
    return ss.str();
}

std::string IndexCheckpointFormatter::getMasterBuilderCheckpointName(versionid_t versionId)
{
    std::stringstream ss;
    ss << versionId;
    return ss.str();
}

std::string IndexCheckpointFormatter::getSlaveBuilderCheckpointId(const std::string& clusterName,
                                                                  const proto::Range& range)
{
    std::stringstream ss;
    ss << clusterName << "_slave_builder_" << range.from() << "_" << range.to();
    return ss.str();
}

std::string IndexCheckpointFormatter::getSlaveBuilderCheckpointName(versionid_t versionId)
{
    std::stringstream ss;
    ss << versionId;
    return ss.str();
}

}} // namespace build_service::common

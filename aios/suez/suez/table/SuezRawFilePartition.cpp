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
#include "suez/table/SuezRawFilePartition.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/partition_group_resource.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/SuezError.h"
#include "suez/sdk/SuezRawFilePartitionData.h"
#include "suez/table/InnerDef.h"
#include "suez/table/TablePathDefine.h"

namespace suez {
class DataOptionWrapper;
} // namespace suez

using namespace std;
using namespace fslib::fs;

using namespace indexlib::partition;
using namespace indexlib::config;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SuezRawFilePartition);

#define SUEZ_PREFIX ToJsonString(_pid, true).c_str()

namespace suez {

static const std::string BS_FILE_NAME = "file_name";
static const std::string SUEZ_OPS_NEED_RESERVE = "need_reserve";

SuezRawFilePartition::SuezRawFilePartition(const TableResource &tableResource,
                                           const CurrentPartitionMetaPtr &partitionMeta)
    : SuezPartition(partitionMeta, tableResource.pid, tableResource.metricsReporter)
    , _fileDeployer(tableResource.deployManager)
    , _dataSize(-1)
    , _memoryController(tableResource.globalIndexResource->GetMemoryQuotaController())
    , _partitionData(std::make_shared<SuezRawFilePartitionData>(tableResource.pid)) {}

SuezRawFilePartition::~SuezRawFilePartition() {}

StatusAndError<DeployStatus> SuezRawFilePartition::deploy(const TargetPartitionMeta &target, bool distDeploy) {
    string remoteDataPath = TablePathDefine::constructIndexPath(target.getRawIndexRoot(), _pid);
    auto dpRes = doDeploy(remoteDataPath, getLocalIndexPath());
    return setDeployStatus(dpRes, target);
}

DeployStatus SuezRawFilePartition::doDeploy(const std::string &remoteDataPath, const std::string &localIndexPath) {
    return _fileDeployer.deploy(remoteDataPath, localIndexPath);
}

void SuezRawFilePartition::cancelDeploy() { _fileDeployer.cancel(); }

void SuezRawFilePartition::cancelLoad() {
    // do nothing
}

StatusAndError<TableStatus> SuezRawFilePartition::load(const TargetPartitionMeta &target, bool force) {
    IncVersion version = target.getIncVersion();
    auto ret = doLoad(getLocalIndexPath());
    if (ret.status != TS_LOADED) {
        return ret;
    }
    _partitionMeta->setIncVersion(version);
    SUEZ_PREFIX_LOG(INFO, "load version [%d] done", version);
    return StatusAndError<TableStatus>(TS_LOADED, ERROR_NONE);
}

StatusAndError<TableStatus> SuezRawFilePartition::doLoad(const std::string &localIndexPath) {
    try {
        auto schema = indexlib::index_base::SchemaAdapter::LoadSchemaByVersionId(localIndexPath);
        if (!schema) {
            SUEZ_PREFIX_LOG(ERROR, "load schema failed");
            return StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_RAW_FILE_TABLE_ERROR);
        }
        string fileName;
        if (!schema->GetValueFromUserDefinedParam(BS_FILE_NAME, fileName)) {
            SUEZ_PREFIX_LOG(ERROR, "get file_name from schema failed");
            return StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_RAW_FILE_TABLE_ERROR);
        }
        auto filePath = FileSystem::joinFilePath(localIndexPath, fileName);
        string reserverFlag;
        if (schema->GetValueFromUserDefinedParam(SUEZ_OPS_NEED_RESERVE, reserverFlag) && reserverFlag == "yes") {
            auto dataSize = getDirSize(filePath);
            if (dataSize == -1) {
                SUEZ_PREFIX_LOG(ERROR, "reserve memory failed when try get dir size");
                return StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_RAW_FILE_TABLE_ERROR);
            }
            if (_memoryController->TryAllocate(dataSize).IsOK()) {
                _dataSize = dataSize;
                _partitionData->setFilePath(filePath);
            } else {
                SUEZ_PREFIX_LOG(ERROR,
                                "reserve memory failed, expected %ld, left %ld",
                                dataSize,
                                _memoryController->GetFreeQuota());
                return StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_LACKMEM_ERROR);
            }
        } else {
            _partitionData->setFilePath(filePath);
        }
        return StatusAndError<TableStatus>(TS_LOADED);
    } catch (const exception &e) {
        SUEZ_PREFIX_LOG(ERROR, "load schema failed, exception [%s]", e.what());
        return StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_RAW_FILE_TABLE_ERROR);
    } catch (...) {
        SUEZ_PREFIX_LOG(ERROR, "load schema failed, unknown exception");
        return StatusAndError<TableStatus>(TS_ERROR_UNKNOWN, LOAD_RAW_FILE_TABLE_ERROR);
    }
}

void SuezRawFilePartition::unload() {
    _partitionData->setFilePath("");
    _partitionMeta->setIncVersion(-1);
    if (_dataSize != -1) {
        SUEZ_PREFIX_LOG(DEBUG,
                        "free memory quota, free size %ld, before free left %ld",
                        _dataSize,
                        _memoryController->GetFreeQuota());
        _memoryController->Free(_dataSize);
        _dataSize = -1;
    }
    SUEZ_PREFIX_LOG(INFO, "unload done");
}

void SuezRawFilePartition::stopRt() { _partitionMeta->setRtStatus(TRS_SUSPENDED); }

void SuezRawFilePartition::suspendRt() { _partitionMeta->setRtStatus(TRS_SUSPENDED); }

void SuezRawFilePartition::resumeRt() { _partitionMeta->setRtStatus(TRS_BUILDING); }

bool SuezRawFilePartition::hasRt() const { return false; }

bool SuezRawFilePartition::isInUse() const { return _partitionData.use_count() > 1; }

int32_t SuezRawFilePartition::getUseCount() const { return _partitionData.use_count(); }

SuezPartitionDataPtr SuezRawFilePartition::getPartitionData() const { return _partitionData; }

int64_t SuezRawFilePartition::getDirSize(const string &dir) {
    if (dir.empty()) {
        SUEZ_PREFIX_LOG(ERROR, "invalid dir");
        return -1;
    }
    fslib::EntryInfoMap entryInfos;
    auto ec = FileSystem::listDir(dir, entryInfos, 4 /* threadnum */);
    if (fslib::EC_OK != ec) {
        SUEZ_PREFIX_LOG(ERROR, "list dir failed, path[%s]", dir.c_str());
        return -1;
    }
    int64_t totalSize = 0;
    for (const auto &it : entryInfos) {
        totalSize += it.second.length;
    }
    return totalSize;
}

} // namespace suez

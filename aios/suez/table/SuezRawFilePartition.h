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

#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "suez/common/TableMeta.h"
#include "suez/deploy/FileDeployer.h"
#include "suez/sdk/SuezRawFilePartitionData.h"
#include "suez/table/SuezPartition.h"

namespace suez {
class DataOptionWrapper;

class SuezRawFilePartition : public SuezPartition {
public:
    SuezRawFilePartition(const TableResource &tableResource, const CurrentPartitionMetaPtr &partitionMeta);
    ~SuezRawFilePartition();

private:
    SuezRawFilePartition(const SuezRawFilePartition &);
    SuezRawFilePartition &operator=(const SuezRawFilePartition &);

public:
    StatusAndError<DeployStatus> deploy(const TargetPartitionMeta &target, const bool distDeploy) override;
    void cancelDeploy() override;
    void cancelLoad() override;
    StatusAndError<TableStatus> load(const TargetPartitionMeta &target, bool force) override;
    void unload() override;
    void stopRt() override;
    void suspendRt() override;
    void resumeRt() override;
    bool hasRt() const override;
    bool isInUse() const override;
    int32_t getUseCount() const override;
    SuezPartitionDataPtr getPartitionData() const override;
    bool isRecovered() const override { return true; }

private:
    // virtual for test
    virtual DeployStatus doDeploy(const std::string &remote, const std::string &local);
    virtual StatusAndError<TableStatus> doLoad(const std::string &localIndexPath);

private:
    int64_t getDirSize(const std::string &dir);

private:
    FileDeployer _fileDeployer;
    int64_t _dataSize;
    indexlib::util::MemoryQuotaControllerPtr _memoryController;
    SuezRawFilePartitionDataPtr _partitionData;
};

using SuezRawFilePartitionPtr = std::shared_ptr<SuezRawFilePartition>;

} // namespace suez

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
#include "sql/ops/scan/ScanR.h"

#include <engine/ResourceInitContext.h>

#include "indexlib/partition/partition_reader_snapshot.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/resource/SqlConfig.h"

namespace indexlibv2 {
namespace framework {
class ITabletReader;
} // namespace framework
} // namespace indexlibv2
namespace indexlib {
namespace partition {
class IndexPartitionReader;
} // namespace partition
} // namespace indexlib

namespace sql {

const std::string ScanR::RESOURCE_ID = "sql.scan_r";

ScanR::ScanR() {}

ScanR::~ScanR() {}

void ScanR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_SUB_GRAPH);
}

bool ScanR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode ScanR::init(navi::ResourceInitContext &ctx) {
    partitionReaderSnapshot = _tableInfoR->createPartitionReaderSnapshot(ctx.getPartId());
    if (!partitionReaderSnapshot) {
        SQL_LOG(ERROR, "create partition reader snapshot failed");
        return navi::EC_ABORT;
    }
    partCount = ctx.getPartCount();
    partId = ctx.getPartId();
    const auto &sqlConfig = _sqlConfigResource->getSqlConfig();
    scanConfig.enableScanTimeout = sqlConfig.enableScanTimeout;
    scanConfig.asyncScanConcurrency = sqlConfig.asyncScanConcurrency;
    kernelPoolPtr = graphMemoryPoolR->getPool();
    return navi::EC_NONE;
}

std::shared_ptr<indexlib::partition::IndexPartitionReader>
ScanR::getPartitionReader(const std::string &tableName) const {
    return partitionReaderSnapshot->GetIndexPartitionReader(tableName);
}

std::shared_ptr<indexlibv2::framework::ITabletReader>
ScanR::getTabletReader(const std::string &tableName) const {
    return partitionReaderSnapshot->GetTabletReader(tableName);
}

REGISTER_RESOURCE(ScanR);

} // namespace sql

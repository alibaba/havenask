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

#include <limits>
#include <memory>
#include <stddef.h>
#include <string>

#include "autil/mem_pool/Pool.h"
#include "indexlib/partition/index_application.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/resource/PartitionInfoR.h"
#include "sql/resource/SqlConfigResource.h"
#include "suez/turing/navi/TableInfoR.h"

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
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

struct ScanConfig {
    bool enableScanTimeout = true;
    size_t asyncScanConcurrency = std::numeric_limits<size_t>::max();
};

class ScanR : public navi::Resource {
public:
    ScanR();
    ~ScanR();
    ScanR(const ScanR &) = delete;
    ScanR &operator=(const ScanR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    std::shared_ptr<indexlib::partition::IndexPartitionReader>
    getPartitionReader(const std::string &tableName) const;
    std::shared_ptr<indexlibv2::framework::ITabletReader>
    getTabletReader(const std::string &tableName) const;

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

public:
    RESOURCE_DEPEND_ON(navi::GraphMemoryPoolR, graphMemoryPoolR);
    RESOURCE_DEPEND_ON(suez::turing::TableInfoR, _tableInfoR);
    RESOURCE_DEPEND_ON(PartitionInfoR, _partitionInfoR);
    RESOURCE_DEPEND_ON(SqlConfigResource, _sqlConfigResource);

public:
    indexlib::partition::PartitionReaderSnapshotPtr partitionReaderSnapshot;
    navi::NaviPartId partCount = navi::INVALID_NAVI_PART_COUNT;
    navi::NaviPartId partId = navi::INVALID_NAVI_PART_ID;
    ScanConfig scanConfig;
    autil::mem_pool::PoolPtr kernelPoolPtr;
};

NAVI_TYPEDEF_PTR(ScanR);

} // namespace sql

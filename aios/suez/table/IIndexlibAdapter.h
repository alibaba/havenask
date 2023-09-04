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

#include <memory>
#include <set>

#include "suez/common/InnerDef.h"
#include "suez/common/TableMeta.h"
#include "suez/table/TableVersion.h"

namespace kmonitor {
class MetricsReporter;
using MetricsReporterPtr = std::shared_ptr<MetricsReporter>;
} // namespace kmonitor

namespace indexlibv2 {
namespace config {
class ITabletSchema;
}
} // namespace indexlibv2

namespace build_service {
namespace proto {
class PartitionId;
}
} // namespace build_service

namespace build_service {
namespace workflow {
struct RealtimeBuilderResource;
}
} // namespace build_service

namespace suez {

// 这层抽象是为了解短期内indexlib新老两个partition并存但接口又不统一的问题

class TableWriter;
class SuezPartitionData;
class TableBuilder;
class PartitionProperties;
class WALConfig;

class IIndexlibAdapter {
public:
    virtual ~IIndexlibAdapter() = default;

public:
    virtual TableStatus open(const PartitionProperties &properties, const TableVersion &tableVersion) = 0;
    virtual TableStatus reopen(bool force, const TableVersion &tableVersion) = 0;

    virtual void cleanIndexFiles(const std::set<IncVersion> &toKeepVersions) = 0;
    virtual bool cleanUnreferencedIndexFiles(const std::set<std::string> &toKeepFiles) = 0;

    virtual void reportAccessCounter(const std::string &name) const = 0;

    virtual std::shared_ptr<indexlibv2::config::ITabletSchema> getSchema() const = 0;
    virtual std::pair<TableVersion, SchemaVersion> getLoadedVersion() const = 0;

    virtual TableBuilder *createBuilder(const build_service::proto::PartitionId &pid,
                                        const build_service::workflow::RealtimeBuilderResource &rtResource,
                                        const PartitionProperties &properties) const = 0;

    virtual std::shared_ptr<SuezPartitionData>
    createSuezPartitionData(const PartitionId &pid, uint64_t totalPartitionCount, bool hasRt) const = 0;
};

} // namespace suez

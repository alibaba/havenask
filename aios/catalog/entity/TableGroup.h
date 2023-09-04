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

#include <functional>
#include <map>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "catalog/entity/DiffResult.h"
#include "catalog/entity/EntityBase.h"
#include "catalog/entity/LoadStrategy.h"
#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

SPECIALIZE_TRAITS(TableGroup, TableGroupId);

class TableGroup : public EntityBase<TableGroup> {
public:
    TableGroup() = default;
    ~TableGroup() = default;
    TableGroup(const TableGroup &) = delete;
    TableGroup &operator=(const TableGroup &) = delete;

public:
    const auto &tableGroupConfig() const { return _tableGroupConfig; };
    const auto &loadStrategies() const { return _loadStrategies; };

    void copyDetail(TableGroup *other) const;
    Status fromProto(const proto::TableGroup &proto);
    template <typename T>
    Status toProto(T *proto, proto::DetailLevel::Code detailLevel = proto::DetailLevel::DETAILED) const;
    Status compare(const TableGroup *other, DiffResult &diffResult) const;
    void updateChildStatus(const proto::EntityStatus &target, UpdateResult &updateResult);
    void alignVersion(CatalogVersion version);

    Status create(const proto::TableGroup &request);
    Status update(const proto::TableGroup &request);
    Status drop();
    Status createLoadStrategy(const proto::LoadStrategy &request);
    Status updateLoadStrategy(const proto::LoadStrategy &request);
    Status dropLoadStrategy(const LoadStrategyId &id);
    Status getLoadStrategy(const std::string &tableName, LoadStrategy *&loadStrategy) const;
    bool hasLoadStrategy(const std::string &tableName) const;
    std::vector<std::string> listLoadStrategy() const;

private:
    Status fromProtoOrCreate(const proto::TableGroup &proto, bool create);
    Status addEntityCheck(const proto::LoadStrategy &proto);
    using LoadStrategyExec = std::function<Status(LoadStrategy &)>;
    Status execute(const std::string &tableName, LoadStrategyExec exec);

private:
    proto::TableGroupConfig _tableGroupConfig;
    std::map<std::string, std::unique_ptr<LoadStrategy>> _loadStrategies;
    AUTIL_LOG_DECLARE();
};

extern template Status TableGroup::toProto<>(proto::TableGroup *, proto::DetailLevel::Code) const;
extern template Status TableGroup::toProto<>(proto::TableGroupRecord *, proto::DetailLevel::Code) const;

} // namespace catalog

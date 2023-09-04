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
#include <vector>

#include "catalog/entity/EntityId.h"
#include "catalog/proto/CatalogEntity.pb.h"

namespace catalog {

template <typename T>
struct EntityIdVersionStatus {
    T id;
    CatalogVersion version;
    proto::EntityStatus status;

    EntityIdVersionStatus() = default;

    EntityIdVersionStatus(const T &id, CatalogVersion version, const proto::EntityStatus &status) {
        this->id = id;
        this->version = version;
        this->status = status;
    }
};

struct UpdateResult {
    std::unique_ptr<EntityIdVersionStatus<CatalogId>> catalog;
    std::vector<EntityIdVersionStatus<DatabaseId>> databases;
    std::vector<EntityIdVersionStatus<TableId>> tables;
    std::vector<EntityIdVersionStatus<PartitionId>> partitions;
    std::vector<EntityIdVersionStatus<TableStructureId>> tableStructures;
    std::vector<EntityIdVersionStatus<FunctionId>> functions;
    std::vector<EntityIdVersionStatus<TableGroupId>> tableGroups;
    std::vector<EntityIdVersionStatus<LoadStrategyId>> loadStrategies;

    template <typename IdType>
    void addDiff(const IdType &id, CatalogVersion version, const proto::EntityStatus &status) {
        EntityIdVersionStatus<IdType> changedId(id, version, status);
        if constexpr (std::is_same_v<IdType, CatalogId>) {
            catalog = std::make_unique<EntityIdVersionStatus<CatalogId>>();
            *catalog = changedId;
        } else if constexpr (std::is_same_v<IdType, DatabaseId>) {
            databases.emplace_back(std::move(changedId));
        } else if constexpr (std::is_same_v<IdType, TableId>) {
            tables.emplace_back(std::move(changedId));
        } else if constexpr (std::is_same_v<IdType, PartitionId>) {
            partitions.emplace_back(std::move(changedId));
        } else if constexpr (std::is_same_v<IdType, TableStructureId>) {
            tableStructures.emplace_back(std::move(changedId));
        } else if constexpr (std::is_same_v<IdType, FunctionId>) {
            functions.emplace_back(std::move(changedId));
        } else if constexpr (std::is_same_v<IdType, TableGroupId>) {
            tableGroups.emplace_back(std::move(changedId));
        } else if constexpr (std::is_same_v<IdType, LoadStrategyId>) {
            loadStrategies.emplace_back(std::move(changedId));
        } else {
        }
    }

    bool hasDiff() const {
        return catalog != nullptr || !databases.empty() || !tables.empty() || !partitions.empty() ||
               !tableStructures.empty() || !functions.empty() || !tableGroups.empty() || !loadStrategies.empty();
    }
};

} // namespace catalog

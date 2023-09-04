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

#include <string>

#include "catalog/proto/CatalogEntity.pb.h"

namespace catalog {

using CatalogId = std::string;

struct DatabaseId {
    std::string databaseName;
    std::string catalogName;
};

struct TableId {
    std::string tableName;
    std::string databaseName;
    std::string catalogName;
};

struct TableGroupId {
    std::string tableGroupName;
    std::string databaseName;
    std::string catalogName;
};

struct FunctionId {
    std::string functionName;
    std::string databaseName;
    std::string catalogName;
};

struct TableStructureId {
    std::string tableName;
    std::string databaseName;
    std::string catalogName;
};

struct PartitionId {
    std::string partitionName;
    std::string tableName;
    std::string databaseName;
    std::string catalogName;
    proto::PartitionType::Code partitionType;

    bool operator<(const PartitionId &other) const {
        return std::tie(partitionName, tableName, databaseName, catalogName, partitionType) <
               std::tie(other.partitionName, other.tableName, other.databaseName, other.catalogName, partitionType);
    }
};

struct LoadStrategyId {
    std::string tableName;
    std::string tableGroupName;
    std::string databaseName;
    std::string catalogName;
};

} // namespace catalog

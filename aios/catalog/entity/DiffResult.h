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

#include "catalog/entity/Type.h"

namespace catalog {

struct DiffResult {
    std::unique_ptr<proto::CatalogRecord> catalog;
    std::vector<std::unique_ptr<proto::DatabaseRecord>> databases;
    std::vector<std::unique_ptr<proto::TableRecord>> tables;
    std::vector<std::unique_ptr<proto::PartitionRecord>> partitions;
    std::vector<std::unique_ptr<proto::TableStructureRecord>> tableStructures;
    std::vector<std::unique_ptr<proto::FunctionRecord>> functions;
    std::vector<std::unique_ptr<proto::TableGroupRecord>> tableGroups;
    std::vector<std::unique_ptr<proto::LoadStrategyRecord>> loadStrategies;

    bool hasDiff() const {
        return catalog != nullptr || !databases.empty() || !tables.empty() || !partitions.empty() ||
               !tableStructures.empty() || !functions.empty() || !tableGroups.empty() || !loadStrategies.empty();
    }
};

} // namespace catalog

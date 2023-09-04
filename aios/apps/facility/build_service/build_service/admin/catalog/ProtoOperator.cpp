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
#include "build_service/admin/catalog/ProtoOperator.h"

namespace catalog::proto {

bool operator<(const catalog::proto::BuildId& lhs, const catalog::proto::BuildId& rhs)
{
    if (lhs.catalog_name() != rhs.catalog_name()) {
        return lhs.catalog_name() < rhs.catalog_name();
    }
    if (lhs.database_name() != rhs.database_name()) {
        return lhs.database_name() < rhs.database_name();
    }
    if (lhs.table_name() != rhs.table_name()) {
        return lhs.table_name() < rhs.table_name();
    }
    if (lhs.partition_name() != rhs.partition_name()) {
        return lhs.partition_name() < rhs.partition_name();
    }
    return lhs.generation_id() < rhs.generation_id();
}

bool operator==(const BuildId& lhs, const BuildId& rhs)
{
    return lhs.generation_id() == rhs.generation_id() && lhs.partition_name() == rhs.partition_name() &&
           lhs.table_name() == rhs.table_name() && lhs.database_name() == rhs.database_name() &&
           lhs.catalog_name() == rhs.catalog_name();
}
bool operator==(const BuildTarget& lhs, const BuildTarget& rhs)
{
    return lhs.type() == rhs.type() && lhs.build_state() == rhs.build_state() && lhs.config_path() == rhs.config_path();
}

} // namespace catalog::proto

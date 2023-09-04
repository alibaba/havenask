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
#include "build_service/admin/catalog/CatalogPartitionIdentifier.h"

namespace build_service::admin {
BS_LOG_SETUP(admin, CatalogPartitionIdentifier);

CatalogPartitionIdentifier::CatalogPartitionIdentifier(const std::string& partitionName, const std::string& tableName,
                                                       const std::string& databaseName, const std::string& catalogName)
    : _partitionName(partitionName)
    , _tableName(tableName)
    , _databaseName(databaseName)
    , _catalogName(catalogName)
{
}

CatalogPartitionIdentifier::CatalogPartitionIdentifier(std::string&& partitionName, std::string&& tableName,
                                                       std::string&& databaseName, std::string&& catalogName)
    : _partitionName(partitionName)
    , _tableName(tableName)
    , _databaseName(databaseName)
    , _catalogName(catalogName)
{
}

void CatalogPartitionIdentifier::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("partition_name", _partitionName, _partitionName);
    json.Jsonize("table_name", _tableName, _tableName);
    json.Jsonize("database_name", _databaseName, _databaseName);
    json.Jsonize("catalog_name", _catalogName, _catalogName);
}

bool CatalogPartitionIdentifier::operator<(const CatalogPartitionIdentifier& other) const
{
    if (_catalogName != other._catalogName) {
        return _catalogName < other._catalogName;
    }
    if (_databaseName != other._databaseName) {
        return _databaseName < other._databaseName;
    }
    if (_tableName != other._tableName) {
        return _tableName < other._tableName;
    }
    return _partitionName < other._partitionName;
}

bool CatalogPartitionIdentifier::operator==(const CatalogPartitionIdentifier& other) const
{
    return _catalogName == other._catalogName && _databaseName == other._databaseName &&
           _tableName == other._tableName && _partitionName == other._partitionName;
}

} // namespace build_service::admin

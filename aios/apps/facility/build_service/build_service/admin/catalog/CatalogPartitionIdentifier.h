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

#include "autil/legacy/jsonizable.h"
#include "build_service/util/Log.h"

namespace build_service::admin {

class CatalogPartitionIdentifier : public autil::legacy::Jsonizable
{
public:
    CatalogPartitionIdentifier() = default;
    CatalogPartitionIdentifier(const std::string& partitionName, const std::string& tableName,
                               const std::string& databaseName, const std::string& catalogName);
    CatalogPartitionIdentifier(std::string&& partitionName, std::string&& tableName, std::string&& databaseName,
                               std::string&& catalogName);
    ~CatalogPartitionIdentifier() = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    const std::string& getPartitionName() const { return _partitionName; }
    const std::string& getTableName() const { return _tableName; }
    const std::string& getDatabaseName() const { return _databaseName; }
    const std::string& getCatalogName() const { return _catalogName; }

public:
    bool operator<(const CatalogPartitionIdentifier& other) const;
    bool operator==(const CatalogPartitionIdentifier& other) const;

private:
    std::string _partitionName; // catalog partition ~> bs generation
    std::string _tableName;     // catalog table ~> bs cluster: mainse_summary
    std::string _databaseName;  // catalog database ~> bs data table: mainse
    std::string _catalogName;   // catalog ~> biz

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::admin

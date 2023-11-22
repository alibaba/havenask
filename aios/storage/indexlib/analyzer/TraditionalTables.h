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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlibv2 { namespace analyzer {

class TraditionalTable : public autil::legacy::Jsonizable
{
public:
    TraditionalTable() = default;
    ~TraditionalTable() = default;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    std::string tableName;
    std::map<uint16_t, uint16_t> table;

private:
    static bool hexToUint16(const std::string& str, uint16_t& value);
};

class TraditionalTables : public autil::legacy::Jsonizable
{
public:
    TraditionalTables() = default;
    ~TraditionalTables() = default;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    const TraditionalTable* getTraditionalTable(const std::string& tableName) const;
    void addTraditionalTable(const TraditionalTable& table);

private:
    std::vector<TraditionalTable> _tables;

    AUTIL_LOG_DECLARE();
};

using TraditionalTablesPtr = std::shared_ptr<TraditionalTables>;

}} // namespace indexlibv2::analyzer

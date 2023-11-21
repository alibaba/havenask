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

#include "navi/engine/Data.h"
#include "navi/engine/Type.h"
#include "sql/proto/SqlQueryConfig.pb.h"

namespace sql {

class SqlQueryConfigType : public navi::Type {
public:
    SqlQueryConfigType();
    ~SqlQueryConfigType();
    SqlQueryConfigType(const SqlQueryConfigType &) = delete;
    SqlQueryConfigType &operator=(const SqlQueryConfigType &) = delete;

public:
    navi::TypeErrorCode serialize(navi::TypeContext &ctx, const navi::DataPtr &data) const override;
    navi::TypeErrorCode deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const override;

private:
    static constexpr size_t SQL_QUERY_CONFIG_DATA_CHECKSUM_BEGIN = 0xBE000001;
    static constexpr size_t SQL_QUERY_CONFIG_DATA_CHECKSUM_END = 0xED000001;

public:
    static const std::string TYPE_ID;
};

class SqlQueryConfigData : public navi::Data {
public:
    SqlQueryConfigData();
    SqlQueryConfigData(SqlQueryConfig config);
    ~SqlQueryConfigData();
    SqlQueryConfigData(const SqlQueryConfigData &) = delete;
    SqlQueryConfigData &operator=(const SqlQueryConfigData &) = delete;

public:
    const SqlQueryConfig &getConfig() const;

private:
    SqlQueryConfig _config;
};

NAVI_TYPEDEF_PTR(SqlQueryConfigData);

} // namespace sql

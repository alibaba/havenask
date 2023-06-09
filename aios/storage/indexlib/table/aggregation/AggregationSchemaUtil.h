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

#include "indexlib/base/Status.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlibv2::table {

class AggregationSchemaUtil
{
public:
    template <typename SchemaType>
    static StatusOr<std::string> GetPrimaryKeyIndexName(const SchemaType* schema)
    {
        auto [s, pkIndexName] = schema->template GetSetting<std::string>("primary_key_index_name");
        if (!s.IsOK() && !s.IsNotFound()) {
            return {s.steal_error()};
        } else if (s.IsOK()) {
            return {std::move(pkIndexName)};
        } else {
            auto indexConfigs = schema->GetIndexConfigs(index::KV_INDEX_TYPE_STR);
            if (indexConfigs.size() == 0) {
                return {Status::NotFound()};
            } else {
                return {indexConfigs[0]->GetIndexName()};
            }
        }
    }

    template <typename SchemaType>
    static StatusOr<bool> IsUniqueEnabled(const SchemaType* schema)
    {
        auto [s, enableUnique] = schema->template GetSetting<bool>("enable_unique");
        if (!s.IsOK() && !s.IsNotFound()) {
            return {s.steal_error()};
        } else if (s.IsNotFound()) {
            enableUnique = false;
        }
        return {enableUnique};
    }

    template <typename SchemaType>
    static StatusOr<std::string> GetUniqueField(const SchemaType* schema)
    {
        auto [s, uniqueField] = schema->template GetSetting<std::string>("unique_field");
        if (s.IsOK()) {
            return {std::move(uniqueField)};
        } else if (!s.IsNotFound()) {
            return {s.steal_error()};
        } else {
            auto pkIndexConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(schema->GetPrimaryKeyIndexConfig());
            if (!pkIndexConfig) {
                return {Status::NotFound("no unique field specified")};
            }
            return {pkIndexConfig->GetKeyFieldName()};
        }
    }

    template <typename SchemaType>
    static StatusOr<std::string> GetVersionField(const SchemaType* schema)
    {
        auto [s, versionField] = schema->template GetSetting<std::string>("version_field");
        if (s.IsOK()) {
            return {std::move(versionField)};
        }
        return {s.steal_error()};
    }

    template <typename SchemaType>
    static StatusOr<bool> IsAutoGenDeleteKey(const SchemaType* schema)
    {
        auto [s, autoGenDeleteKey] = schema->template GetSetting<bool>("auto_gen_delete_key");
        if (!s.IsOK() && !s.IsNotFound()) {
            return {s.steal_error()};
        } else if (s.IsNotFound()) {
            autoGenDeleteKey = true;
        }
        return {autoGenDeleteKey};
    }
};

} // namespace indexlibv2::table

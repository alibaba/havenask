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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/IDirectory.h"

namespace indexlibv2::config {

struct TruncateIndexNameItem : public autil::legacy::Jsonizable {
public:
    TruncateIndexNameItem();
    TruncateIndexNameItem(const std::string& indexName, const std::vector<std::string>& truncNames);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::string indexName;
    std::vector<std::string> truncateNames;
};

using ItemVector = std::vector<std::shared_ptr<TruncateIndexNameItem>>;
using NameMap = std::map<std::string, size_t>;

class TruncateIndexNameMapper : public autil::legacy::Jsonizable
{
public:
    explicit TruncateIndexNameMapper(const std::shared_ptr<indexlib::file_system::IDirectory>& truncateMetaDir);
    ~TruncateIndexNameMapper();

public:
    Status Load();
    Status Dump(const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool Lookup(const std::string& indexName, std::vector<std::string>& truncNames) const;

private:
    void AddItem(const std::string& indexName, const std::vector<std::string>& truncateIndexNames);
    Status DoDump();
    Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& metaDir);
    std::shared_ptr<TruncateIndexNameItem> Lookup(const std::string& indexName) const;

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _truncateMetaDir;
    ItemVector _truncateNameItems;
    NameMap _nameMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config

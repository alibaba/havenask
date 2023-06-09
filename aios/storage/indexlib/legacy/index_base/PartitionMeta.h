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

#include "autil/legacy/jsonizable.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/file_system/Directory.h"

namespace indexlib::legacy::index_base {

class PartitionMeta : public autil::legacy::Jsonizable
{
public:
    PartitionMeta();
    PartitionMeta(const indexlibv2::config::SortDescriptions& sortDescs);
    ~PartitionMeta();

public:
    const indexlibv2::config::SortDescription& GetSortDescription(size_t idx) const;
    void AddSortDescription(const std::string& sortField, const indexlibv2::config::SortPattern& sortPattern);
    void AddSortDescription(const std::string& sortField, const std::string& sortPatternStr);

    indexlibv2::config::SortPattern GetSortPattern(const std::string& sortField);

    void AssertEqual(const PartitionMeta& other);

    bool HasSortField(const std::string& sortField) const;

    const indexlibv2::config::SortDescriptions& GetSortDescriptions() const { return _sortDescriptions; }

    size_t Size() const { return _sortDescriptions.size(); }

public:
    void Store(const std::string& rootDir, file_system::FenceContext* fenceContext) const;
    void Store(const file_system::DirectoryPtr& directory) const;
    void Load(const std::string& rootDir);
    void Load(const file_system::DirectoryPtr& directory);
    static bool IsExist(const std::string& rootDir);
    static PartitionMeta LoadPartitionMeta(const file_system::DirectoryPtr& rootDirectory);
    static PartitionMeta LoadFromString(const std::string& jsonStr);

public:
    void Jsonize(JsonWrapper& json) override;

private:
    indexlibv2::config::SortDescriptions _sortDescriptions;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::legacy::index_base

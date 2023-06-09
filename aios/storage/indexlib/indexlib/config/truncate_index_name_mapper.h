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
#ifndef __INDEXLIB_TRUNCATE_INDEX_NAME_MAPPER_H
#define __INDEXLIB_TRUNCATE_INDEX_NAME_MAPPER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace config {

struct TruncateIndexNameItem : public autil::legacy::Jsonizable {
public:
    TruncateIndexNameItem();
    TruncateIndexNameItem(const std::string& indexName, const std::vector<std::string>& truncNames);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::string mIndexName;
    std::vector<std::string> mTruncateNames;
};
DEFINE_SHARED_PTR(TruncateIndexNameItem);

////////////////////////////////////////////////////////////////////

class TruncateIndexNameMapper : public autil::legacy::Jsonizable
{
public:
    TruncateIndexNameMapper(const file_system::DirectoryPtr& truncateMetaDir);
    ~TruncateIndexNameMapper();

public:
    void Load();
    void Load(const file_system::DirectoryPtr& metaDir);
    void Dump(const IndexSchemaPtr& indexSchema);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool Lookup(const std::string& indexName, std::vector<std::string>& truncNames) const;

public:
    // public for test
    void AddItem(const std::string& indexName, const std::vector<std::string>& truncateIndexNames);
    void DoDump();

private:
    TruncateIndexNameItemPtr Lookup(const std::string& indexName) const;

private:
    typedef std::vector<TruncateIndexNameItemPtr> ItemVector;
    typedef std::map<std::string, size_t> NameMap;

private:
    file_system::DirectoryPtr mTruncateMetaDir;
    ItemVector mTruncateNameItems;
    NameMap mNameMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexNameMapper);
}} // namespace indexlib::config

#endif //__INDEXLIB_TRUNCATE_INDEX_NAME_MAPPER_H

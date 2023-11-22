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
#include "indexlib/config/index_config.h"
#include "indexlib/config/modify_item.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class IndexSchemaImpl;
typedef std::shared_ptr<IndexSchemaImpl> IndexSchemaImplPtr;
} // namespace indexlib::config
namespace indexlib::file_system {
class Directory;
typedef std::shared_ptr<Directory> DirectoryPtr;
} // namespace indexlib::file_system

namespace indexlib { namespace config {

class IndexSchema : public autil::legacy::Jsonizable
{
private:
    typedef std::vector<IndexConfigPtr> IndexConfigVector;
    typedef std::map<std::string, indexid_t> NameMap;
    typedef std::vector<std::vector<indexid_t>> FlagVector;

public:
    typedef IndexConfigVector::const_iterator Iterator;

public:
    IndexSchema();
    ~IndexSchema() {}

public:
    void AddIndexConfig(const IndexConfigPtr& indexInfo);

    // return NULL if index is is_deleted or not exist
    // return object if index is is_normal or is_disable (maybe not ready)
    IndexConfigPtr GetIndexConfig(const std::string& indexName) const;
    IndexConfigPtr GetIndexConfig(indexid_t id) const;
    bool IsDeleted(indexid_t id) const;

    // return not include indexid which is deleted, but will include disabled index
    const std::vector<indexid_t>& GetIndexIdList(fieldid_t fieldId) const;

    // index count include deleted & disabled index
    size_t GetIndexCount() const;

    // Begin & End include deleted & disabled index
    Iterator Begin() const;
    Iterator End() const;

    // iterator will access to target config object filtered by type
    IndexConfigIteratorPtr CreateIterator(bool needVirtual = false, IndexStatus type = is_normal) const;

    // not include deleted index, include disabled index
    bool IsInIndex(fieldid_t fieldId) const;
    bool AllIndexUpdatableForField(fieldid_t fieldId) const;
    bool AnyIndexUpdatable() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexSchema& other) const;
    void AssertCompatible(const IndexSchema& other) const;

    // API for primary key start
    bool HasPrimaryKeyIndex() const;
    bool HasPrimaryKeyAttribute() const;
    InvertedIndexType GetPrimaryKeyIndexType() const;
    std::string GetPrimaryKeyIndexFieldName() const;
    fieldid_t GetPrimaryKeyIndexFieldId() const;
    const SingleFieldIndexConfigPtr& GetPrimaryKeyIndexConfig() const;
    // API for primary key end
    void LoadTruncateTermInfo(const file_system::DirectoryPtr& metaDir);
    void LoadTruncateTermInfo(const std::string& metaDir);
    void Check();
    void DeleteIndex(const std::string& indexName);
    void DisableIndex(const std::string& indexName);
    void EnableBloomFilterForIndex(const std::string& indexName, uint32_t multipleNum);
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();

    void CollectBaseVersionIndexInfo(ModifyItemVector& indexs) const;

public:
    // for test
    void SetPrimaryKeyIndexConfig(const SingleFieldIndexConfigPtr& config);

private:
    IndexSchemaImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexSchema> IndexSchemaPtr;
//////////////////////////////////////////////////////////////////////////
}} // namespace indexlib::config

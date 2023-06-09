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
#ifndef __INDEXLIB_INDEX_SCHEMA_IMPL_H
#define __INDEXLIB_INDEX_SCHEMA_IMPL_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/modify_item.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace config {

class IndexSchemaImpl : public autil::legacy::Jsonizable
{
private:
    typedef std::vector<std::shared_ptr<IndexConfig>> IndexConfigVector;
    typedef std::map<std::string, indexid_t> NameMap;
    typedef std::vector<std::vector<indexid_t>> FlagVector;

public:
    typedef IndexConfigVector::const_iterator Iterator;

public:
    IndexSchemaImpl() : mPrimaryKeyIndexId(INVALID_FIELDID), mBaseIndexCount(-1), mOnlyAddVirtual(false) {}

    ~IndexSchemaImpl() {}

public:
    void AddIndexConfig(const std::shared_ptr<IndexConfig>& indexInfo);
    IndexConfigPtr GetIndexConfig(const std::string& indexName) const;
    IndexConfigPtr GetIndexConfig(indexid_t id) const;
    bool IsDeleted(indexid_t id) const;

    const std::vector<indexid_t>& GetIndexIdList(fieldid_t fieldId) const;
    size_t GetIndexCount() const { return mIndexConfigs.size() + mVirtualIndexConfigs.size(); }

    Iterator Begin() const { return mIndexConfigs.begin(); }
    Iterator End() const { return mIndexConfigs.end(); }
    IndexConfigIteratorPtr CreateIterator(bool needVirtual, IndexStatus type) const;

    bool IsInIndex(fieldid_t fieldId) const;
    bool AllIndexUpdatableForField(fieldid_t fieldId) const;
    bool AnyIndexUpdatable() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexSchemaImpl& other) const;
    void AssertCompatible(const IndexSchemaImpl& other) const;

    // API for primary key start
    bool HasPrimaryKeyIndex() const { return mPrimaryKeyIndexConfig != NULL; }
    bool HasPrimaryKeyAttribute() const;
    InvertedIndexType GetPrimaryKeyIndexType() const { return mPrimaryKeyIndexConfig->GetInvertedIndexType(); }
    std::string GetPrimaryKeyIndexFieldName() const;
    fieldid_t GetPrimaryKeyIndexFieldId() const { return mPrimaryKeyIndexId; }
    const SingleFieldIndexConfigPtr& GetPrimaryKeyIndexConfig() const { return mPrimaryKeyIndexConfig; }
    // API for primary key end

    void LoadTruncateTermInfo(const file_system::DirectoryPtr& metaDir);
    void LoadTruncateTermInfo(const std::string& metaDir);
    void Check();

    void SetPrimaryKeyIndexConfig(const SingleFieldIndexConfigPtr& config) { mPrimaryKeyIndexConfig = config; }

    void DeleteIndex(const std::string& indexName);
    void DisableIndex(const std::string& indexName);
    void EnableBloomFilterForIndex(const std::string& indexName, uint32_t multipleNum);
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();

    void CollectBaseVersionIndexInfo(ModifyItemVector& indexs);

private:
    void AddIndexConfig(const std::shared_ptr<IndexConfig>& indexConfig, NameMap& nameMap,
                        IndexConfigVector& intoxicants);

    void SetExistFlag(const std::shared_ptr<IndexConfig>& indexConfig);
    void SetExistFlag(fieldid_t fieldId, indexid_t indexId);

    void SetNonExistFlag(const IndexConfigPtr& indexConfig);
    void SetNonExistFlag(fieldid_t fieldId, indexid_t indexId);

    void CheckTireIndex();
    void CheckIndexNameNotSummary(const IndexConfigPtr& indexConfig) const;
    void ResetVirtualIndexId();

private:
    IndexConfigVector mIndexConfigs;
    IndexConfigVector mVirtualIndexConfigs;
    NameMap mIndexName2IdMap;
    NameMap mVirtualIndexname2IdMap;

    // for IsInIndex
    FlagVector mFlagVector;

    // for primarykey index
    SingleFieldIndexConfigPtr mPrimaryKeyIndexConfig;
    fieldid_t mPrimaryKeyIndexId;
    int32_t mBaseIndexCount;
    bool mOnlyAddVirtual;

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<IndexSchemaImpl> IndexSchemaImplPtr;
//////////////////////////////////////////////////////////////////////////
}} // namespace indexlib::config

#endif //__INDEXLIB_INDEX_SCHEMA_IMPL_H

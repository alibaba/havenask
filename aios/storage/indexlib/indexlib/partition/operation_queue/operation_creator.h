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
#ifndef __INDEXLIB_OPERATION_CREATOR_H
#define __INDEXLIB_OPERATION_CREATOR_H

#include <memory>

#include "autil/LongHashValue.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/primary_key/PrimaryKeyHashConvertor.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/util/HashString.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

namespace indexlib { namespace partition {

class OperationCreator
{
public:
    OperationCreator(const config::IndexPartitionSchemaPtr& schema) : mSchema(schema)
    {
        const config::IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        config::PrimaryKeyIndexConfigPtr pkConfig =
            DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        if (pkConfig) {
            config::FieldConfigPtr fieldConfig = pkConfig->GetFieldConfig();
            _fieldType = fieldConfig->GetFieldType();
            _primaryKeyHashType = pkConfig->GetPrimaryKeyHashType();
        }
    }
    virtual ~OperationCreator() {}

public:
    // doc->HasPrimaryKey() must be true
    virtual bool Create(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                        OperationBase** operation) = 0;

public:
    // for test
    InvertedIndexType GetPkIndexType() const
    {
        assert(mSchema->GetIndexSchema()->HasPrimaryKeyIndex());
        InvertedIndexType pkIndexType = mSchema->GetIndexSchema()->GetPrimaryKeyIndexType();
        assert(pkIndexType == it_primarykey64 || pkIndexType == it_primarykey128);
        return pkIndexType;
    }

protected:
    void GetPkHash(const document::IndexDocumentPtr& indexDoc, autil::uint128_t& pkHash);

protected:
    config::IndexPartitionSchemaPtr mSchema;
    FieldType _fieldType;
    index::PrimaryKeyHashType _primaryKeyHashType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationCreator);

////////////////////////////////////////////////////////
inline void OperationCreator::GetPkHash(const document::IndexDocumentPtr& indexDoc, autil::uint128_t& pkHash)
{
    assert(indexDoc);
    assert(!indexDoc->GetPrimaryKey().empty());
    const std::string& pkString = indexDoc->GetPrimaryKey();

    InvertedIndexType pkIndexType = GetPkIndexType();
    if (pkIndexType == it_primarykey64) {
        uint64_t hashValue;
        index::KeyHasherWrapper::GetHashKey(_fieldType, _primaryKeyHashType, pkString.c_str(), pkString.size(),
                                            hashValue);
        index::PrimaryKeyHashConvertor::ToUInt128(hashValue, pkHash);
    } else {
        index::KeyHasherWrapper::GetHashKey(pkString.c_str(), pkString.size(), pkHash);
    }
}
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPERATION_CREATOR_H

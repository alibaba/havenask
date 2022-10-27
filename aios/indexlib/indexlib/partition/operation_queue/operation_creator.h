#ifndef __INDEXLIB_OPERATION_CREATOR_H
#define __INDEXLIB_OPERATION_CREATOR_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include <autil/LongHashValue.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/hash_string.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/primary_key_hash_convertor.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/document/index_document/normal_document/index_document.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(partition);

class OperationCreator
{
public:
    OperationCreator(const config::IndexPartitionSchemaPtr& schema)
        : mSchema(schema)
    {
        const config::IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        config::PrimaryKeyIndexConfigPtr pkConfig = 
            DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        if (pkConfig)
        {
            config::FieldConfigPtr fieldConfig = pkConfig->GetFieldConfig();
            mHashFunc.reset(util::KeyHasherFactory::CreatePrimaryKeyHasher(
                                fieldConfig->GetFieldType(),
                                pkConfig->GetPrimaryKeyHashType()));
        }

    }
    virtual ~OperationCreator() {}

public:
    // doc->HasPrimaryKey() must be true
    virtual OperationBase* Create(
            const document::NormalDocumentPtr& doc,
            autil::mem_pool::Pool *pool) = 0;

public:
    //for test
    IndexType GetPkIndexType() const
    {
        assert(mSchema->GetIndexSchema()->HasPrimaryKeyIndex());
        IndexType pkIndexType = mSchema->GetIndexSchema()->GetPrimaryKeyIndexType();
        assert(pkIndexType == it_primarykey64 || pkIndexType == it_primarykey128);
        return pkIndexType;
    }
    
protected:
    void GetPkHash(const document::IndexDocumentPtr& indexDoc,
                   autil::uint128_t &pkHash);

protected:
    config::IndexPartitionSchemaPtr mSchema;
    util::KeyHasherPtr mHashFunc;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationCreator);

////////////////////////////////////////////////////////
inline void OperationCreator::GetPkHash(
        const document::IndexDocumentPtr& indexDoc,
        autil::uint128_t &pkHash)
{
    assert(indexDoc);
    assert(!indexDoc->GetPrimaryKey().empty());
    assert(mHashFunc);
    const std::string &pkString = indexDoc->GetPrimaryKey();

    IndexType pkIndexType = GetPkIndexType();
    if (pkIndexType == it_primarykey64)
    {
        uint64_t hashValue;
        mHashFunc->GetHashKey(pkString.c_str(), pkString.size(), hashValue);
        index::PrimaryKeyHashConvertor::ToUInt128(hashValue, pkHash);
    } 
    else 
    {
        mHashFunc->GetHashKey(pkString.c_str(), pkString.size(), pkHash);
    }
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_CREATOR_H

#include "indexlib/partition/join_cache/join_cache_initializer.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, JoinCacheInitializer);

JoinCacheInitializer::JoinCacheInitializer() 
{
}

JoinCacheInitializer::~JoinCacheInitializer() 
{
}

void JoinCacheInitializer::Init(
        const AttributeReaderPtr& attrReader, 
        const IndexPartitionReaderPtr auxReader)
{
    assert(attrReader);
    assert(auxReader);
    
    mJoinFieldAttrReader = attrReader;
    mAuxPartitionReader = auxReader;
    mPkIndexReader = mAuxPartitionReader->GetPrimaryKeyReader();
    assert(mPkIndexReader);
}

bool JoinCacheInitializer::GetInitValue(
        docid_t docId, char* buffer, size_t bufLen) const
{
    docid_t joinDocId = GetJoinDocId(docId);
    assert(bufLen >= sizeof(docid_t));
    docid_t &value = *((docid_t*)buffer);
    value = joinDocId;
    return true;
}

bool JoinCacheInitializer::GetInitValue(
        docid_t docId, ConstString& value, Pool *memPool) const
{
    docid_t joinDocId = GetJoinDocId(docId);
    value = SingleValueAttributeConvertor<docid_t>::EncodeValue(
            joinDocId, memPool);
    return true;
}

docid_t JoinCacheInitializer::GetJoinDocId(docid_t docId) const
{
    string joinFieldValue;
    assert(mJoinFieldAttrReader);
    if (!mJoinFieldAttrReader->Read(docId, joinFieldValue, NULL))
    {
        IE_LOG(ERROR, "Read join field value fail!");
        return INVALID_DOCID;
    }

    assert(mPkIndexReader);
    return mPkIndexReader->Lookup(joinFieldValue);
}

IE_NAMESPACE_END(partition);


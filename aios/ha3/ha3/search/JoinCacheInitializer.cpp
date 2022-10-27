#include <ha3/search/JoinCacheInitializer.h>
#include <indexlib/index/attribute/single_value_attribute_convertor.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, JoinCacheInitializer);

JoinCacheInitializer::JoinCacheInitializer() 
{
}

JoinCacheInitializer::~JoinCacheInitializer() 
{
}

void JoinCacheInitializer::Init(
        const AttributeReaderPtr& attrReader, 
        IndexPartition* auxPartition)
{
    assert(attrReader);
    assert(auxPartition);
    
    mJoinFieldAttrReader = attrReader;
    mAuxPartitionReader = auxPartition->GetReader();
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
    if (!mJoinFieldAttrReader->Read(docId, joinFieldValue))
    {
        IE_LOG(ERROR, "Read join field value fail!");
        return INVALID_DOCID;
    }

    assert(mPkIndexReader);
    return mPkIndexReader->Lookup(joinFieldValue);
}

IE_NAMESPACE_END(index);


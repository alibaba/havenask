#include <autil/MultiValueType.h>
#include "indexlib/partition/join_cache/join_docid_reader_creator.h"
#include "indexlib/partition/join_cache/join_cache_initializer_creator.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/partition/join_cache/join_docid_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, JoinDocidReaderCreator);

JoinDocidReaderCreator::JoinDocidReaderCreator() 
{
}

JoinDocidReaderCreator::~JoinDocidReaderCreator() 
{
}

JoinDocidReaderBase* JoinDocidReaderCreator::Create(
        const IndexPartitionReaderPtr& mainIndexPartReader,
        const IndexPartitionReaderPtr& joinIndexPartReader,
        const JoinField& joinField,
        uint64_t auxPartitionIdentifier,
        bool isSubJoin,
        mem_pool::Pool* pool)
{
    string joinFieldName = joinField.fieldName;
    AttributeReaderPtr attributeReader = 
        mainIndexPartReader->GetAttributeReader(joinField.fieldName);
    if (!attributeReader)
    {
        IE_LOG(WARN, "get attribute reader for [%s] failed!", 
                joinFieldName.c_str());
        return NULL;
    }
    
    IndexReaderPtr pkIndexReaderPtr = 
        joinIndexPartReader->GetPrimaryKeyReader();
    if (!pkIndexReaderPtr)
    {
        IE_LOG(WARN, "get primary key index reader failed!");
        return NULL;
    }
    JoinDocidCacheIterator *docIdCacheIter = NULL;
    if (joinField.genJoinCache)
    {
        string virtualAttributeName = JoinCacheInitializerCreator::GenerateVirtualAttributeName(
                isSubJoin ? BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX
                : BUILD_IN_JOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX,
                auxPartitionIdentifier, joinField.fieldName, joinIndexPartReader->GetIncVersionId());
        docIdCacheIter = createJoinDocIdCacheIterator(mainIndexPartReader,
                virtualAttributeName, pool);
        if (docIdCacheIter == NULL)
        {
            IE_LOG(WARN, "get join cache failed");
            return NULL;
        }
    }
    
    const DeletionMapReaderPtr &delMapReader = 
        joinIndexPartReader->GetDeletionMapReader();

    AttrType attrType = attributeReader->GetType();
    assert(!attributeReader->IsMultiValue());
    
    JoinDocidReaderBase* joinDocidReader = NULL;
#define CREATE_JOIN_DOCID_READER_TYPE(attr_type)                        \
    case attr_type:                                                     \
    {                                                                   \
        typedef AttrTypeTraits<attr_type>::AttrItemType JoinAttrType;         \
        joinDocidReader = createJoinDocidReaderTyped<JoinAttrType>(     \
                attributeReader, pkIndexReaderPtr,                   \
                docIdCacheIter, delMapReader, joinFieldName,            \
                isSubJoin,  pool);                                      \
    }                                                                   \
    break

    switch (attrType) {
        NUMBER_ATTRIBUTE_MACRO_HELPER(CREATE_JOIN_DOCID_READER_TYPE);
        CREATE_JOIN_DOCID_READER_TYPE(AT_STRING);
    default:
        IE_LOG(WARN, "unsupport type[%d] for join field[%s]", (int32_t)attrType,
                joinFieldName.c_str());
        break;
    }
#undef CREATE_JOIN_DOCID_READER_TYPE    
    return joinDocidReader;
}

JoinDocidReaderCreator::JoinDocidCacheIterator* JoinDocidReaderCreator::createJoinDocIdCacheIterator(
        const IndexPartitionReaderPtr &indexPartReader, 
        const string &joinAttrName,
        mem_pool::Pool* pool)
{
    const AttributeReaderPtr &joinDocIdCacheReader = 
        indexPartReader->GetAttributeReader(joinAttrName);
    if (!joinDocIdCacheReader) {
        return NULL;
    }
    AttributeIteratorBase *iterBase = 
        joinDocIdCacheReader->CreateIterator(pool);
    if (!iterBase) {
        return NULL;
    }
    JoinDocidCacheIterator* docIdCacheIter = 
        dynamic_cast<JoinDocidCacheIterator*>(iterBase);
    if (!docIdCacheIter) {
        POOL_DELETE_CLASS(iterBase);
        return NULL;
    }
    return docIdCacheIter;
}

template<typename JoinAttrType>
JoinDocidReaderBase* JoinDocidReaderCreator::createJoinDocidReaderTyped(
        const AttributeReaderPtr &attrReaderPtr,
        const IndexReaderPtr &pkIndexReaderPtr,
        JoinDocidCacheIterator *docIdCacheIter, 
        const DeletionMapReaderPtr &delMapReader,
        const string &joinAttrName,
        bool isSubJoin,
        mem_pool::Pool* pool) 
{
#define JOIN_DOCID_READER_HELPER(pk_type)                                             \
    typedef JoinDocidReader<JoinAttrType, pk_type> JoinDocidReader;                      \
    typedef typename JoinDocidReader::JoinAttributeIterator JoinAttributeIterator;       \
    typedef typename JoinDocidReader::PrimaryKeyIndexReader PrimaryKeyIndexReader;       \
    typedef typename JoinDocidReader::PrimaryKeyIndexReaderPtr PrimaryKeyIndexReaderPtr; \
    JoinAttributeIterator* attrIter =                                                    \
            dynamic_cast<JoinAttributeIterator*>(attrReaderPtr->CreateIterator(pool));   \
    PrimaryKeyIndexReaderPtr pkIndexReaderTypedPtr =                                     \
            tr1::dynamic_pointer_cast<PrimaryKeyIndexReader>(pkIndexReaderPtr);          \
    if (!attrIter || !pkIndexReaderTypedPtr) {                                           \
        return NULL;                                                                     \
    }                                                                                    \
    JoinDocidReaderBase* reader = POOL_NEW_CLASS(pool, JoinDocidReader,                  \
            joinAttrName, docIdCacheIter, attrIter, pkIndexReaderTypedPtr,               \
            delMapReader);                                                               \
    return reader;

    IndexType indexType = pkIndexReaderPtr->GetIndexType();
    assert(indexType == it_primarykey64 || indexType == it_primarykey128);

    if (indexType == it_primarykey64) {
        JOIN_DOCID_READER_HELPER(uint64_t);
    } else {
        JOIN_DOCID_READER_HELPER(uint128_t);
    }
#undef JOIN_DOCID_READER_HELPER
}


IE_NAMESPACE_END(partition);


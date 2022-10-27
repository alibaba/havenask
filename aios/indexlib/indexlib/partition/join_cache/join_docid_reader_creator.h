#ifndef __INDEXLIB_JOIN_DOCID_READER_CREATOR_H
#define __INDEXLIB_JOIN_DOCID_READER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/join_cache/join_docid_reader_base.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"

IE_NAMESPACE_BEGIN(partition);

class JoinDocidReaderCreator
{
public:
    typedef index::AttributeIteratorTyped<docid_t> JoinDocidCacheIterator;
    JoinDocidReaderCreator();
    ~JoinDocidReaderCreator();
public:
    static JoinDocidReaderBase* Create(
        const IndexPartitionReaderPtr& mainIndexPartReader,
        const IndexPartitionReaderPtr& joinIndexPartReader,
        const JoinField& joinField,
        uint64_t auxPartitionIdentifier,
        bool isSubJoin,
        autil::mem_pool::Pool* pool);
private:
    template<typename JoinAttrType>
    static JoinDocidReaderBase* createJoinDocidReaderTyped(
            const index::AttributeReaderPtr &attrReaderPtr,
            const index::IndexReaderPtr &pkIndexReaderPtr,
            JoinDocidCacheIterator *docIdCacheIter, 
            const index::DeletionMapReaderPtr &delMapReader,
            const std::string &joinAttrName,
            bool isSubJoin,
            autil::mem_pool::Pool* pool);

    static JoinDocidCacheIterator* createJoinDocIdCacheIterator(
            const IndexPartitionReaderPtr &indexPartReader, 
            const std::string &joinAttrName,
            autil::mem_pool::Pool* pool);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinDocidReaderCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_JOIN_DOCID_READER_CREATOR_H

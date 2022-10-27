#ifndef __INDEXLIB_JOIN_CACHE_INITIALIZER_H
#define __INDEXLIB_JOIN_CACHE_INITIALIZER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer.h"
#include "indexlib/partition/index_partition.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);

IE_NAMESPACE_BEGIN(partition);

class JoinCacheInitializer : public common::AttributeValueInitializer
{
public:
    JoinCacheInitializer();
    ~JoinCacheInitializer();

public:
    void Init(const index::AttributeReaderPtr& attrReader,
              const IndexPartitionReaderPtr auxReader);

    /* override*/ bool GetInitValue(
            docid_t docId, char* buffer, size_t bufLen) const;

    /* override*/ bool GetInitValue(
            docid_t docId, autil::ConstString& value, 
            autil::mem_pool::Pool *memPool) const;
private:
    docid_t GetJoinDocId(docid_t docId) const;

private:
    index::AttributeReaderPtr mJoinFieldAttrReader;
    IndexPartitionReaderPtr mAuxPartitionReader;
    index::PrimaryKeyIndexReaderPtr mPkIndexReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinCacheInitializer);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_JOIN_CACHE_INITIALIZER_H

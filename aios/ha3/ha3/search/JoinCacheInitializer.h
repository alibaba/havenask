#ifndef ISEARCH_JOIN_CACHE_INITIALIZER_H
#define ISEARCH_JOIN_CACHE_INITIALIZER_H

#include <tr1/memory>
#include <indexlib/indexlib.h>
#include <indexlib/common/log.h>
#include <indexlib/common_define.h>
#include <indexlib/index/attribute/attribute_value_initializer.h>
#include <indexlib/partition/index_partition.h>

IE_NAMESPACE_BEGIN(index);

class JoinCacheInitializer : public AttributeValueInitializer
{
public:
    JoinCacheInitializer();
    ~JoinCacheInitializer();

public:
    void Init(const AttributeReaderPtr& attrReader, 
              IndexPartition* auxPartition);

    bool GetInitValue(
            docid_t docId, char* buffer, size_t bufLen) const override;

    bool GetInitValue(
            docid_t docId, autil::ConstString& value, 
            autil::mem_pool::Pool *memPool) const override;
private:
    docid_t GetJoinDocId(docid_t docId) const;

private:
    AttributeReaderPtr mJoinFieldAttrReader;
    IndexPartitionReaderPtr mAuxPartitionReader;
    PrimaryKeyIndexReaderPtr mPkIndexReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinCacheInitializer);

IE_NAMESPACE_END(index);

#endif //ISEARCH_JOIN_CACHEINITIALIZER_H

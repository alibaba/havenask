#ifndef __INDEXLIB_JOIN_CACHE_INITIALIZER_CREATOR_H
#define __INDEXLIB_JOIN_CACHE_INITIALIZER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer_creator.h"
#include "indexlib/partition/index_partition.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartition);

IE_NAMESPACE_BEGIN(partition);

class JoinCacheInitializerCreator : public common::AttributeValueInitializerCreator
{
public:
    JoinCacheInitializerCreator();
    ~JoinCacheInitializerCreator();
    
public:
    bool Init(const std::string &joinFieldName,
              bool isSubField,
              config::IndexPartitionSchemaPtr mainPartSchema,
              partition::OnlinePartition* auxPart,
              versionid_t auxReaderIncVersionId);

    /* override*/ common::AttributeValueInitializerPtr Create(
        const index_base::PartitionDataPtr &partitionData);

    // for segment build (local segment data)
    /* override*/ common::AttributeValueInitializerPtr Create(
        const index::InMemorySegmentReaderPtr &inMemSegmentReader);

    static std::string GenerateVirtualAttributeName(
        const std::string& prefixName, uint64_t auxPartitionIdentifier,
        const std::string& joinField, versionid_t vertionid);

    
private:
    config::AttributeConfigPtr mJoinFieldAttrConfig;
    partition::OnlinePartition* mAuxPart;
    versionid_t mAuxReaderIncVersionId;
    std::string mAuxTableName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinCacheInitializerCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_JOIN_CACHE_INITIALIZER_CREATOR_H

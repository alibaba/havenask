#ifndef __INDEXLIB_PARTITION_MODIFIER_CREATOR_H
#define __INDEXLIB_PARTITION_MODIFIER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/modifier/partition_modifier.h"

IE_NAMESPACE_BEGIN(partition);

class PartitionModifierCreator
{
public:
    PartitionModifierCreator();
    ~PartitionModifierCreator();

public:
    static PartitionModifierPtr CreatePatchModifier(
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::PartitionDataPtr& partitionData,
            bool needPk, bool enablePackFile);

    static PartitionModifierPtr CreateInplaceModifier(
            const config::IndexPartitionSchemaPtr& schema,
            const IndexPartitionReaderPtr& reader);

    static PartitionModifierPtr CreateOfflineModifier(
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::PartitionDataPtr& partitionData,
            bool needPk, bool enablePackFile);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionModifierCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_MODIFIER_CREATOR_H

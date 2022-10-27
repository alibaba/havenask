#ifndef __INDEXLIB_OFFLINE_MODIFIER_H
#define __INDEXLIB_OFFLINE_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/modifier/patch_modifier.h"

IE_NAMESPACE_BEGIN(partition);

class OfflineModifier : public PatchModifier
{
public:
    OfflineModifier(const config::IndexPartitionSchemaPtr& schema,
                    bool enablePackFile)
        : PatchModifier(schema, enablePackFile, true)
    {}
    ~OfflineModifier() {}
public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              const util::BuildResourceMetricsPtr& buildResourceMetrics
              = util::BuildResourceMetricsPtr()) override;
    
    bool RemoveDocument(docid_t docId) override;

private:
    index::DeletionMapReaderPtr mDeletionMapReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineModifier);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OFFLINE_MODIFIER_H

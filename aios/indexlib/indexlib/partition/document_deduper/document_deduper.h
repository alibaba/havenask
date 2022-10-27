#ifndef __INDEXLIB_DOCUMENT_DEDUPER_H
#define __INDEXLIB_DOCUMENT_DEDUPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, SingleFieldIndexConfig);

IE_NAMESPACE_BEGIN(partition);

class DocumentDeduper
{
public:
    DocumentDeduper(const config::IndexPartitionSchemaPtr& schema);
    virtual ~DocumentDeduper();
public:
    virtual void Dedup() = 0;

protected:
    static const config::SingleFieldIndexConfigPtr& GetPrimaryKeyIndexConfig(
            const config::IndexPartitionSchemaPtr& partitionSchema);

protected:
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentDeduper);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_DOCUMENT_DEDUPER_H

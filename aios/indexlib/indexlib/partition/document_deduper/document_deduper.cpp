#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, DocumentDeduper);

DocumentDeduper::DocumentDeduper(const IndexPartitionSchemaPtr& schema)
    : mSchema(schema)
{
}

DocumentDeduper::~DocumentDeduper() 
{
}

const SingleFieldIndexConfigPtr& DocumentDeduper::GetPrimaryKeyIndexConfig(
        const IndexPartitionSchemaPtr& partitionSchema)
{
    const IndexSchemaPtr& indexSchema = partitionSchema->GetIndexSchema();
    assert(indexSchema);
    return indexSchema->GetPrimaryKeyIndexConfig();
}

IE_NAMESPACE_END(partition);


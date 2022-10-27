#include "indexlib/common/document_deduper_helper.h"
#include "indexlib/config/primary_key_index_config.h"

using namespace std;

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(common);

DocumentDeduperHelper::DocumentDeduperHelper() 
{
}

DocumentDeduperHelper::~DocumentDeduperHelper() 
{
}

bool DocumentDeduperHelper::DelayDedupDocument(
        const IndexPartitionOptions& options,
        const IndexConfigPtr& pkConfig)
{
    if (!pkConfig || pkConfig->GetIndexType() == it_trie ||
        pkConfig->GetIndexType() == it_kv ||
        pkConfig->GetIndexType() == it_kkv)
    {
        return false;
    }
    PrimaryKeyIndexConfigPtr pkIndexConfig = DYNAMIC_POINTER_CAST(
        PrimaryKeyIndexConfig, pkConfig);
    assert(pkIndexConfig);
    if (pkIndexConfig->GetPrimaryKeyIndexType() == pk_hash_table)
    {
        return false;
    }
    if (!options.IsOffline())
    {
        return false;
    }
    return !options.GetBuildConfig().speedUpPrimaryKeyReader;
}

IE_NAMESPACE_END(common);


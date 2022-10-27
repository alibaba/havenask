#include "indexlib/partition/document_deduper/built_segments_document_deduper.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, BuiltSegmentsDocumentDeduper);

void BuiltSegmentsDocumentDeduper::Dedup()
{
    IE_LOG(INFO, "begin dedup built segments");
    const IndexConfigPtr& indexConfig = GetPrimaryKeyIndexConfig(mSchema);
    assert(indexConfig);
    IndexType type = indexConfig->GetIndexType();
    if (type == it_primarykey64)
    {
        DedupDocuments<uint64_t>();
    }
    if (type == it_primarykey128)
    {
        DedupDocuments<autil::uint128_t>();
    }
    assert(type != it_trie);
    IE_LOG(INFO, "end dedup built segments");
}

IE_NAMESPACE_END(partition);


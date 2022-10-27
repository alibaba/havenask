#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"
#include "indexlib/document/index_document/kv_document/kv_index_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/common/multi_region_rehasher.h"
#include <autil/ConstString.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, KVKeyExtractor);

bool KVKeyExtractor::GetKey(const NormalDocumentPtr& doc, uint64_t& key)
{
    if (mRegionId != INVALID_REGIONID && mRegionId != doc->GetRegionId())
    {
        IE_LOG(ERROR, "region id miss match between doc [%d] and schema [%d]",
               doc->GetRegionId(), mRegionId);
        ERROR_COLLECTOR_LOG(ERROR, "region id miss match between doc [%d] and schema [%d]",
                            doc->GetRegionId(), mRegionId);
        return false;
    }

    KVIndexDocumentPtr kvIndexDocument = doc->GetKVIndexDocument();
    if (kvIndexDocument)
    {
        key = kvIndexDocument->GetPkeyHash();
    }
    else
    {
        // for legacy kv document
        const string& pkStr = doc->GetIndexDocument()->GetPrimaryKey();
        ConstString keyStr(pkStr.c_str(), pkStr.size());
        if (!mKeyHasher->GetHashKey(keyStr, key))
        {
            IE_LOG(WARN, "get hash key[%s] failed", keyStr.c_str());
            ERROR_COLLECTOR_LOG(WARN, "get hash key[%s] failed", keyStr.c_str());
            return false;
        }
    }
    if (mRegionId != INVALID_REGIONID)
    {
        key = MultiRegionRehasher::GetRehashKey(key, mRegionId);
    }
    return true;
}

void KVKeyExtractor::HashKey(const std::string& key, uint64_t &keyHash)
{
    ConstString keyStr(key);
    mKeyHasher->GetHashKey(keyStr, keyHash);
}

IE_NAMESPACE_END(document);

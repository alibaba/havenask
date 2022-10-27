#include "indexlib/document/document_parser/kv_parser/kkv_keys_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/common/multi_region_rehasher.h"
#include "indexlib/config/kkv_index_config.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, KKVKeysExtractor);

KKVKeysExtractor::KKVKeysExtractor(const KKVIndexConfigPtr& kkvConfig)
    : mRegionId(kkvConfig->GetRegionCount() > 1 ? kkvConfig->GetRegionId() : INVALID_REGIONID)
{
    mPrefixKeyHasher.reset(KeyHasherFactory::Create(
                    kkvConfig->GetPrefixFieldConfig()->GetFieldType(), kkvConfig->UseNumberHash()));
    mSuffixKeyHasher.reset(KeyHasherFactory::Create(
                    kkvConfig->GetSuffixFieldConfig()->GetFieldType(), true));
}

KKVKeysExtractor::~KKVKeysExtractor() 
{
}

bool KKVKeysExtractor::AddKVIndexDocForLegacy(const NormalDocumentPtr& doc)
{
    vector<string> keys;
    const string& pkStr = doc->GetIndexDocument()->GetPrimaryKey();
    StringUtil::fromString(pkStr, keys, string(1, MULTI_VALUE_SEPARATOR));
    if (keys.empty() || keys.size() > 2)
    {
        IE_LOG(ERROR, "invalid pk [%s] for kkv", pkStr.c_str());
        return false;
    }

    KVIndexDocumentPtr kvDoc(new KVIndexDocument(doc->GetPool()));
    dictkey_t key = 0;
    ConstString pkeyStr(keys[0]);
    mPrefixKeyHasher->GetHashKey(pkeyStr, key);
    kvDoc->SetPkeyHash(key);
    if (keys.size() == 2)
    {
        ConstString skeyStr(keys[1]);
        mSuffixKeyHasher->GetHashKey(skeyStr, key);
        kvDoc->SetSkeyHash(key);
    }
    doc->SetKVIndexDocument(kvDoc);
    return true;
}

bool KKVKeysExtractor::GetKeys(const NormalDocumentPtr& doc, uint64_t& pkey,
                               uint64_t& skey, OperationType& operationType)
{
    DocOperateType opType = doc->GetOriginalOperateType();
    if (unlikely(opType != ADD_DOC && opType != DELETE_DOC))
    {
        ERROR_COLLECTOR_LOG(ERROR, "unsupported doc type [%d] for kkv table", (int)opType);
        return false;
    }

    if (mRegionId != INVALID_REGIONID && mRegionId != doc->GetRegionId())
    {
        ERROR_COLLECTOR_LOG(ERROR, "region id miss match between doc [%d] and schema [%d]",
                            doc->GetRegionId(), mRegionId);
        return false;
     }
    
    KVIndexDocumentPtr indexDocument = doc->GetKVIndexDocument();
    //legacy code for old document
    if (!indexDocument)
    {
        if (!AddKVIndexDocForLegacy(doc))
        {
            ERROR_COLLECTOR_LOG(ERROR, "add kv index doc for legacy format fail!");
            return false;
        }
        indexDocument = doc->GetKVIndexDocument();
        assert(indexDocument);
    }
    pkey = (uint64_t)indexDocument->GetPkeyHash();
    skey = (uint64_t)indexDocument->GetSkeyHash();

    if (mRegionId != INVALID_REGIONID)
    {
        pkey = MultiRegionRehasher::GetRehashKey(pkey, mRegionId);
    }    

    if (indexDocument->HasSkey())
    {
        operationType = (opType == ADD_DOC) ? ADD : DELETE_SKEY;
        return true;
    }

    if (opType != DELETE_DOC)
    {
        ERROR_COLLECTOR_LOG(ERROR, "add doc with pkey hash [%lu] has no skey",
                            indexDocument->GetPkeyHash());
        return false;
    }
    operationType = DELETE_PKEY;
    return true;
}

bool KKVKeysExtractor::HashPrefixKey(
        const string& pkey, uint64_t &pkeyHash)
{
    ConstString pkeyStr(pkey);
    return mPrefixKeyHasher->GetHashKey(pkeyStr, pkeyHash);
}

bool KKVKeysExtractor::HashSuffixKey(
        const string& suffixKey, uint64_t &suffixKeyHash)
{
    ConstString suffixKeyStr(suffixKey);
    return mSuffixKeyHasher->GetHashKey(suffixKeyStr, suffixKeyHash);
}

IE_NAMESPACE_END(document);


#include "indexlib/document/document_parser/kv_parser/kkv_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kkv_keys_extractor.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, KkvDocumentParser);

KkvDocumentParser::KkvDocumentParser(const IndexPartitionSchemaPtr& schema)
    : KvDocumentParser(schema)
{
}

KkvDocumentParser::~KkvDocumentParser() 
{
}

bool KkvDocumentParser::InitKeyExtractor()
{
    if (mSchema->GetTableType() != tt_kkv)
    {
        ERROR_COLLECTOR_LOG(ERROR, "init KkvDocumentParser fail, "
                            "unsupport tableType [%s]!",
                            IndexPartitionSchema::TableType2Str(
                                    mSchema->GetTableType()).c_str());
        return false;
    }

    mKkvKeyExtractor.reset(new MultiRegionKKVKeysExtractor(mSchema));
    return true;
}

void KkvDocumentParser::SetPrimaryKeyField(
        const IndexlibExtendDocumentPtr &document,
        const IndexSchemaPtr &indexSchema,
        regionid_t regionId, DocOperateType opType)
{
    const string& pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
    if (pkFieldName.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "prefix key field is empty!");
        return;
    }

    const RawDocumentPtr& rawDoc = document->getRawDocument();
    string pkValue = rawDoc->getField(pkFieldName);
    uint64_t pkeyHash = 0;
    mKkvKeyExtractor->HashPrefixKey(pkValue, pkeyHash, regionId);

    KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig,
            indexSchema->GetPrimaryKeyIndexConfig());
    assert(kkvIndexConfig);
    const string& skeyFieldName = kkvIndexConfig->GetSuffixFieldName();

    string skeyValue = rawDoc->getField(skeyFieldName);
    uint64_t skeyHash = 0;
    bool hasSkey = false;
    if (!skeyValue.empty())
    {
        mKkvKeyExtractor->HashSuffixKey(skeyValue, skeyHash, regionId);
        hasSkey = true;
    }

    if (!hasSkey && opType == ADD_DOC)
    {
        ERROR_COLLECTOR_LOG(ERROR, "suffix key field is empty for add doc, "
                            "which prefix key is [%s]!", pkValue.c_str());
        return;
    }

    if (hasSkey)
    {
        document->setIdentifier(pkValue + ":" + skeyValue);
    }
    else
    {
        document->setIdentifier(pkValue);
    }
    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    classifiedDoc->createKVIndexDocument(pkeyHash, skeyHash, hasSkey);
}

IE_NAMESPACE_END(document);


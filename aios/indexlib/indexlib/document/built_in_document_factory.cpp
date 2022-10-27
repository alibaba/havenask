#include "indexlib/document/built_in_document_factory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_parser/normal_parser/normal_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/kkv_document_parser.h"
#include "indexlib/document/document_parser/line_data_parser/line_data_document_parser.h"
#include "indexlib/util/env_util.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, BuiltInDocumentFactory);

const size_t BuiltInDocumentFactory::MAX_KEY_SIZE = 4096;

BuiltInDocumentFactory::BuiltInDocumentFactory()
{
    size_t maxKeySize = EnvUtil::GetEnv("DEFAULT_RAWDOC_MAX_KEY_SIZE",
            BuiltInDocumentFactory::MAX_KEY_SIZE);
    mHashKeyMapManager.reset(new KeyMapManager(maxKeySize));
}

BuiltInDocumentFactory::~BuiltInDocumentFactory() 
{
}

RawDocument* BuiltInDocumentFactory::CreateRawDocument(
        const IndexPartitionSchemaPtr& schema)
{
    return new (std::nothrow) DefaultRawDocument(mHashKeyMapManager);
}

DocumentParser* BuiltInDocumentFactory::CreateDocumentParser(
        const IndexPartitionSchemaPtr& schema)
{
    if (!schema)
    {
        IE_LOG(ERROR, "schema is null!");
        return nullptr;
    }
    
    TableType tableType = schema->GetTableType();
    if (tableType == tt_kv)
    {
        return new (std::nothrow) KvDocumentParser(schema);
    }
    else if (tableType == tt_kkv)
    {
        return new (std::nothrow) KkvDocumentParser(schema);
    }
    else if (tableType == tt_linedata)
    {
        return new (std::nothrow) LineDataDocumentParser(schema);
    }

    if (tableType == tt_customized)
    {
        IE_LOG(WARN, "customized table type not set customized_document_config, "
               "will use NormalDocumentParser by default!");
    }
    return new (std::nothrow) NormalDocumentParser(schema);
}

IE_NAMESPACE_END(document);


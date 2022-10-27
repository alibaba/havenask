#include "indexlib/index/normal/inverted_index/test/multi_sharding_index_writer_unittest.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiShardingIndexWriterTest);

MultiShardingIndexWriterTest::MultiShardingIndexWriterTest()
{
}

MultiShardingIndexWriterTest::~MultiShardingIndexWriterTest()
{
}

void MultiShardingIndexWriterTest::CaseSetUp()
{
}

void MultiShardingIndexWriterTest::CaseTearDown()
{
}

void MultiShardingIndexWriterTest::TestSimpleProcess()
{
    string fields = "name:string;title:text;pk:string";
    string indexs = "pk_index:primarykey64:pk;"
                    "str_index:string:name:false:2;"
                    "pack_index:pack:title:false:2";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(fields, indexs, "", "");

    string docStrs = "cmd=add,pk=0,name=yida,title=staff;"
                     "cmd=add,pk=1,name=pujian,title=abc;"
                     "cmd=add,pk=2,name=pujian,title=staff;"
                     "cmd=add,pk=3,name=shoujing,title=def;"
                     "cmd=add,pk=4,name=yexiao,title=hig;"
                     "cmd=add,pk=4,name=yida,title=abc;";

    vector<NormalDocumentPtr> docs = 
        DocumentCreator::CreateDocuments(schema, docStrs);

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    DirectoryPtr indexDirectory = 
        GET_SEGMENT_DIRECTORY()->MakeDirectory("index");

    PrepareIndex(indexSchema, "str_index", docs, indexDirectory);
    CheckIndex(indexSchema, "str_index", docs, indexDirectory);

    PrepareIndex(indexSchema, "pack_index", docs, indexDirectory);
    CheckIndex(indexSchema, "pack_index", docs, indexDirectory);
}


void MultiShardingIndexWriterTest::PrepareIndex(
        const IndexSchemaPtr& indexSchema, 
        const string& indexName,
        const vector<NormalDocumentPtr>& docs, 
        const DirectoryPtr& indexDirectory)
{
    assert(indexSchema);
    assert(indexDirectory);

    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    assert(indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING);
    assert(!IndexFormatOption::IsNumberIndex(indexConfig));
    SegmentMetricsPtr metrics(new SegmentMetrics);
    metrics->SetDistinctTermCount(indexConfig->GetIndexName(), HASHMAP_INIT_SIZE);
    IndexPartitionOptions options;
    MultiShardingIndexWriterPtr writer(
        new MultiShardingIndexWriter(metrics, options));
    writer->Init(indexConfig, NULL);
    for (size_t i = 0; i < docs.size(); i++)
    {
        const IndexDocumentPtr& indexDoc = docs[i]->GetIndexDocument();
        assert(indexDoc);
        IndexDocument::FieldVector::const_iterator iter = indexDoc->GetFieldBegin();
        IndexDocument::FieldVector::const_iterator iterEnd = indexDoc->GetFieldEnd();

        for (; iter != iterEnd; ++iter)
        {
            const Field* field = *iter;
            if (!field)
            {
                continue;
            }

            fieldid_t fieldId = field->GetFieldId();
            const vector<indexid_t>& indexIds = 
                indexSchema->GetIndexIdList(fieldId);
            vector<indexid_t>::const_iterator iter = 
                find(indexIds.begin(), indexIds.end(), indexConfig->GetIndexId());
            if (iter == indexIds.end())
            {
                continue;
            }

            writer->AddField(field);
        }
        writer->EndDocument(*indexDoc);
    }
    writer->EndSegment();
    writer->Dump(indexDirectory, &mSimplePool);
    indexDirectory->Sync(true);
}

void MultiShardingIndexWriterTest::CheckIndex(
        const IndexSchemaPtr& indexSchema, 
        const string& indexName,
        const vector<NormalDocumentPtr>& docs, 
        const DirectoryPtr& indexDirectory)
{
    assert(indexSchema);
    assert(indexDirectory);

    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    assert(indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING);

    const vector<IndexConfigPtr>& shardingIndexConfigs = 
        indexConfig->GetShardingIndexConfigs();
    assert(!shardingIndexConfigs.empty());

    vector<DictionaryReaderPtr> dictReaders;
    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i)
    {
        DictionaryReaderPtr reader(DictionaryCreator::CreateReader(shardingIndexConfigs[i]));
        reader->Open(indexDirectory->GetDirectory(
                        shardingIndexConfigs[i]->GetIndexName(), true),
                     DICTIONARY_FILE_NAME);
        dictReaders.push_back(reader);
    }
    
    for (size_t i = 0; i < docs.size(); i++)
    {
        const IndexDocumentPtr& indexDoc = docs[i]->GetIndexDocument();
        assert(indexDoc);
        IndexDocument::FieldVector::const_iterator iter = indexDoc->GetFieldBegin();
        IndexDocument::FieldVector::const_iterator iterEnd = indexDoc->GetFieldEnd();

        for (; iter != iterEnd; ++iter)
        {
            const IndexTokenizeField* field = dynamic_cast<IndexTokenizeField*>(*iter);
            if (!field)
            {
                continue;
            }

            fieldid_t fieldId = field->GetFieldId();
            const vector<indexid_t>& indexIds = 
                indexSchema->GetIndexIdList(fieldId);
            vector<indexid_t>::const_iterator iter = 
                find(indexIds.begin(), indexIds.end(), indexConfig->GetIndexId());
            if (iter == indexIds.end())
            {
                continue;
            }
            
            for (auto iterField = field->Begin(); 
                 iterField != field->End(); ++iterField)
            {
                const Section* section = *iterField;
                for (size_t i = 0; i < section->GetTokenCount(); i++)
                {
                    const Token* token = section->GetToken(i);
                    dictkey_t hashKey = token->GetHashKey();
                    CheckToken(dictReaders, hashKey);
                }
            }
        }
    }
}


void MultiShardingIndexWriterTest::CheckToken(
        const vector<DictionaryReaderPtr>& dictReaders,
        dictkey_t tokenKey)
{
    size_t matchCount = 0;
    dictvalue_t dictValue;
    for (size_t i = 0; i < dictReaders.size(); i++)
    {
        if (dictReaders[i]->Lookup(tokenKey, dictValue))
        {
            size_t idx = tokenKey % dictReaders.size();
            ASSERT_EQ(i, idx);
            matchCount++;
        }
    }
    ASSERT_EQ((size_t)1, matchCount);
}
 

IE_NAMESPACE_END(index);


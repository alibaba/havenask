#include "indexlib/index/test/indexlib_exception_perftest.h"
#include "indexlib/storage/exception_trigger.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/common/term.h"
#include "indexlib/common/error_code.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/util/env_util.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/merger/partition_merger_creator.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexlibExceptionPerfTest);

IndexlibExceptionPerfTest::IndexlibExceptionPerfTest()
{
}

IndexlibExceptionPerfTest::~IndexlibExceptionPerfTest()
{
}

void IndexlibExceptionPerfTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    string field = "string:string;text2:text;text:text;number:uint32;pk:uint64;"
                   "ends:uint32;nid:uint32";
    string index = "index_string:string:string;index_pk:primarykey64:pk;"
                   "index_text2:text:text2;index_text:text:text;index_number:number:number";
    string attribute = "string;number;pk;ends;nid";
    string summary = "string;number;pk";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
                            "subpkstr:string;sub_long:uint32;",
                            "sub_pk:primarykey64:subpkstr",
                            "sub_long;", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    SchemaMaker::SetAdaptiveHighFrequenceDictionary(
            "index_text2", "DOC_FREQUENCY#1", hp_both, mSchema);
    TruncateParams param("3:1", "", "index_text=ends:ends#DESC|nid#ASC:::;"
                         "index_text2=ends:ends#DESC|nid#ASC:::");
    TruncateConfigMaker::RewriteSchema(mSchema, param);
    mOptions.GetMergeConfig().truncateOptionConfig = 
        TruncateConfigMaker::MakeConfig(param);
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));
    mFullDocs = "cmd=add,string=hello1,text=text1 text2 text3,"
                "text2=text1 text2 text3,number=1,pk=1,ends=1,nid=1,"
                "sub_pk=1001,sub_long=1001;"
                "cmd=add,string=hello2,text=text1 text2 text3,"
                "text2=text1 text2 text3,number=2,pk=2,ends=2,nid=2,"
                "sub_pk=1002,sub_long=1002;"
                "cmd=add,string=hello3,text=text1 text2 text3,"
                "text2=text1 text2 text3,number=3,pk=3,ends=3,nid=3,"
                "sub_pk=1003,sub_long=1003;"
                "cmd=add,string=hello4,text=text1 text2 text4,"
                "text2=text1 text2 text4,number=4,pk=4,ends=4,nid=4,"
                "sub_pk=1004,sub_long=1004;"
                "cmd=add,string=hello5,text=text1 text2 text5,"
                "text2=text1 text2 text5,number=5,pk=5,ends=5,nid=5,"
                "sub_pk=1005,sub_long=1005;"
                "cmd=delete,pk=1;";

    mIncDocs = "cmd=add,string=hello6,text=text6 text2 text3,"
               "text2=text6 text2 text3,number=6,pk=6,ends=6,nid=6,"
               "sub_pk=2001,sub_long=2001;"
               "cmd=add,string=hello7,text=text7 text2 text3,"
               "text2=text7 text2 text3,number=7,pk=7,ends=7,nid=7,"
               "sub_pk=2002,sub_long=2002;"
               "cmd=add,string=hello8,text=text8 text2 text3,"
               "text2=text8 text2 text3,number=8,pk=8,ends=8,nid=8,"
               "sub_pk=2003,sub_long=2003;"
               "cmd=add,string=hello9,text=text9 text2 text3,"
               "text2=text9 text2 text3,number=9,pk=9,ends=9,nid=9,"
               "sub_pk=2004,sub_long=2004;"
               "cmd=add,string=hello10,text=text10 text2 text3,"
               "text2=text10 text2 text3,number=10,pk=10,ends=10,nid=10,"
               "sub_pk=2005,sub_long=2005;"
               "cmd=delete,pk=2;"
               "cmd=delete,pk=10;";
    mRawDocStr = "{ 1, "                                            \
                 "[title: (token1)],"                               \
                 "[ends: (1)],"                                     \
                 "[userid: (1)],"                                   \
                 "[nid: (1)],"                                      \
                 "[url: (url1)],"                                   \
                 "};"                                               \
                 "{ 2, "                                            \
                 "[title: (token1 token2)],"                        \
                 "[ends: (2)],"                                     \
                 "[userid: (2)],"                                   \
                 "[url: (url2)],"                                   \
                 "[nid: (2)],"                                      \
                 "};"                                               \
                 "{ 3, "                                            \
                 "[title: (token1 token2 token3)],"                 \
                 "[ends: (3)],"                                     \
                 "[userid: (3)],"                                   \
                 "[nid: (3)],"                                      \
                 "[url: (url3)],"                                   \
                 "};"                                               \
                 "{ 4, "                                            \
                 "[title: (token1 token2 token3 token4)],"          \
                 "[ends: (4)],"                                     \
                 "[userid: (4)],"                                   \
                 "[nid: (4)],"                                      \
                 "[url: (url4)],"                                   \
                 "};"                                               \
                 "{ 5, "                                            \
                 "[title: (token1 token2 token3 token4 token5)],"   \
                 "[ends: (5)],"                                     \
                 "[userid: (5)],"                                   \
                 "[nid: (6)],"                                      \
                 "[url: (url5)],"                                   \
                 "};"                                               \
                 "{ 5, "                                            \
                 "[title: (token1 token3 token5)],"                 \
                 "[ends: (5)],"                                     \
                 "[userid: (5)],"                                   \
                 "[nid: (5)],"                                      \
                 "[url: (url6)],"                                   \
                 "};"                                               \
                 "{ 4, "                                            \
                 "[title: (token2 token4)],"                        \
                 "[ends: (4)],"                                     \
                 "[userid: (4)],"                                   \
                 "[nid: (7)],"                                      \
                 "[url: (url7)],"                                   \
                 "};";
}

void IndexlibExceptionPerfTest::CaseTearDown()
{
}

void IndexlibExceptionPerfTest::InnerTestBuild(
        size_t normalIOCount, bool asyncDump, bool asyncIO, bool asyncMerge)
{
    TearDown();
    SetUp();
    storage::FileSystemWrapper::Reset();
    cout << "=======================" << endl;
    cout << "exception begin: " << normalIOCount << endl;
    storage::ExceptionTrigger::InitTrigger(normalIOCount);
    
    OfflineConfig& offlineConfig = mOptions.GetOfflineConfig();
    offlineConfig.buildConfig.maxDocCount = 3;
    if (asyncIO)
    {
        offlineConfig.mergeConfig.mergeIOConfig.enableAsyncWrite = true;
        offlineConfig.mergeConfig.mergeIOConfig.writeBufferSize = 10;
        offlineConfig.mergeConfig.mergeIOConfig.enableAsyncRead = true;
    }

    if (asyncDump)
    {
        offlineConfig.buildConfig.dumpThreadCount = 3;
    }

    if (asyncMerge)
    {
        offlineConfig.mergeConfig.mergeThreadCount = 3;
    }
    
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    bool ret1 = psm.Transfer(BUILD_FULL_NO_REOPEN, mFullDocs, 
                             "", "");    
    if (!ret1)
    {
        storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
        return;
    }
    ASSERT_FALSE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs, 
                              "", ""));
    storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}


void IndexlibExceptionPerfTest::TestSimpleProcess()
{
    for (size_t i = 0; i < 3; i++)
    {
        InnerTestBuild(i, true, true, true);
    }
}

void IndexlibExceptionPerfTest::TestCaseForCreateBucketMapsExceptionAndRecover()
{
    {
        //Test create bucket map exception
        string attrPath = FileSystemWrapper::JoinPath(
                GET_TEST_DATA_PATH(), "segment_0_level_0/attribute/ends/data");
        InnerTestTruncateIOException(attrPath, FileSystemWrapper::MMAP_ERROR);
    }

    {
        //Test create truncate posting writer error
        string attrPath = FileSystemWrapper::JoinPath(
                GET_TEST_DATA_PATH(), "instance_0/segment_1_level_0/index/title_ends/posting");
        InnerTestTruncateIOException(attrPath, FileSystemWrapper::OPEN_ERROR);
    }
    
    {
        //Test file system wrapper with pool
        string attrPath = FileSystemWrapper::JoinPath(
                GET_TEST_DATA_PATH(), "instance_0/segment_1_level_0/index/title/posting");
        InnerTestTruncateIOException(attrPath, FileWrapper::COMMON_FILE_WRITE_ERROR, true);
    }
}

void IndexlibExceptionPerfTest::TestCaseForKKVIOExceptionShutDown()
{
    for (size_t i = 0; i < 10; i++)
    {
        InnerTestCaseForKKVIOExceptionShutDown(i);
    }
}

void IndexlibExceptionPerfTest::InnerTestCaseForKKVIOExceptionShutDown(size_t normalIOCount)
{
    TearDown();
    SetUp();
    
    string field = "pkey:string;skey:int32;value:uint32;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKKVSchema(
            field, "pkey", "skey", "value;skey;");

    string docStrs = "cmd=add,pkey=1,skey=1,value=1;"
                     "cmd=add,pkey=1,skey=2,value=1;";
    std::vector<document::NormalDocumentPtr> docs =
        DocumentCreator::CreateDocuments(schema, docStrs);
    
    mOptions.GetBuildConfig(false).buildTotalMemory = 20;
    mOptions.SetEnablePackageFile(false);
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, schema, mQuotaControl);
    indexBuilder.Init();
    for (size_t i = 0; i < docs.size(); i++)
    {
        indexBuilder.Build(docs[i]);
    }
    indexBuilder.EndIndex();

    storage::ExceptionTrigger::InitTrigger(normalIOCount);
    ASSERT_ANY_THROW(indexBuilder.Merge(mOptions));
    storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}

void IndexlibExceptionPerfTest::TestCaseForIOExceptionShutDown()
{
    TruncateParams param("3:1", "", "title=ends:ends#DESC|nid#ASC:::");
    string expectedDocStr = "{ 5, "                                     \
                            "[title: (token1 token3)],"                 \
                            "[ends: (5)],"                              \
                            "[userid: (5)],"                            \
                            "[nid: (5)],"                               \
                            "};";

    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);
    mOptions.SetEnablePackageFile(false);
    OfflineConfig& offlineConfig = mOptions.GetOfflineConfig();
    offlineConfig.mergeConfig.mergeIOConfig.enableAsyncWrite = true;
    offlineConfig.mergeConfig.mergeIOConfig.writeBufferSize = 10;
    offlineConfig.mergeConfig.mergeIOConfig.enableAsyncRead = true;
    
    // build index
    BuildTruncateIndex(mSchema, mRawDocStr, false);
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl);
    indexBuilder.Init();
    string path = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), "instance_0/segment_1_level_0/index/title_ends/posting");
    FileWrapper::SetError(FileWrapper::WRITE_ERROR, path);
    string metaPath = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), "truncate_meta/title_ends");
    FileWrapper::SetError(FileWrapper::CLOSE_ERROR, metaPath);

    ASSERT_THROW(indexBuilder.Merge(mOptions), misc::ExceptionBase);
    FileWrapper::CleanError();
    FileSystemWrapper::ClearError();
    ASSERT_NO_THROW(indexBuilder.Merge(mOptions));
}

void IndexlibExceptionPerfTest::BuildTruncateIndex(
        const IndexPartitionSchemaPtr& schema, 
        const string& rawDocStr, bool needMerge)
{
    DocumentMaker::DocumentVect docVect;
    DocumentMaker::MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(rawDocStr, schema, 
                                 docVect, mockIndexPart);

    // build index
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, schema, mQuotaControl);
    INDEXLIB_TEST_TRUE(indexBuilder.Init());

    for (size_t i = 0; i < docVect.size(); ++i)
    {
        indexBuilder.BuildDocument(docVect[i]);
    }
    indexBuilder.EndIndex();
    if (needMerge)
    {
        indexBuilder.Merge(mOptions);
    }
}

void IndexlibExceptionPerfTest::InnerTestTruncateIOException(
        const string& path, uint32_t errorType, bool asyncIO)
{
    TearDown();
    SetUp();
    storage::FileSystemWrapper::Reset();
    TruncateParams param("3:1", "", "title=ends:ends#DESC|nid#ASC:::");
    string expectedDocStr = "{ 5, "                                     \
                            "[title: (token1 token3)],"                 \
                            "[ends: (5)],"                              \
                            "[userid: (5)],"                            \
                            "[nid: (5)],"                               \
                            "};";

    ResetIndexPartitionSchema(param);
    mOptions.GetMergeConfig().truncateOptionConfig = TruncateConfigMaker::MakeConfig(param);
    mOptions.SetEnablePackageFile(false);
    if (asyncIO)
    {
        OfflineConfig& offlineConfig = mOptions.GetOfflineConfig();
        offlineConfig.mergeConfig.mergeIOConfig.enableAsyncWrite = true;
        offlineConfig.mergeConfig.mergeIOConfig.writeBufferSize = 10;
        offlineConfig.mergeConfig.mergeIOConfig.enableAsyncRead = true;
    }
    // build index
    BuildTruncateIndex(mSchema, mRawDocStr, false);
    partition::IndexBuilder indexBuilder(mRootDir, mOptions, mSchema, mQuotaControl);
    indexBuilder.Init();
    if (asyncIO)
    {
        FileWrapper::SetError(errorType, path);
    }
    else
    {
        FileSystemWrapper::SetError(errorType, path);
    }
    ASSERT_THROW(indexBuilder.Merge(mOptions), misc::ExceptionBase);
    FileWrapper::CleanError();
    FileSystemWrapper::ClearError();
    storage::FileSystemWrapper::Reset();
    ASSERT_NO_THROW(indexBuilder.Merge(mOptions));
}

void IndexlibExceptionPerfTest::ResetIndexPartitionSchema(const TruncateParams& param)
{
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(
        mSchema,
        // field schema
        "title:text;ends:uint32;userid:uint32;nid:uint32;url:string",
        // index schema
        "title:text:title;pk:primarykey64:url",
        // attribute schema
        "ends;userid;nid",
        // summary schema
        "");
    mSchema->AddDictionaryConfig("title_vol", "token1");
    TruncateConfigMaker::RewriteSchema(mSchema, param);
}

void IndexlibExceptionPerfTest::TestCaseForDocReclaimException()
{
    for (size_t i = 0; i < 20; i++)
    {
        InnerTestForDocReclaimException(i);
    }
}

void IndexlibExceptionPerfTest::InnerTestForDocReclaimException(size_t normalIOCount)
{
    TearDown();
    SetUp();
    storage::FileSystemWrapper::Reset();
    IndexPartitionOptions options;
    string configFilePath = GET_TEST_DATA_PATH() + "doc_reclaim_config_path";
    options.GetOfflineConfig().mergeConfig.docReclaimConfigPath = configFilePath;
    string jsonStr = "[{"
                         "\"reclaim_index\" : \"index\","
                         "\"reclaim_terms\" : [\"2\"]"
                     "},"
                     "{"
                         "\"reclaim_index\" : \"pk_index\","
                         "\"reclaim_terms\" : [\"3\"]"
                     "}]";
    FileSystemWrapper::AtomicStore(configFilePath, jsonStr);
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(GET_TEST_DATA_PATH(), "STRING", SFP_INDEX);
    provider.Build("0,1,2,3#4,5,6#7,8", SFP_OFFLINE);
    PartitionMergerPtr partitionMerger(
            PartitionMergerCreator::CreateSinglePartitionMerger(
                    GET_TEST_DATA_PATH(), options, NULL, ""));
    storage::ExceptionTrigger::InitTrigger(normalIOCount);
    ASSERT_ANY_THROW(partitionMerger->Merge());
    storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
    ASSERT_NO_THROW(partitionMerger.reset(
            PartitionMergerCreator::CreateSinglePartitionMerger(
                    GET_TEST_DATA_PATH(),  options, NULL, "")));
}

void IndexlibExceptionPerfTest::TestIndexBuilderInitFail()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    storage::ExceptionTrigger::InitTrigger(0);
    partition::IndexBuilderPtr indexBuilder(
        new partition::IndexBuilder(GET_TEST_DATA_PATH(), options, 
                    mSchema, mQuotaControl));
    ASSERT_FALSE(indexBuilder->Init());
    ASSERT_FALSE(indexBuilder->Init());
    storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}

void IndexlibExceptionPerfTest::TestSeekWithIOException()
{
    storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
    PartitionStateMachine psm;
    string jsonStr =
    R"(                                                 
{
    "load_config" :
    [
        {
            "file_patterns" : ["_INDEX_"],
            "load_strategy" : "cache",
            "load_strategy_param": {
                "global_cache":true,
                "direct_io":true
            }
        },
        {                                                                   
            "file_patterns" : [".*"],                                  
            "load_strategy" : "mmap",                                  
            "load_strategy_param" : {                                    
                "lock" : false                                           
            }                                                              
        }
    ]                                                                 
}
    )";
    FromJsonString(mOptions.GetOnlineConfig().loadConfigList, jsonStr);
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    //mOptions.TEST_mQuickExit = 
    EnvUtil::SetEnv("RS_BLOCK_CACHE", "cache_size=10;block_size=1024;num_shard_bits=7;use_prefetcher=false;");
    storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    mFullDocs.clear();
    for (size_t i = 0; i < 20000; i ++) {
        mFullDocs += "cmd=add,pk=" + autil::StringUtil::toString(i) + ",string=hello,text=text1 text2 text3,"
                     "text2=text1 text2 text3,number=1,ends=1,nid=1;";
    }
    psm.Transfer(BUILD_FULL, mFullDocs, 
                 "", "");
    IOExceptionType result = IOExceptionType::NO_EXCEPTION;
    auto InnerSeekWithIOException = [this]
                                    (size_t normalIOCount, const string& termStr)->IOExceptionType {
        storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
        PartitionStateMachine psm;
        EXPECT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
        psm.Transfer(QUERY, "", 
                     "string:hello", "");
        auto part = psm.GetIndexPartition();
        storage::ExceptionTrigger::InitTrigger(normalIOCount);
        auto indexReader = part->GetReader()->GetIndexReader();
        common::Term term(termStr, "index_text");
        PostingIterator* it = nullptr;
        try
        {
            it = indexReader->Lookup(term);
        }
        catch (const misc::FileIOException& e)
        {
            storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
            return IOExceptionType::LOOKUP;
        }
        docid_t docId = 0, retDoc = 0;
        common::ErrorCode ec = common::ErrorCode::OK;
        EXPECT_TRUE(it);
        //EXPECT_TRUE(it->GetTermMeta()->GetDocFreq() > 0);
//        cout << "term freq: " << it->GetTermMeta()->GetDocFreq() << endl;
        do
        {
            retDoc = docId;
            ec = it->SeekDocWithErrorCode(retDoc, docId);
        } while (ec == common::ErrorCode::OK && docId != INVALID_DOCID);
        delete it;
        storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
        cout << "return doc: " << retDoc << endl;
        if (ec == common::ErrorCode::OK)
        {
            return IOExceptionType::NO_EXCEPTION;
        }
        return IOExceptionType::SEEK;
    };
    for (size_t i = 0; i < 300; i ++)
    {
        IOExceptionType ret = IOExceptionType::NO_EXCEPTION;
        ASSERT_NO_THROW(ret = InnerSeekWithIOException(i, "text1"));
        if (IOExceptionType::SEEK == ret)
        {
            result = ret;
            break;
        }
        else if (IOExceptionType::LOOKUP == ret)
        {
            cout << "lookup io exception" << endl;
        }
    }
    ASSERT_EQ(IOExceptionType::SEEK, result);
    storage::ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
    EnvUtil::UnsetEnv("RS_BLOCK_CACHE");
}

IE_NAMESPACE_END(index);


#include <autil/ConstString.h>
#include <autil/StringUtil.h>
#include <autil/Thread.h>
#include "indexlib/index/normal/summary/test/summary_reader_intetest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SummaryReaderIntetest);

SummaryReaderIntetest::SummaryReaderIntetest()
{
}

SummaryReaderIntetest::~SummaryReaderIntetest()
{
}

void SummaryReaderIntetest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(false).enablePackageFile = false;
    mOptions.GetBuildConfig(false).keepVersionCount = 10;
    mOptions.GetBuildConfig(true).keepVersionCount = 10;

    mJsonStringHead = R"(
    {
        "attributes": [
            "quantity",
            "provcity",
            "vip"
        ],
        "fields": [
            { "field_name": "quantity", "field_type": "INTEGER" },
            { "field_name": "provcity", "compress_type": "uniq|equal", "field_type": "STRING" },
            { "field_name": "category", "field_type": "INTEGER" },
            { "field_name": "nid", "field_type": "STRING" },
            { "field_name": "zk_time", "field_type": "STRING" },
            { "field_name": "title", "field_type": "STRING" },
            { "field_name": "user", "field_type": "STRING" },
            { "field_name": "user_id", "field_type": "STRING" },
            { "field_name": "vip", "field_type": "STRING" },
            { "field_name": "ends", "field_type": "STRING" },
            { "field_name": "pid", "field_type": "STRING" },
            { "field_name": "nick", "field_type": "STRING" }
        ],
        "indexs": [
            {
                "index_fields": "nid",
                "index_name": "pk",
                "index_type": "PRIMARYKEY64",
                "pk_hash_type": "default_hash",
                "pk_storage_type": "hash_table"
            }
        ],
        "table_name": "mainse_summary",
        "table_type": "normal",
    )";
    mJsonStringTail = R"(
    }
    )";
}

void SummaryReaderIntetest::CaseTearDown()
{
    if (mPsm.GetIndexPartition())
    {
        mPsm.GetIndexPartition()->Close();
    }
}

void SummaryReaderIntetest::TestSimpleProcess()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "string1:string;long1:uint32;", "index1:string:string1;", "", "long1");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,long1=1,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "index1:hello", "long1=1"));
    string rtDoc = "cmd=add,string1=hello,long1=2,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello", "long1=1;long1=2"));
}

void SummaryReaderIntetest::CheckSummary(docid_t docId,
        const vector<summarygroupid_t> groupVec,
        const string& expectStr)
{
    stringstream msg;
    msg << "Check DocId[" << docId << "], Group["
        << StringUtil::toString(groupVec, ",") << "]"
        << "expect: " << expectStr << endl;

    const partition::IndexPartitionPtr& partition = mPsm.GetIndexPartition();
    const partition::IndexPartitionReaderPtr& reader = partition->GetReader();
    SummaryReaderPtr summaryReader = reader->GetSummaryReader();
    SearchSummaryDocument summaryDoc(NULL, 4096);
    if (groupVec.empty())
    {
        ASSERT_TRUE(summaryReader->GetDocument(docId, &summaryDoc)) << msg.str();
    }
    else
    {
        ASSERT_TRUE(summaryReader->GetDocument(docId, &summaryDoc, groupVec)) << msg.str();
    }

    FieldSchemaPtr fieldSchema = mSchema->GetFieldSchema();
    assert(fieldSchema);
    SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();
    map<string, string> actual;
    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();
        if (summarySchema && summarySchema->IsInSummary(fieldId))
        {
            string fieldName = (*it)->GetFieldName();
            summaryfieldid_t summaryFieldId =
                summarySchema->GetSummaryFieldId(fieldId);
            summarygroupid_t summaryGroupId =
                summarySchema->FieldIdToSummaryGroupId(fieldId);
            const ConstString* fieldValue =
                summaryDoc.GetFieldValue(summaryFieldId);
            if (!fieldValue) 
            {
                if (expectStr.empty())
                {
                    cout << summaryGroupId << ": !" << fieldName << endl;
                }
                continue;
            }
            string fieldValueStr = fieldValue->toString();
            if (expectStr.empty())
            {
                cout << summaryGroupId << ": " << fieldName << "=" << fieldValueStr << endl;
            }
            actual.insert(make_pair(fieldName, fieldValueStr));
        }
    }
    if (!expectStr.empty())
    {
        vector<vector<string>> expect;
        StringUtil::fromString(expectStr, expect, "=", ",");
        ASSERT_EQ(expect.size(), actual.size());
        for (size_t i = 0; i < expect.size(); ++i)
        {
            ASSERT_TRUE(expect[i].size() == 1 || expect[i].size() == 2) << msg.str();
            const string& expectName = expect[i][0];
            const string& expectValue = expect[i].size() == 2 ? expect[i][1] : "";
            ASSERT_TRUE(actual.count(expectName)) << msg.str();
            ASSERT_EQ(expectValue, actual[expectName]) << msg.str();
        }
    }
}

void SummaryReaderIntetest::TestGroup()
{    
    string jsonString = mJsonStringHead + R"(
        "summarys": {
            "compress": true,
            "compress_block_size": 1,
            "compress_type" : "",
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ],
            "summary_groups" : [
                {
                    "group_name" : "mainse",
                    "compress": true,
                    "compress_block_size": 5,
                    "compress_type" : "zstd",
                    "summary_fields" : [ "quantity", "zk_time", "ends" ]
                },
                {
                    "group_name" : "inshop",
                    "compress": false,
                    "summary_fields" : [ "user", "user_id", "nick" ]
                },
                {
                    "group_name" : "vip",
                    "summary_fields" : [ "vip" ]
                }
            ]
        }
        )" + mJsonStringTail;
    mSchema.reset(new IndexPartitionSchema("noname"));
    ASSERT_NO_THROW(FromJsonString(*mSchema, jsonString));

    INDEXLIB_TEST_TRUE(mPsm.Init(mSchema, mOptions, mRootDir));
    string fullDoc = "cmd=add,nid=1,quantity=11,user=abc,vip=111,ts=1;";
    ASSERT_TRUE(mPsm.Transfer(BUILD_FULL, fullDoc, "pk:1",
                             "docid=0,nid=1,quantity=11,user=abc,vip=111"));
    CheckSummary(0, {},
                 "nid=1,title=,pid=,provcity=,category=,"
                 "quantity=11,zk_time=,ends=,"
                 "user=abc,user_id=,nick=,"
                 "vip=111");
    CheckSummary(0, {0}, "nid=1,title=,pid=,provcity=,category=");
    CheckSummary(0, {1,3}, "quantity=11,zk_time=,ends=,vip=111");


    string rtDoc = "cmd=add,nid=2,quantity=22,user=def,vip=222,ts=2;";
    ASSERT_TRUE(mPsm.Transfer(BUILD_RT, rtDoc, "pk:2", "docid=1"));

    const partition::IndexPartitionPtr& partition = mPsm.GetIndexPartition();
    const partition::IndexPartitionReaderPtr& reader = partition->GetReader();
    SummaryReaderPtr summaryReader = reader->GetSummaryReader();
    SearchSummaryDocument summaryDoc(NULL, 4096);
    ASSERT_FALSE(summaryReader->GetDocument(0, &summaryDoc, {4}));
    CheckSummary(0, {},
                 "nid=1,title=,pid=,provcity=,category=,"
                 "quantity=11,zk_time=,ends=,"
                 "user=abc,user_id=,nick=,"
                 "vip=111");
    CheckSummary(1, {},
                 "nid=2,title=,pid=,provcity=,category=,"
                 "quantity=22,zk_time=,ends=,"
                 "user=def,user_id=,nick=,"
                 "vip=222");
    CheckSummary(1, {0}, "nid=2,title=,pid=,provcity=,category=");
    CheckSummary(1, {1,3}, "quantity=22,zk_time=,ends=,vip=222");
}

void SummaryReaderIntetest::TestAllGroupNoNeedStore()
{
    // all fields is attributes
    string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "provcity", "quantity" ],
            "summary_groups" : [
                { "group_name" : "vip", "summary_fields": [ "vip" ]}
            ]
        }
        )" + mJsonStringTail;
    mSchema.reset(new IndexPartitionSchema("noname"));
    ASSERT_NO_THROW(FromJsonString(*mSchema, jsonString));
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        INDEXLIB_TEST_TRUE(mPsm.Init(mSchema, mOptions, mRootDir));
        string fullDoc = "cmd=add,nid=1,quantity=11,user=abc,vip=111,provcity=bj,ts=1;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_FULL, fullDoc, "pk:1", "docid=0"));
        EXPECT_TRUE(mPsm.Transfer(QUERY, "", "pk:1",
                                  "docid=0,nid=,quantity=11,user=,vip=111,provcity=bj"));
        CheckSummary(0, {}, "provcity=bj,quantity=11,vip=111");
        CheckSummary(0, {0}, "provcity=bj,quantity=11");
        CheckSummary(0, {1}, "vip=111");
        CheckSummary(0, {0,1}, "provcity=bj,quantity=11,vip=111");

        string rtDoc = "cmd=add,nid=2,quantity=22,provcity=nj,vip=222,ts=2;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_RT, rtDoc, "pk:2", "docid=1"));
        CheckSummary(1, {0}, "provcity=nj,quantity=22");
        CheckSummary(1, {1}, "vip=222");
        CheckSummary(1, {0,1}, "provcity=nj,quantity=22,vip=222");

        ASSERT_TRUE(mPsm.Transfer(BUILD_INC, rtDoc, "pk:2", "docid=1"));
        CheckSummary(1, {0}, "provcity=nj,quantity=22");
        CheckSummary(1, {1}, "vip=222");
        CheckSummary(1, {0,1}, "provcity=nj,quantity=22,vip=222");

        rtDoc = "cmd=add,nid=3,quantity=33,vip=333,ts=3;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_RT_SEGMENT, rtDoc, "pk:3", "docid=2"));
        CheckSummary(2, {0}, "provcity=,quantity=33");
        CheckSummary(2, {1}, "vip=333");
        CheckSummary(2, {0,1}, "provcity=,quantity=33,vip=333");        
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        INDEXLIB_TEST_TRUE(mPsm.Init(mSchema, mOptions, mRootDir));
        string fullDoc = "cmd=add,nid=1,quantity=11,user=abc,vip=111,provcity=bj,ts=1;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_FULL, fullDoc, "pk:1", "docid=0"));
        EXPECT_TRUE(mPsm.Transfer(QUERY, "", "pk:1",
                                  "docid=0,nid=,quantity=11,user=,vip=111,provcity=bj"));
        CheckSummary(0, {}, "provcity=bj,quantity=11,vip=111");
        CheckSummary(0, {0}, "provcity=bj,quantity=11");
        CheckSummary(0, {1}, "vip=111");
        CheckSummary(0, {0,1}, "provcity=bj,quantity=11,vip=111");

        string rtDoc = "cmd=add,nid=2,quantity=22,provcity=nj,vip=222,ts=2;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_RT, rtDoc, "pk:2", "docid=1"));
        CheckSummary(1, {0}, "provcity=nj,quantity=22");
        CheckSummary(1, {1}, "vip=222");
        CheckSummary(1, {0,1}, "provcity=nj,quantity=22,vip=222");

        ASSERT_TRUE(mPsm.Transfer(BUILD_INC, rtDoc, "pk:2", "docid=2")); 
        CheckSummary(2, {0}, "provcity=nj,quantity=22");
        CheckSummary(2, {1}, "vip=222");
        CheckSummary(2, {0,1}, "provcity=nj,quantity=22,vip=222");

        rtDoc = "cmd=add,nid=3,quantity=33,vip=333,ts=3;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_RT_SEGMENT, rtDoc, "pk:3", "docid=3"));
        CheckSummary(3, {0}, "provcity=,quantity=33");
        CheckSummary(3, {1}, "vip=333");
        CheckSummary(3, {0,1}, "provcity=,quantity=33,vip=333");        
    }    
}

void SummaryReaderIntetest::TestDefaultGroupNoNeedStore()
{
    // all fields in default group is attribute
    string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "provcity", "quantity" ],
            "summary_groups" : [
            {
                "group_name" : "inshop",
                "compress": true,
                "compress_type" : "zlib_default",
                "summary_fields": [ "user", "user_id" ]
            }
            ]
        }
        )" + mJsonStringTail;
    mSchema.reset(new IndexPartitionSchema("noname"));
    ASSERT_NO_THROW(FromJsonString(*mSchema, jsonString));
    {
        EnvGuard env("VERSION_FORMAT_NUM", "1");
        INDEXLIB_TEST_TRUE(mPsm.Init(mSchema, mOptions, mRootDir));
        string fullDoc = "cmd=add,nid=1,quantity=11,user=abc,user_id=111,provcity=bj,ts=1;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_FULL, fullDoc, "pk:1", "docid=0"));
        EXPECT_TRUE(mPsm.Transfer(QUERY, "", "pk:1",
                                  "docid=0,nid=,quantity=11,user=abc,user_id=111,provcity=bj"));
        CheckSummary(0, {}, "provcity=bj,quantity=11,user=abc,user_id=111");
        CheckSummary(0, {0}, "provcity=bj,quantity=11");
        CheckSummary(0, {1}, "user=abc,user_id=111");
        CheckSummary(0, {0,1}, "provcity=bj,quantity=11,user=abc,user_id=111");

        string rtDoc = "cmd=add,nid=2,quantity=22,user=def,user_id=222,provcity=nj,ts=2;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_RT, rtDoc, "pk:2", "docid=1"));
        CheckSummary(1, {0}, "provcity=nj,quantity=22");
        CheckSummary(1, {1}, "user=def,user_id=222");
        CheckSummary(1, {0,1}, "provcity=nj,quantity=22,user=def,user_id=222");

        ASSERT_TRUE(mPsm.Transfer(BUILD_INC, rtDoc, "pk:2", "docid=1"));
        CheckSummary(1, {0}, "provcity=nj,quantity=22");
        CheckSummary(1, {1}, "user=def,user_id=222");
        CheckSummary(1, {0,1}, "provcity=nj,quantity=22,user=def,user_id=222");

        rtDoc = "cmd=add,nid=3,quantity=33,user=hijk,user_id=333,ts=3;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_RT_SEGMENT, rtDoc, "pk:3", "docid=2"));
        CheckSummary(2, {0}, "provcity=,quantity=33");
        CheckSummary(2, {1}, "user=hijk,user_id=333");
        CheckSummary(0, {0,1}, "provcity=bj,quantity=11,user=abc,user_id=111");
        CheckSummary(1, {0,1}, "provcity=nj,quantity=22,user=def,user_id=222");
        CheckSummary(2, {0,1}, "provcity=,quantity=33,user=hijk,user_id=333");        
    }
    {
        TearDown();
        SetUp();
        EnvGuard env("VERSION_FORMAT_NUM", "2");
        INDEXLIB_TEST_TRUE(mPsm.Init(mSchema, mOptions, mRootDir));
        string fullDoc = "cmd=add,nid=1,quantity=11,user=abc,user_id=111,provcity=bj,ts=1;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_FULL, fullDoc, "pk:1", "docid=0"));
        EXPECT_TRUE(mPsm.Transfer(QUERY, "", "pk:1",
                                  "docid=0,nid=,quantity=11,user=abc,user_id=111,provcity=bj"));
        CheckSummary(0, {}, "provcity=bj,quantity=11,user=abc,user_id=111");
        CheckSummary(0, {0}, "provcity=bj,quantity=11");
        CheckSummary(0, {1}, "user=abc,user_id=111");
        CheckSummary(0, {0,1}, "provcity=bj,quantity=11,user=abc,user_id=111");

        string rtDoc = "cmd=add,nid=2,quantity=22,user=def,user_id=222,provcity=nj,ts=2;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_RT, rtDoc, "pk:2", "docid=1"));
        CheckSummary(1, {0}, "provcity=nj,quantity=22");
        CheckSummary(1, {1}, "user=def,user_id=222");
        CheckSummary(1, {0,1}, "provcity=nj,quantity=22,user=def,user_id=222");

        ASSERT_TRUE(mPsm.Transfer(BUILD_INC, rtDoc, "pk:2", "docid=2"));
        CheckSummary(2, {0}, "provcity=nj,quantity=22");
        CheckSummary(2, {1}, "user=def,user_id=222");
        CheckSummary(2, {0,1}, "provcity=nj,quantity=22,user=def,user_id=222");

        rtDoc = "cmd=add,nid=3,quantity=33,user=hijk,user_id=333,ts=3;";
        ASSERT_TRUE(mPsm.Transfer(BUILD_RT_SEGMENT, rtDoc, "pk:3", "docid=3"));
        CheckSummary(3, {0}, "provcity=,quantity=33");
        CheckSummary(3, {1}, "user=hijk,user_id=333");
        CheckSummary(0, {0,1}, "provcity=bj,quantity=11,user=abc,user_id=111");
        CheckSummary(1, {0,1}, "provcity=nj,quantity=22,user=def,user_id=222");
        CheckSummary(2, {0,1}, "provcity=nj,quantity=22,user=def,user_id=222");        

    }    
}

void SummaryReaderIntetest::TestCompress()
{
    string jsonString = mJsonStringHead + R"(
        "summarys": {
            "compress": true,
            "compress_type": "zstd",
            "summary_fields": [ "nid" ],
            "summary_groups": [
            { "group_name": "1", "compress": true, "compress_type": "", "summary_fields": [ "nick" ] },
            { "group_name": "2", "compress": true, "compress_type": "zlib_default", "summary_fields": [ "pid" ] },
            { "group_name": "3", "compress": true, "compress_type": "zlib", "summary_fields": [ "ends" ] },
            { "group_name": "4", "compress": true, "compress_type": "snappy", "summary_fields": [ "user" ] },
            { "group_name": "5", "compress": true, "compress_type": "lz4", "summary_fields": [ "title" ] },
            { "group_name": "6", "compress": true, "compress_type": "lz4hc", "summary_fields": [ "category" ] }
            ]
        }
        )" + mJsonStringTail;
    mSchema.reset(new IndexPartitionSchema("noname"));
    ASSERT_NO_THROW(FromJsonString(*mSchema, jsonString));

    INDEXLIB_TEST_TRUE(mPsm.Init(mSchema, mOptions, mRootDir));
    string fullDoc = "cmd=add,ts=1,nid=1,nick=a1,pid=b1,ends=c1,user=d1,title=e1,category=f1;";
    ASSERT_TRUE(mPsm.Transfer(BUILD_FULL, fullDoc, "pk:1", "docid=0"));
    CheckSummary(0, {}, "nid=1,nick=a1,pid=b1,ends=c1,user=d1,title=e1,category=f1");
    
    string rtDoc = "cmd=add,ts=2,nid=2,nick=a2,pid=b2,ends=c2,user=d2,title=e2,category=f2;";
    ASSERT_TRUE(mPsm.Transfer(BUILD_RT, rtDoc, "pk:2", "docid=1"));
    CheckSummary(1, {}, "nid=2,nick=a2,pid=b2,ends=c2,user=d2,title=e2,category=f2");
}

void SummaryReaderIntetest::TestCompressMultiThread()
{
    string jsonString = mJsonStringHead + R"(
        "summarys": {
            "compress": true,
            "compress_type": "zstd",
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ]
        }
        )" + mJsonStringTail;
    mSchema.reset(new IndexPartitionSchema("noname"));
    ASSERT_NO_THROW(FromJsonString(*mSchema, jsonString));

    INDEXLIB_TEST_TRUE(mPsm.Init(mSchema, mOptions, mRootDir));

    string result = "title=title,pid=11,provcity=provcity,category=111";
    string fullDoc = "cmd=add,ts=1,nid=1," + result;
    ASSERT_TRUE(mPsm.Transfer(BUILD_FULL, fullDoc, "pk:1", "docid=0,title=title,pid=11,provcity=provcity,category=111"));
    string rtDoc = "cmd=add,ts=2,nid=2," + result;
    ASSERT_TRUE(mPsm.Transfer(BUILD_RT, rtDoc, "pk:2", "docid=1,title=title,pid=11,provcity=provcity,category=111"));

    vector<ThreadPtr> readThreads;
    size_t threadCount = 10;
    for (size_t i = 0 ; i < threadCount; ++i)
    {
        auto checkFunc = [this, result]() {
            for (size_t j = 0; j < 100; ++j) {
                ASSERT_TRUE(mPsm.Transfer(QUERY, "", "pk:1", "docid=0," + result));
                ASSERT_TRUE(mPsm.Transfer(QUERY, "", "pk:2", "docid=1," + result));
            }
        };
        ThreadPtr thread = Thread::createThread(tr1::bind(checkFunc));
        readThreads.push_back(thread);
    }
    for (auto& thread : readThreads)
    {
        thread.reset();
    }
}

IE_NAMESPACE_END(index);


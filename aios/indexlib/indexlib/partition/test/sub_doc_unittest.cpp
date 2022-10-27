#include "indexlib/partition/test/sub_doc_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index/test/deploy_index_checker.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/util/env_util.h"
#include "indexlib/config/impl/merge_config_impl.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocTest);

SubDocTest::SubDocTest()
{
}

SubDocTest::~SubDocTest()
{
}

void SubDocTest::CaseSetUp()
{
    string field = "pkstr:string;text1:text;long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "pkstr;";

    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);

    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
                            "substr1:string;subpkstr:string;sub_long:uint32;",
                            "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
                            "substr1;subpkstr;sub_long;",
                            "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mPartMeta.reset(new PartitionMeta);
    mPartMeta->AddSortDescription("sortField", sp_asc);
    mRootDir = GET_TEST_DATA_PATH();

    mOptions.GetOnlineConfig().buildConfig.speedUpPrimaryKeyReader =
        std::get<0>(GET_CASE_PARAM());
    mOptions.GetOfflineConfig().buildConfig.speedUpPrimaryKeyReader =
        std::get<0>(GET_CASE_PARAM());
    mOptions.SetEnablePackageFile(std::get<1>(GET_CASE_PARAM()));
    mOptions.GetOnlineConfig().SetEnableRedoSpeedup(true);
    mOptions.GetOnlineConfig().SetVersionTsAlignment(1); 
    mPsm.Init(mSchema, mOptions, mRootDir);
}

void SubDocTest::CaseTearDown()
{
    if (mPsm.GetIndexPartition())
    {
        mPsm.GetIndexPartition()->Close();
    }
}

bool SubDocTest::CheckDocCount(const test::PartitionStateMachine& psm,
                               size_t mainDocCount, size_t subDocCount)
{
    const partition::IndexPartitionPtr& partition = psm.GetIndexPartition();
    const IndexPartitionReaderPtr& reader = partition->GetReader();
    PartitionInfoPtr mainInfo = reader->GetPartitionInfo();
    PartitionInfoPtr subInfo = mainInfo->GetSubPartitionInfo();

    assert(subInfo == reader->GetSubPartitionReader()->GetPartitionInfo());

    if (mainDocCount != mainInfo->GetTotalDocCount())
    {
        IE_LOG(ERROR, "Actual Main DocCount[%lu] != Exppect[%lu]",
               mainInfo->GetTotalDocCount(), mainDocCount);
        return false;
    }
    if (subDocCount != subInfo->GetTotalDocCount())
    {
        IE_LOG(ERROR, "Actual Sub DocCount[%lu] != Exppect[%lu]",
               subInfo->GetTotalDocCount(), mainDocCount);
        return false;
    }
    return true;
}

void SubDocTest::TestAddMainDocWithSubDoc()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub2,substr1=s1^s2";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,main_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s2", 
                                    "docid=1,subpkstr=sub2,sub_join=0;"));

    string docString2 = "cmd=add,pkstr=hello2,ts=1,subpkstr=sub3^sub4,substr1=s3^s4";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello2", 
                                    "docid=1,pkstr=hello2,main_join=4;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s4", 
                                    "docid=3,subpkstr=sub4,sub_join=1;"));

    string docStringRt = "cmd=add,pkstr=hello3,ts=2,subpkstr=sub5,substr1=s5";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docStringRt, "pk:hello3", 
                                    "docid=2,pkstr=hello3,main_join=5;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s5", 
                                    "docid=4,subpkstr=sub5,sub_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "__subdocid:4", 
                                    "docid=4,subpkstr=sub5,sub_join=2;"));
}

void SubDocTest::TestAddMainDocOnly()
{
    string docString1 = "cmd=add,pkstr=hello1,text1=m,ts=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString1, "pack1:m", 
                                    "docid=0,pkstr=hello1,main_join=0;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(mPsm, 1, 0));

    string docString2 = "cmd=add,pkstr=hello2,text1=m,ts=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pack1:m", 
                                    "docid=0,pkstr=hello1,main_join=0;"
                                    "docid=1,pkstr=hello2,main_join=0;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(mPsm, 2, 0));

    string docString3 = "cmd=add,pkstr=hello3,text1=m,ts=3";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString3, "pack1:m", 
                                    "docid=0,pkstr=hello1,main_join=0;"
                                    "docid=1,pkstr=hello2,main_join=0;"
                                    "docid=2,pkstr=hello3,main_join=0;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(mPsm, 3, 0));
}

void SubDocTest::TestAddThreeDocWithOneEmptySubDoc()
{
    string docString = "cmd=add,pkstr=pk1,text1=m,subpkstr=sub1^sub2,substr1=s^s;"
                       "cmd=add,pkstr=pk2,text1=m;"
                       "cmd=add,pkstr=pk3,text1=m,subpkstr=sub3,substr1=s;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", 
                    "docid=0,pkstr=pk1,main_join=2;"
                    "docid=1,pkstr=pk2,main_join=2;"
                    "docid=2,pkstr=pk3,main_join=3;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                    "docid=0,subpkstr=sub1,sub_join=0;"
                    "docid=1,subpkstr=sub2,sub_join=0;"
                    "docid=2,subpkstr=sub3,sub_join=2;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(mPsm, 3, 3));
}

void SubDocTest::TestAddDocWithoutPk()
{
    string docString = "cmd=add,text1=m,subpkstr=sub1,substr1=s;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", ""));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", ""));
}

void SubDocTest::TestAddDocInRtOnly()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub2,substr1=s1^s2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,main_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s2", 
                                    "docid=1,subpkstr=sub2,sub_join=0;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(psm, 1, 2));

    string docString2 = "cmd=add,pkstr=hello2,ts=1,subpkstr=sub3^sub4,substr1=s3^s4";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString2, "pk:hello2", 
                                    "docid=1,pkstr=hello2,main_join=4;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s4", 
                                    "docid=3,subpkstr=sub4,sub_join=1;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(psm, 2, 4));
}

void SubDocTest::TestAddDupMainDoc()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1,substr1=s1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,main_join=1;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,sub_join=0;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(psm, 1, 1));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "pk:hello", 
                                    "docid=1,main_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=1,sub_join=1;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(psm, 2, 2));

    string docString2 = "cmd=add,pkstr=hello,ts=1,subpkstr=sub1,substr1=s1";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString2, "pk:hello", 
                                    "docid=2,main_join=3;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s1", 
                                    "docid=2,sub_join=2;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(psm, 3, 3));
}

void SubDocTest::TestAddDupMainDocWithDiffSubDoc()
{
    string docString = "cmd=add,pkstr=main,text1=m,ts=0,subpkstr=sub1,substr1=s;"
                       "cmd=add,pkstr=main,text1=m,ts=0,subpkstr=sub2^sub3,substr1=s^s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", 
                                    "docid=1,main_join=3;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                                    "docid=1,subpkstr=sub2,sub_join=1;"
                                    "docid=2,subpkstr=sub3,sub_join=1;"));

    string docString1 = "cmd=add,pkstr=main,text1=m,ts=1,subpkstr=sub4,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString1, "pack1:m", 
                                    "docid=2,main_join=4;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s", 
                                    "docid=3,subpkstr=sub4,sub_join=2;"));

    string docString2 = "cmd=add,pkstr=main,text1=m,ts=2,subpkstr=sub5,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString2, "pack1:m", 
                                    "docid=3,main_join=5;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s", 
                                    "docid=4,subpkstr=sub5,sub_join=3;"));
}

void SubDocTest::TestAddDupSubDocInFull()
{
    // Main PK diff, Sub Pk dup
    string docString = "cmd=add,pkstr=pk1,text1=m,subpkstr=sub1,substr1=s;"
                       "cmd=add,pkstr=pk2,text1=m,subpkstr=sub1,substr1=s;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", 
                    "docid=0,pkstr=pk1,main_join=1;"
                    "docid=1,pkstr=pk2,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                    "docid=1,subpkstr=sub1,sub_join=1;"));
}

void SubDocTest::TestAddDupSubDocInInc()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1,substr1=s1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,sub_join=0;"));

    // inc dup with full
    string dupDocString = "cmd=add,pkstr=hello2,ts=2,subpkstr=sub1,substr1=s1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, dupDocString, "pk:hello", 
                                    "docid=0,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "pk:hello2", 
                                    "docid=1,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=1,sub_join=1;"));
}

void SubDocTest::TestAddDupSubDocInRt()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1,substr1=s1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,sub_join=0;"));

    // dup with full
    string dupDocString = "cmd=add,pkstr=hello2,ts=2,subpkstr=sub1,substr1=s1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, dupDocString, "pk:hello", 
                                    "docid=0,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "pk:hello2", 
                                    "docid=1,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s1", 
                                    "docid=1,sub_join=1;"));

    // dup in one rt
    string dupDocString1 = "cmd=add,pkstr=hello3,ts=4,subpkstr=sub2,substr1=s2;"
                           "cmd=add,pkstr=hello4,ts=4,subpkstr=sub2,substr1=s2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, dupDocString1, "pk:hello3", 
                                    "docid=2,main_join=3;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "pk:hello4",
                                    "docid=3,main_join=4;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s2", 
                                    "docid=3,sub_join=3;"));
}

void SubDocTest::TestAddDupSubDocInOneMainDocInRt()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub1,sub_long=1^2,substr1=s^s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                                    "docid=0,sub_long=2,sub_join=0;"));

    string docString2 = "cmd=add,pkstr=hello2,ts=0,subpkstr=ns0^ns1^ns2^ns1^ns1,"
                        "sub_long=1^2^3^4^5,substr1=ns^ns^ns^ns^ns";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString2, "pk:hello2", 
                                    "docid=1,main_join=4;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:ns", 
                                    "docid=1,sub_long=1,sub_join=1;"
                                    "docid=2,sub_long=3,sub_join=1;"
                                    "docid=3,sub_long=5,sub_join=1;"));
}

void SubDocTest::TestAddDocWithoutAttributeSchema()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "pkstr:string;", "pk:primarykey64:pkstr;", "", "");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "subpkstr:string;", "sub_pk:primarykey64:subpkstr", "", "");
    schema->SetSubIndexPartitionSchema(subSchema);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));

    string docString = "cmd=add,pkstr=main1,ts=0,subpkstr=sub1";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:main1", 
                                    "docid=0,main_join=1;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "sub_pk:sub1", 
                                    "docid=0,sub_join=0;"));
}

void SubDocTest::TestBuildingSegmentDedup()
{
    string docString = "cmd=add,pkstr=m1,text1=m,ts=0,subpkstr=s1,substr1=s;"
                       "cmd=add,pkstr=m1,text1=m,ts=0,subpkstr=s1,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString,
                    "pack1:m", "docid=1,main_join=2"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "",
                    "subindex1:s", "docid=1,sub_join=1;"));

    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString,
                    "pack1:m", "docid=3,main_join=4"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "",
                    "subindex1:s", "docid=3,sub_join=3;"));

    string docStringRt = "cmd=add,pkstr=m1,text1=m,ts=1,subpkstr=s1,substr1=s;"
                         "cmd=add,pkstr=m1,text1=m,ts=1,subpkstr=s1,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docStringRt,
                    "pack1:m", "docid=5,main_join=6"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "",
                    "subindex1:s", "docid=5,sub_join=5;"));
}

void SubDocTest::TestBuiltSegmentsDedup()
{
    mOptions.GetOnlineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    INDEXLIB_TEST_TRUE(mPsm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,pkstr=m1,text1=m,ts=0,subpkstr=s1,substr1=s;"
                       "cmd=add,pkstr=m1,text1=m,ts=0,subpkstr=s1,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString,
                    "pack1:m", "docid=1,main_join=2"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "",
                    "subindex1:s", "docid=1,sub_join=1;"));

    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString,
                    "pack1:m", "docid=3,main_join=4"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "",
                    "subindex1:s", "docid=3,sub_join=3;"));

    string docStringRt = "cmd=add,pkstr=m1,text1=m,ts=1,subpkstr=s1,substr1=s;"
                         "cmd=add,pkstr=m1,text1=m,ts=1,subpkstr=s1,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docStringRt,
                    "pack1:m", "docid=5,main_join=6"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "",
                    "subindex1:s", "docid=5,sub_join=5;"));
}

void SubDocTest::TestRewriteToUpdateDocWithMainFieldModified()
{
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "sub_pk:sub1", 
                                    "docid=0,subpkstr=sub1,sub_join=0;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(mPsm, 1, 1));

    string docString2 = "cmd=add,pkstr=hello,long1=2,ts=1,modify_fields=long1,subpkstr=sub1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=2,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "sub_pk:sub1", 
                                    "docid=0,subpkstr=sub1,sub_join=0;"));
    INDEXLIB_TEST_TRUE(CheckDocCount(mPsm, 1, 1));
}

void SubDocTest::TestRewriteToUpdateDocWithSubFieldModified()
{
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1^sub2,sub_long=1^2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,long1=1,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "sub_pk:sub2", 
                                    "docid=1,sub_long=2,sub_join=0;"));
    string docString2 = "cmd=add,pkstr=hello,long1=2,ts=1,modify_fields=long1#sub_long,"
                        "subpkstr=sub1^sub2,sub_long=3^4,sub_modify_fields=^sub_long";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello", 
                                    "docid=0,long1=2,main_join=2"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "sub_pk:sub1", 
                                    "docid=0,sub_long=1,sub_join=0;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "sub_pk:sub2", 
                                    "docid=1,sub_long=4,sub_join=0;"));
}

void SubDocTest::TestRewriteToUpdateDocWithOnlySubFieldModified()
{
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1^sub2,sub_long=1^2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,long1=1,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "sub_pk:sub2", 
                                    "docid=1,sub_long=2,sub_join=0;"));

    string docString2 = "cmd=add,pkstr=hello,long1=2,ts=1,modify_fields=sub_long,"
                        "subpkstr=sub1^sub2,sub_long=3^4,sub_modify_fields=^sub_long";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello", 
                                    "docid=0,long1=1,main_join=2"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "sub_pk:sub1", 
                                    "docid=0,sub_long=1,sub_join=0;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "sub_pk:sub2", 
                                    "docid=1,sub_long=4,sub_join=0;"));
}


void SubDocTest::TestRewriteToUpdateDocForExceptionSubModifiedField()
{
    string docString = "cmd=add,pkstr=hello,long1=1,subpkstr=sub1,sub_long=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "sub_pk:sub1", 
                                    "docid=0,sub_long=1,sub_join=0;"));
    string docString2 = "cmd=add,pkstr=hello,long1=2,modify_fields=long1#sub_long,"
                        "subpkstr=sub1,sub_long=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello", 
                                    "docid=1,long1=2,main_join=2"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "sub_pk:sub1", 
                                    "docid=1,sub_long=2,sub_join=1;"));
}

void SubDocTest::TestUpdateMainDocOnly()
{
    // set flush interval = 3 second
    setenv("INDEXLIB_DATA_FLUSH_PARAM", "quota_interval=3000", true);
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1,substr1=s1,sub_long=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_long=1,sub_join=0;"));

    string docString1 = "cmd=update_field,pkstr=hello,long1=2,ts=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString1, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=2,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_long=1,sub_join=0;"));

    string docString2 = "cmd=update_field,pkstr=hello,long1=3,ts=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString2, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=3,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_long=1,sub_join=0;"));
}

void SubDocTest::TestUpdateSubDocOnly()
{
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1,substr1=s1,sub_long=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_long=1,sub_join=0;"));

    string docString1 = "cmd=update_field,pkstr=hello,ts=1,subpkstr=sub1,sub_long=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString1, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0,sub_long=2;"));

    string docString2 = "cmd=update_field,pkstr=hello,ts=2,subpkstr=sub1,sub_long=3";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString2, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0,sub_long=3;"));
}

void SubDocTest::TestUpdateMainDocWithSubDoc()
{
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1,substr1=s1,sub_long=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_long=1,sub_join=0;"));

    string docString1 = "cmd=update_field,pkstr=hello,long1=2,ts=1,subpkstr=sub1,sub_long=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString1, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=2,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0,sub_long=2;"));

    string docString2 = "cmd=update_field,pkstr=hello,long1=3,ts=2,subpkstr=sub1,sub_long=3";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString2, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=3,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0,sub_long=3;"));
}

void SubDocTest::TestUpdateMainDocWithSubDocInOneSegment()
{
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1,substr1=s1,sub_long=1;"
                       "cmd=update_field,pkstr=hello,long1=2,ts=1,subpkstr=sub1,sub_long=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=2,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0,sub_long=2;"));
}

void SubDocTest::TestUpdateSubPkNotInSameDoc()
{
    string docString = "cmd=add,pkstr=pk1,text1=m,ts=0,sub_long=1,subpkstr=sub1,substr1=s;"
                       "cmd=add,pkstr=pk2,text1=m,ts=0,sub_long=2,subpkstr=sub2,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", 
                    "docid=0,pkstr=pk1,main_join=1;"
                    "docid=1,pkstr=pk2,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                    "docid=0,subpkstr=sub1,sub_long=1,sub_join=0;"
                    "docid=1,subpkstr=sub2,sub_long=2,sub_join=1;"));

    string docString2 = "cmd=update_field,pkstr=pk1,ts=1,sub_long=3,subpkstr=sub2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pack1:m", 
                                    "docid=0,pkstr=pk1,main_join=1;"
                                    "docid=1,pkstr=pk2,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s",
                                    "docid=0,subpkstr=sub1,sub_long=1,sub_join=0;"
                                    "docid=1,subpkstr=sub2,sub_long=2,sub_join=1;"));
}

void SubDocTest::TestDeleteMainDocInOneFullSegment()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1,substr1=s1;"
                       "cmd=delete,pkstr=hello,ts=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", ""));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s1", ""));
}

void SubDocTest::TestDeleteMainDocInOneRtSegment()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1,substr1=s1;"
                       "cmd=delete,pkstr=hello,ts=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString, "pk:hello", ""));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s1", ""));
}

void SubDocTest::TestDeleteMainDocOnly()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1,substr1=s1;"
                       "cmd=add,pkstr=hello2,ts=0,subpkstr=sub2,substr1=s2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s2", 
                                    "docid=1,subpkstr=sub2,sub_join=1;"));
    
    string docString2 = "cmd=delete,pkstr=hello,ts=1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello", ""));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", ""));

    string docString3 = "cmd=delete,pkstr=hello2,ts=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString3, "pk:hello2", ""));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s2", ""));
}

void SubDocTest::TestDeleteSubDocOnly()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1,substr1=s1;"
                       "cmd=add,pkstr=hello2,ts=0,subpkstr=sub2,substr1=s2";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s2", 
                                    "docid=1,subpkstr=sub2,sub_join=1;"));
    
    string docString2 = "cmd=delete_sub,pkstr=hello,ts=1,subpkstr=sub1";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello", 
                                    "docid=0,main_join=1"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", ""));

    string docString3 = "cmd=delete_sub,pkstr=hello2,ts=2,subpkstr=sub2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString3, "pk:hello2", 
                                    "docid=1,main_join=2"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s2", ""));
}

void SubDocTest::TestDeleteSubPkNotInSameDoc()
{
    string docString = "cmd=add,pkstr=pk1,text1=m,ts=0,subpkstr=sub1,substr1=s;"
                       "cmd=add,pkstr=pk2,text1=m,ts=0,subpkstr=sub2,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", 
                    "docid=0,pkstr=pk1,main_join=1;"
                    "docid=1,pkstr=pk2,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                    "docid=0,subpkstr=sub1,sub_join=0;"
                    "docid=1,subpkstr=sub2,sub_join=1;"));

    string docString2 = "cmd=delete_sub,pkstr=pk1,ts=1,subpkstr=sub2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pack1:m", 
                                    "docid=0,pkstr=pk1,main_join=1;"
                                    "docid=1,pkstr=pk2,main_join=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s",
                                    "docid=0,subpkstr=sub1,sub_join=0;"
                                    "docid=1,subpkstr=sub2,sub_join=1;"));
}

void SubDocTest::TestObsoleteRtDoc()
{
    string docString = "cmd=add,pkstr=main,long1=1,text1=m,ts=10,subpkstr=sub1,substr1=s,sub_long=1;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", 
                                    "docid=0,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                                    "docid=0,sub_long=1,subpkstr=sub1,sub_join=0;"));

    string docString2 = "cmd=add,pkstr=main,text1=m,ts=2,subpkstr=sub5,substr1=s";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString2, "pack1:m",
                                    "docid=0,long1=1,main_join=1;")); 
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s", 
                                    "docid=0,sub_long=1,subpkstr=sub1,sub_join=0;"));

    string docString3 = "cmd=delete,pkstr=main,ts=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString3, "pack1:m", 
                                    "docid=0,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s", 
                                    "docid=0,sub_long=1,subpkstr=sub1,sub_join=0;"));

    string docString4 = "cmd=delete_sub,pkstr=main,ts=2,subpkstr=sub1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString4, "pack1:m",
                                    "docid=0,long1=1,main_join=1;")); 
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s", 
                                    "docid=0,sub_long=1,subpkstr=sub1,sub_join=0;"));

    string docString5 = "cmd=update_field,long1=2,pkstr=main,ts=2,subpkstr=sub1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString5, "pack1:m", 
                                    "docid=0,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s", 
                                    "docid=0,sub_long=1,subpkstr=sub1,sub_join=0;"));

    string docString6 = "cmd=update_field,pkstr=main,ts=2,subpkstr=sub1,sub_long=2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString6, "pack1:m", 
                                    "docid=0,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s", 
                                    "docid=0,sub_long=1,subpkstr=sub1,sub_join=0;"));
    // reopen
    string docStringOther = "cmd=add,pkstr=other,text1=o,ts=10,subpkstr=subOther,substr1=so";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docStringOther, "pack1:m", 
                                    "docid=0,long1=1,main_join=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s", 
                                    "docid=0,sub_long=1,subpkstr=sub1,sub_join=0;"));
}

void SubDocTest::TestNeedDumpByMaxDocCount()
{
    PartitionStateMachine psm;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string docString = "cmd=add,pkstr=pk1,text1=m,subpkstr=sub1^sub2,substr1=s^s;"
                       "cmd=add,pkstr=pk2,text1=m,subpkstr=sub3,substr1=s;"
                       "cmd=add,pkstr=pk3,text1=m,subpkstr=sub4^sub5,substr1=s^s;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m",
                    "docid=0;docid=1;docid=2"));

    const IndexPartitionPtr& partition = psm.GetIndexPartition();
    IndexPartitionReaderPtr reader = partition->GetReader();
    Version version = reader->GetVersion();
    ASSERT_EQ((size_t)3, version.GetSegmentCount());
}

void SubDocTest::TestNeedDumpByMemorUse()
{
    PartitionStateMachine psm;
    mOptions.GetOfflineConfig().buildConfig.buildTotalMemory = 40;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    
    string substring1(1024 * 1024 * 20, 'a'); // 10M string attribute
    string docString = "cmd=add,pkstr=pk1,text1=m,subpkstr=sub1,substr1="
                       + substring1 + ";";
    docString += "cmd=add,pkstr=pk2,text1=m,subpkstr=sub2,substr1=s;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m",
                    "docid=0;docid=1"));

    const IndexPartitionPtr& partition = psm.GetIndexPartition();
    IndexPartitionReaderPtr reader = partition->GetReader();
    Version version = reader->GetVersion();
    ASSERT_EQ((size_t)2, version.GetSegmentCount());
}

void SubDocTest::TestReOpenWithAdd()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1,substr1=s1,sub_long=1";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString, "pk:hello", 
                                    "docid=0,long1=1,main_join=1;"));
    string docString2 = "cmd=add,pkstr=hello,long1=3,ts=1,subpkstr=sub1,substr1=s1,sub_long=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString2, "pk:hello", 
                                    "docid=1,long1=3,main_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "pk:hello", 
                                    "docid=2,long1=3,main_join=3;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=2,sub_long=3,sub_join=2;"));
}

void SubDocTest::TestReOpenWithUpdate()
{
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1,substr1=s1,sub_long=1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=1,main_join=1;"));

    string docString1 = "cmd=update_field,pkstr=hello,long1=2,ts=3,subpkstr=sub1,sub_long=2";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString1, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=2,main_join=1;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0,sub_long=2;"));

    string docString2 = "cmd=update_field,pkstr=hello,long1=3,ts=2,subpkstr=sub1,sub_long=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello", 
                                    "docid=0,pkstr=hello,long1=2,main_join=1;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0,sub_long=2;"));

}

void SubDocTest::TestReOpenWithDeleteSub()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1,substr1=s1";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "subindex1:s1", 
                                    "docid=0,subpkstr=sub1,sub_join=0;"));
    
    string docString2 = "cmd=delete_sub,pkstr=hello,ts=2,subpkstr=sub1";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString2, "pk:hello", 
                                    "docid=0,main_join=1"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s1", ""));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "pk:hello", 
                                    "docid=1,main_join=2"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", ""));
}

void SubDocTest::TestForceReOpenWithAdd()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pkstr=hello,long1=1,ts=0,subpkstr=sub1,substr1=s1,sub_long=1";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString, "pk:hello", 
                                    "docid=0,long1=1,main_join=1;"));
    string docString2 = "cmd=add,pkstr=hello,long1=3,ts=1,subpkstr=sub1,substr1=s1,sub_long=3";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString2, "pk:hello", 
                                    "docid=1,long1=3,main_join=2;"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "pk:hello", 
                                    "docid=2,long1=3,main_join=3;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                                    "docid=2,sub_long=3,sub_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "",
                                    "pk:hello", "docid=2,long1=3,main_join=3;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "",
                                    "subindex1:s1", "docid=2,sub_long=3,sub_join=2;"));
}

void SubDocTest::TestIncCoverPartialRtWithDupSubDoc()
{
    {
        TearDown();
        SetUp();        
        EnvGuard env("VERSION_FORMAT_NUM", "1");        
        string docString = "cmd=add,ts=1,pkstr=main1,long1=1,text1=m,subpkstr=sub1^sub2;"
            "cmd=add,ts=2,pkstr=main2,long1=2,text1=m,subpkstr=sub3^sub4,sub_long=2,substr1=s;";
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "",
                                         "docid=0,pkstr=main1;"
                                         "docid=1,pkstr=main2;"));

        string docString1 = "cmd=add,ts=10,pkstr=main1,long1=1,text1=m,subpkstr=sub1^sub2,substr1=s^s;"
            "cmd=add,ts=20,pkstr=main2,long1=3,text1=m;";
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString1, "pack1:m",
                                         "docid=2,pkstr=main1;"
                                         "docid=3,pkstr=main2;"));
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s", 
                                         "docid=4,subpkstr=sub1;"
                                         "docid=5,subpkstr=sub2;"));

        string docString2 = "cmd=add,ts=10,pkstr=main1,long1=1,text1=m,subpkstr=sub1^sub2,substr1=s^s;";
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pack1:m", 
                                         "docid=2,pkstr=main1;"
                                         "docid=4,pkstr=main2;")); // *
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s", 
                                         "docid=4,subpkstr=sub1;"
                                         "docid=5,subpkstr=sub2;"));        
    }
    {
        TearDown();
        SetUp();        
        EnvGuard env("VERSION_FORMAT_NUM", "2"); 
        string docString = "cmd=add,ts=1,pkstr=main1,long1=1,text1=m,subpkstr=sub1^sub2;"
            "cmd=add,ts=2,pkstr=main2,long1=2,text1=m,subpkstr=sub3^sub4,sub_long=2,substr1=s;";
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "",
                                         "docid=0,pkstr=main1;"
                                         "docid=1,pkstr=main2;"));

        string docString1 = "cmd=add,ts=10,pkstr=main1,long1=1,text1=m,subpkstr=sub1^sub2,substr1=s^s;"
            "cmd=add,ts=20,pkstr=main2,long1=3,text1=m;";
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString1, "pack1:m",
                                         "docid=2,pkstr=main1;"
                                         "docid=3,pkstr=main2;"));
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, "", "subindex1:s", 
                                         "docid=4,subpkstr=sub1;"
                                         "docid=5,subpkstr=sub2;"));

        string docString2 = "cmd=add,ts=11,pkstr=main1,long1=1,text1=m,subpkstr=sub1^sub2,substr1=s^s;";
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pack1:m", 
                                         "docid=2,pkstr=main1;"
                                         "docid=4,pkstr=main2;")); // *
        INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s", 
                                         "docid=4,subpkstr=sub1;"
                                         "docid=5,subpkstr=sub2;"));        
     }   
}

void SubDocTest::TestIncCoverEntireRt()
{
    {
        TearDown();
        SetUp();        
        EnvGuard env("VERSION_FORMAT_NUM", "1"); 
        string docString = "cmd=add,ts=0,pkstr=main1,long1=1,text1=m;"
            "cmd=add,ts=0,pkstr=main2,long1=2,text1=m,subpkstr=sub1^sub2,sub_long=1^2,substr1=s^s;"
            "cmd=add,ts=0,pkstr=main3,long1=3,text1=m,subpkstr=sub3,sub_long=3,substr1=s;";

        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, mRootDir);
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", 
                                         "docid=0,pkstr=main1,long1=1,main_join=0;"
                                         "docid=1,pkstr=main2,long1=2,main_join=2;"
                                         "docid=2,pkstr=main3,long1=3,main_join=3;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                                         "docid=0,subpkstr=sub1,sub_long=1,sub_join=1;"
                                         "docid=1,subpkstr=sub2,sub_long=2,sub_join=1;"
                                         "docid=2,subpkstr=sub3,sub_long=3,sub_join=2;"));

        string docString1 = "cmd=add,ts=3,pkstr=main4,long1=4,text1=m,subpkstr=sub4,sub_long=4,substr1=s;"
            "cmd=delete_sub,ts=3,pkstr=main2,subpkstr=sub2;"
            "cmd=delete,ts=3,pkstr=main3;"
            "cmd=update_field,ts=3,pkstr=main1,long1=11;"
            "cmd=update_field,ts=3,pkstr=main2,long1=22,subpkstr=sub1,sub_long=11;";
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, docString1, "pack1:m", 
                                         "docid=0,pkstr=main1,long1=11,main_join=0;"
                                         "docid=1,pkstr=main2,long1=22,main_join=2;"
                                         "docid=3,pkstr=main4,long1=4,main_join=4;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s", 
                                         "docid=0,subpkstr=sub1,sub_long=11,sub_join=1;"
                                         "docid=3,subpkstr=sub4,sub_long=4,sub_join=3;"));

        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString1, "pack1:m", 
                                         "docid=0,pkstr=main1,long1=11,main_join=0;"
                                         "docid=1,pkstr=main2,long1=22,main_join=2;"
                                         "docid=3,pkstr=main4,long1=4,main_join=4;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s", 
                                         "docid=0,subpkstr=sub1,sub_long=11,sub_join=1;"
                                         "docid=3,subpkstr=sub4,sub_long=4,sub_join=3;"));
    }
    {
        TearDown();
        SetUp();        
        EnvGuard env("VERSION_FORMAT_NUM", "2"); 
        string docString = "cmd=add,ts=0,pkstr=main1,long1=1,text1=m;"
            "cmd=add,ts=0,pkstr=main2,long1=2,text1=m,subpkstr=sub1^sub2,sub_long=1^2,substr1=s^s;"
            "cmd=add,ts=0,pkstr=main3,long1=3,text1=m,subpkstr=sub3,sub_long=3,substr1=s;";

        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, mRootDir);
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pack1:m", 
                                         "docid=0,pkstr=main1,long1=1,main_join=0;"
                                         "docid=1,pkstr=main2,long1=2,main_join=2;"
                                         "docid=2,pkstr=main3,long1=3,main_join=3;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "subindex1:s", 
                                         "docid=0,subpkstr=sub1,sub_long=1,sub_join=1;"
                                         "docid=1,subpkstr=sub2,sub_long=2,sub_join=1;"
                                         "docid=2,subpkstr=sub3,sub_long=3,sub_join=2;"));

        string rtDoc = "cmd=add,ts=3,pkstr=main4,long1=4,text1=m,subpkstr=sub4,sub_long=4,substr1=s;"
            "cmd=delete_sub,ts=3,pkstr=main2,subpkstr=sub2;"
            "cmd=delete,ts=3,pkstr=main3;"
            "cmd=update_field,ts=3,pkstr=main1,long1=11;"
            "cmd=update_field,ts=3,pkstr=main2,long1=22,subpkstr=sub1,sub_long=11;";

        string incDoc = rtDoc + "##stopTs=4;";
        
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, "pack1:m", 
                                         "docid=0,pkstr=main1,long1=11,main_join=0;"
                                         "docid=1,pkstr=main2,long1=22,main_join=2;"
                                         "docid=3,pkstr=main4,long1=4,main_join=4;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, "", "subindex1:s", 
                                         "docid=0,subpkstr=sub1,sub_long=11,sub_join=1;"
                                         "docid=3,subpkstr=sub4,sub_long=4,sub_join=3;"));

        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "pack1:m", 
                                         "docid=0,pkstr=main1,long1=11,main_join=0;"
                                         "docid=1,pkstr=main2,long1=22,main_join=2;"
                                         "docid=3,pkstr=main4,long1=4,main_join=4;"));
        INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, "##stopTs=4;", "subindex1:s", 
                                         "docid=0,subpkstr=sub1,sub_long=11,sub_join=1;"
                                         "docid=3,subpkstr=sub4,sub_long=4,sub_join=3;"));
    }    
}

void SubDocTest::TestIncCoverEntireRtWhenIncDiffRt()
{
    // same with TestIncCoverEntireRt, but inc data differ with rt
    string docString = "cmd=add,ts=0,pkstr=main1,long1=1,text1=m;"
                       "cmd=add,ts=0,pkstr=main2,long1=2,text1=m,subpkstr=sub1^sub2,sub_long=1^2,substr1=s^s;"
                       "cmd=add,ts=0,pkstr=main3,long1=3,text1=m,subpkstr=sub3,sub_long=3,substr1=s;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    string docString1 = "cmd=add,ts=3,pkstr=main4,long1=4,text1=m,subpkstr=sub4,sub_long=4,substr1=s;"
                        "cmd=delete_sub,ts=3,pkstr=main2,subpkstr=sub2;"
                        "cmd=delete,ts=3,pkstr=main3;"
                        "cmd=update_field,ts=3,pkstr=main1,long1=11;"
                        "cmd=update_field,ts=3,pkstr=main2,long1=22,subpkstr=sub1,sub_long=11;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString1, "", ""));
    // reopen
    string docString2 = "cmd=add,ts=10,pkstr=main5,long1=5,text1=m,subpkstr=sub5,sub_long=5,substr1=s;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString2, "pack1:m", 
                    "docid=0,pkstr=main1,long1=11,main_join=0;"
                    "docid=1,pkstr=main2,long1=22,main_join=2;"
                    "docid=3,pkstr=main5,long1=5,main_join=4;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s", 
                    "docid=0,subpkstr=sub1,sub_long=11,sub_join=1;"
                    "docid=3,subpkstr=sub5,sub_long=5,sub_join=3;"));
}

void SubDocTest::TestIncCoverPartialRt()
{
    // same with TestIncCoverEntireRt
    string docString = "cmd=add,ts=0,pkstr=main1,long1=1,text1=m;"
                       "cmd=add,ts=0,pkstr=main2,long1=2,text1=m,subpkstr=sub1^sub2,sub_long=1^2,substr1=s^s;"
                       "cmd=add,ts=0,pkstr=main3,long1=3,text1=m,subpkstr=sub3,sub_long=3,substr1=s;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    string docString1 = "cmd=add,ts=3,pkstr=main4,long1=4,text1=m,subpkstr=sub4,sub_long=4,substr1=s;"
                        "cmd=delete_sub,ts=3,pkstr=main2,subpkstr=sub2;"
                        "cmd=delete,ts=3,pkstr=main3;"
                        "cmd=update_field,ts=3,pkstr=main1,long1=11;"
                        "cmd=update_field,ts=3,pkstr=main2,long1=22,subpkstr=sub1,sub_long=11;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT, docString1, "", ""));
    // reopen
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, docString, "pack1:m", 
                    "docid=3,pkstr=main1,long1=11,main_join=3;"
                    "docid=4,pkstr=main2,long1=22,main_join=5;"
                    "docid=6,pkstr=main4,long1=4,main_join=7;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s", 
                    "docid=3,subpkstr=sub1,sub_long=11,sub_join=4;"
                    "docid=6,subpkstr=sub4,sub_long=4,sub_join=6;"));
}

void SubDocTest::TestMerge()
{
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub2,substr1=s1^s2";
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,main_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, "", "subindex1:s2", 
                                    "docid=1,subpkstr=sub2,sub_join=0;"));

    string docString2 = "cmd=add,pkstr=hello2,ts=1,subpkstr=sub3^sub4,substr1=s3^s4";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, docString2, "pk:hello", 
                                    "docid=0,pkstr=hello,main_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, "", "pk:hello2", 
                                    "docid=1,pkstr=hello2,main_join=4;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, "", "subindex1:s4", 
                                    "docid=3,subpkstr=sub4,sub_join=1;"));
}

void SubDocTest::TestMergeForJoinDocIdWithEmptySubDocAndDupDoc()
{
    DoTestMergeForJoinDocIdWithEmptySubDocAndDupDoc(false);
    DoTestMergeForJoinDocIdWithEmptySubDocAndDupDoc(true);
}

void SubDocTest::DoTestMergeForJoinDocIdWithEmptySubDocAndDupDoc(bool multiTargetSegment)
{
    TearDown();
    SetUp();
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.maxDocCount = 2;
    if (multiTargetSegment)
    {
        std::string mergeConfigStr
            = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"3\"}}";
        autil::legacy::FromJsonString(
            options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
    }
    PartitionStateMachine psm;
    string docString = "cmd=add,pkstr=m1,text1=m,subpkstr=s1^s2,substr1=s^s;"
                       "cmd=add,pkstr=m2,text1=m;"
                       "cmd=add,pkstr=m3,text1=m,subpkstr=s3^s4^s5,substr1=s^s^s;"
                       "cmd=add,pkstr=m4,text1=m,subpkstr=s6,substr1=s;";
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, docString, "pack1:m", 
                    "docid=0,main_join=2;"
                    "docid=1,main_join=2;"
                    "docid=2,main_join=5;"
                    "docid=3,main_join=6;"));    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL, "", "subindex1:s", 
                    "docid=0,sub_join=0;"
                    "docid=1,sub_join=0;"
                    "docid=2,sub_join=2;"
                    "docid=3,sub_join=2;"
                    "docid=4,sub_join=2;"
                    "docid=5,sub_join=3;"));

    string docString2 = "cmd=add,pkstr=m1,text1=m,subpkstr=s2,substr1=s;"
                        "cmd=add,pkstr=m4,text1=m,subpkstr=s6,substr1=s;";

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, docString2, "pack1:m", 
                    "docid=0,main_join=0;"
                    "docid=1,main_join=3;"
                    "docid=2,main_join=4;"
                    "docid=3,main_join=5;"));    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, "", "subindex1:s", 
                    "docid=0,sub_join=1;"
                    "docid=1,sub_join=1;"
                    "docid=2,sub_join=1;"
                    "docid=3,sub_join=2;"
                    "docid=4,sub_join=3;"));   
}

void SubDocTest::TestMergeWithEmptySubSegment()
{
    string docString = "cmd=add,pkstr=hello,subpkstr=sub1^sub2";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL, docString, "pk:hello", "docid=0;"));

    // empty sub segment
    string incDoc = "cmd=add,pkstr=hello5;"
                    "cmd=add,pkstr=hello6;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, incDoc, 
                    "pk:hello6", "docid=2;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", 
                    "sub_pk:sub2", "docid=1;"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC, incDoc, "pk:hello6", "docid=2;"));
}

void SubDocTest::TestMergeWithEmptyMainAndSubSegment()
{
    string docString = "cmd=add,pkstr=hello,long1=1,subpkstr=sub1^sub2,substr1=s1^s1";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL, docString, "pk:hello", 
                    "docid=0,long1=1;"));

    string incDoc = "cmd=update_field,pkstr=hello,long1=2;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC, incDoc, "pk:hello", 
                    "docid=0,long1=2;"));

    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, "", "subindex1:s1", 
                    "docid=0;docid=1;"));
    string delDoc = "cmd=delete_sub,pkstr=hello,subpkstr=sub2;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC, delDoc, "subindex1:s1", 
                    "docid=0;"));
}

void SubDocTest::TestGetDocIdWithCommand(std::string command)
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string fullDocs = "cmd=add,pkstr=hello,long1=1,subpkstr=sub1^sub2,sub_long=1^2,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));

    OnlinePartitionPtr onlinePartition(new OnlinePartition);
    onlinePartition->Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions);

    string docStr = "cmd=" + command + ",pkstr=hello,long1=2,subpkstr=sub1,sub_long=10,ts=2;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(mSchema, docStr);
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder builder(onlinePartition, quotaControl);
    ASSERT_TRUE(builder.Init());
    builder.Build(docs[0]);
    ASSERT_EQ((segmentid_t)0, docs[0]->GetSegmentIdBeforeModified());
}

void SubDocTest::TestGetDocIdAfterUpdateDocument()
{
    TestGetDocIdWithCommand("update_field");
}

void SubDocTest::TestGetDocIdAfterOnlyUpdateSubDocument()
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string fullDocs = "cmd=add,pkstr=hello,long1=1,subpkstr=sub1^sub2,sub_long=1^2,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocs, "", ""));

    OnlinePartitionPtr onlinePartition(new OnlinePartition);
    onlinePartition->Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions);

    string docStr = "cmd=update_field,pkstr=hello,subpkstr=sub2,sub_long=12,ts=2;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(mSchema, docStr);
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilder builder(onlinePartition, quotaControl);
    ASSERT_TRUE(builder.Init());
    builder.Build(docs[0]);
    ASSERT_EQ((segmentid_t)0, docs[0]->GetSubDocuments()[0]->GetSegmentIdBeforeModified());
    
    // ASSERT_EQ((segmentid_t)0, docs[0]->GetSegmentIdBeforeModified());
}

void SubDocTest::TestOptimizedReopenForIncNewerThanRt()
{
    // inc is newer than rt, no operation left, load inc segment patch
    DoTestOptimizedReopen("0,1", "2", "2,3", false);
}

void SubDocTest::TestOptimizedReopenForAllModifiedOnDiskDocUnchanged()
{
    // rt is newer than inc, all operations do not need redo
    DoTestOptimizedReopen("0,1", "2,3", "2", false);
}

void SubDocTest::TestOptimizedReopenForAllModifiedOnDiskDocChanged()
{
    // rt is newer than inc, all on disk segment merged to one segment
    // all operations need redo
    DoTestOptimizedReopen("0,1", "2,3", "2", true);
}

void SubDocTest::TestOptimizedReopenForAllModifiedRtDocBeenCovered()
{
    // all operations need redo
    DoTestOptimizedReopen("", "0,1,2,3", "0,1", false);
}

void SubDocTest::TestOptimizedReopenForAllModifiedRtDocNotBeenCovered()
{
    // all operations need redo
    DoTestOptimizedReopen("0", "4,1,2,3", "4", false);
}

void SubDocTest::TestBuildAndMergeWithPackageFile()
{
    InnerTestBuildAndMergeWithPackageFile(true);
    InnerTestBuildAndMergeWithPackageFile(false);
}

void SubDocTest::TestBuildEmptySegmentInSubSegment()
{
    string fullDoc = "cmd=add,pkstr=main0,long1=0,ts=0;";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.maxDocCount = 1;
    buildConfig.enablePackageFile = true;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    file_system::DirectoryPtr segmentDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0_level_0", true);
    FileList fileList;
    segmentDirectory->ListFile("", fileList, true, true);
    
    string deployContent;
    segmentDirectory->Load("deploy_index", deployContent);

    DeployIndexMeta deployIndexMeta;
    deployIndexMeta.FromString(deployContent);

    FileList expectFileList;
    for (size_t i = 0; i < deployIndexMeta.deployFileMetas.size(); ++i) {
        expectFileList.push_back(deployIndexMeta.deployFileMetas[i].filePath);
    }
    expectFileList.push_back("deploy_index");
    expectFileList.push_back("segment_file_list");

    sort(expectFileList.begin(), expectFileList.end());
    ASSERT_EQ(expectFileList.size(), fileList.size());
    for (size_t i = 0; i < fileList.size(); i++)
    {
        ASSERT_EQ(expectFileList[i], fileList[i]);
    }
}

void SubDocTest::InnerTestBuildAndMergeWithPackageFile(bool optimizeMerge)
{
    TearDown();
    SetUp();

    string fullDoc = "cmd=add,pkstr=main0,long1=0,subpkstr=sub0,sub_long=0,ts=0;"
                     "cmd=add,pkstr=main1,long1=1,subpkstr=sub10^sub11,sub_long=10^11,ts=10;"
                     "cmd=add,pkstr=main2,long1=2,subpkstr=sub20^sub22,sub_long=20^22,ts=20;"
                     "cmd=add,pkstr=main3,long1=3,subpkstr=sub30^sub33,sub_long=30^33,ts=30;"
                     "cmd=add,pkstr=main4,long1=4,subpkstr=sub40^sub44,sub_long=40^44,ts=40;"
                     "cmd=add,pkstr=main5,long1=5,subpkstr=sub50^sub55,sub_long=50^55,ts=50;"
                     "cmd=add,pkstr=main6,long1=6,subpkstr=sub60^sub66,sub_long=60^66,ts=60;"
                     "cmd=delete,pkstr=main1,ts=110;"
                     "cmd=add,pkstr=main2,long1=22,subpkstr=sub220^sub222,sub_long=220^222,ts=120;"
                     "cmd=delete_sub,pkstr=main3,subpkstr=sub33,ts=130;"
                     "cmd=update_field,pkstr=main4,long1=14,ts=140;"
                     "cmd=update_field,pkstr=main5,subpkstr=sub50^sub55,sub_long=550^555,ts=150;"
                     "cmd=update_field,pkstr=main6,long1=66,subpkstr=sub60^sub66,sub_long=660^666,ts=160;";

    IndexPartitionOptions options;
    BuildConfig& buildConfig = options.GetOfflineConfig().buildConfig;
    buildConfig.maxDocCount = 1;
    buildConfig.enablePackageFile = true;

    if (!optimizeMerge)
    {
        options.GetMergeConfig().mergeStrategyStr = "specific_segments";
        options.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
                "merge_segments=2,8");
    }

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));

    for (size_t i = 0; i <= 9; i++)
    {
        stringstream ss;
        ss << "segment_" << i << "_level_0";
        CheckPackageFile(mRootDir + ss.str());
    }

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC, "", "", ""));
    
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:main1", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:main2", "long1=22"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub33", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:main4", "long1=14"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub50", "sub_long=550"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub55", "sub_long=555"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:main6", "long1=66"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub60", "sub_long=660"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub66", "sub_long=666"));

    DeployIndexChecker::CheckDeployIndexMeta(mRootDir, 0, INVALID_VERSION);
    DeployIndexChecker::CheckDeployIndexMeta(mRootDir, 1, INVALID_VERSION);
    DeployIndexChecker::CheckDeployIndexMeta(mRootDir, 1, 0);
}

void SubDocTest::DoTestOptimizedReopen(const string& fullDocSeq,
                                       const string& rtDocSeq,
                                       const string& incDocSeq,
                                       bool isIncMerge)
{
    vector<string> docs;
    docs.push_back("cmd=add,pkstr=main0,long1=0,subpkstr=sub0,sub_long=0,ts=0;");
    docs.push_back("cmd=add,pkstr=main1,long1=1,subpkstr=sub10^sub11,sub_long=10^11,ts=10;"
                   "cmd=add,pkstr=main2,long1=2,subpkstr=sub20^sub22,sub_long=20^22,ts=20;"
                   "cmd=add,pkstr=main3,long1=3,subpkstr=sub30^sub33,sub_long=30^33,ts=30;"
                   "cmd=add,pkstr=main4,long1=4,subpkstr=sub40^sub44,sub_long=40^44,ts=40;"
                   "cmd=add,pkstr=main5,long1=5,subpkstr=sub50^sub55,sub_long=50^55,ts=50;"
                   "cmd=add,pkstr=main6,long1=6,subpkstr=sub60^sub66,sub_long=60^66,ts=60;"
                   "cmd=add,pkstr=main8,long1=8,subpkstr=sub80^sub88,sub_long=80^88,ts=61;"
                   "cmd=add,pkstr=main9,long1=9,subpkstr=sub90^sub99,sub_long=90^99,ts=62;");

    docs.push_back("cmd=add,pkstr=main7,long1=7,subpkstr=sub70^sub77,sub_long=70^77,ts=70;"
                   "cmd=delete,pkstr=main8,ts=71;"
                   "cmd=delete_sub,pkstr=main9,subpkstr=sub99,ts=72;");

    docs.push_back("cmd=delete,pkstr=main1,ts=110;"
                   "cmd=add,pkstr=main2,long1=22,subpkstr=sub220^sub222,sub_long=220^222,ts=120;"
                   "cmd=delete_sub,pkstr=main3,subpkstr=sub33,ts=130;"
                   "cmd=update_field,pkstr=main4,long1=14,ts=140;"
                   "cmd=update_field,pkstr=main5,subpkstr=sub50^sub55,sub_long=550^555,ts=150;"
                   "cmd=update_field,pkstr=main6,long1=66,subpkstr=sub60^sub66,sub_long=660^666,ts=160;");

    docs.push_back("cmd=update_field,pkstr=main0,long1=0,subpkstr=sub0,sub_long=0,ts=1;");

    string fullDoc = GetDoc(docs, fullDocSeq);
    string rtDoc = GetDoc(docs, rtDocSeq);
    string incDoc = GetDoc(docs, incDocSeq);

    IndexPartitionOptions options;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));
    if (isIncMerge)
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
    }
    else
    {
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc, "", ""));
    }

    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:main1", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:main2", "long1=22"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub33", ""));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:main4", "docid=4,long1=14"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub50", "docid=9,sub_long=550"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub55", "docid=10,sub_long=555"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "pk:main6", "docid=6,long1=66"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub60", "docid=11,sub_long=660"));
    EXPECT_TRUE(psm.Transfer(QUERY, "", "sub_pk:sub66", "docid=12,sub_long=666"));
}

string SubDocTest::GetDoc(const vector<string>& docs,
                          const string& docSeq)
{
    vector<size_t> docIndexes;
    StringUtil::fromString(docSeq, docIndexes, ",");

    string docStr = "";
    for (size_t i = 0; i < docIndexes.size(); ++i)
    {
        docStr += docs[docIndexes[i]];
    }
    return docStr;
}

void SubDocTest::CheckPackageFile(const string& segmentDirPath)
{
    FileList fileList;
    FileSystemWrapper::ListDirRecursive(segmentDirPath, fileList);
    FileList expectFileList { "deploy_index",
            "counter",
            "package_file.__data__0",
            "package_file.__meta__", 
            "segment_info",
            "segment_file_list",
            "sub_segment/",
            "sub_segment/package_file.__data__0",
            "sub_segment/package_file.__meta__",
            "sub_segment/segment_info",
            "sub_segment/counter" };
    sort(fileList.begin(), fileList.end());
    sort(expectFileList.begin(), expectFileList.end());
    ASSERT_EQ(expectFileList, fileList);
}

bool SubDocTest::CheckAccessCounter(const string& name, int expect)
{
    const IndexPartitionPtr& partition = mPsm.GetIndexPartition();
    IndexPartitionReaderPtr reader = partition->GetReader();
    IndexPartitionReader::AccessCounterMap counters;
    if (name == "pack1" || name == "subindex1"
        || name == "pk" || name == "sub_pk")
    {
        counters = reader->GetIndexAccessCounters();
    }
    else if (name == "long1" || name == "sub_long")
    {
        counters = reader->GetAttributeAccessCounters();
    }
    else
    {
        assert(false);
    }

    typedef IndexPartitionReader::AccessCounterMap::const_iterator Iterator;


    Iterator iter = counters.find(name);
    if (iter == counters.end())
    {
        EXPECT_TRUE(false) << name;
        return false;
    }

    int64_t actual = iter->second->Get();
    if (actual != expect)
    {
        EXPECT_EQ(expect, actual) << name;
        return false;
    }
    return true;
}

void SubDocTest::TestAccessCounters()
{
    // FULL
    string fullDoc = "cmd=add,pkstr=pk1,text1=m,long1=1,subpkstr=sub1,substr1=s,sub_long=11,ts=0;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "pack1:m", 
                    "docid=0,long1=1"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "", "subindex1:s", 
                    "docid=0,sub_long=11;"));
    ASSERT_TRUE(CheckAccessCounter("pack1", 1));
    ASSERT_TRUE(CheckAccessCounter("long1", 1));
    ASSERT_TRUE(CheckAccessCounter("subindex1", 1));
    ASSERT_TRUE(CheckAccessCounter("sub_long", 1));
    // INC
    string incDoc = "cmd=add,pkstr=pk1,text1=m,long1=1,subpkstr=sub1,substr1=s,sub_long=11,ts=10;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_INC_NO_MERGE, incDoc, "pack1:m", 
                    "docid=1,long1=1"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "", "subindex1:s", 
                    "docid=1,sub_long=11;"));
    ASSERT_TRUE(CheckAccessCounter("pack1", 2));
    ASSERT_TRUE(CheckAccessCounter("long1", 2));
    ASSERT_TRUE(CheckAccessCounter("subindex1", 2));
    ASSERT_TRUE(CheckAccessCounter("sub_long", 2));
    // RT
    string rtDoc = "cmd=add,pkstr=pk1,text1=m,long1=1,subpkstr=sub1,substr1=s,sub_long=11,ts=20;";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_RT_SEGMENT, rtDoc, "pack1:m", 
                    "docid=2,long1=1"));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "", "subindex1:s", 
                    "docid=2,sub_long=11;"));
    ASSERT_TRUE(CheckAccessCounter("pack1", 3));
    ASSERT_TRUE(CheckAccessCounter("long1", 3));
    ASSERT_TRUE(CheckAccessCounter("subindex1", 3));
    ASSERT_TRUE(CheckAccessCounter("sub_long", 3));

    ASSERT_TRUE(CheckAccessCounter("pk", 0));
    ASSERT_TRUE(CheckAccessCounter("sub_pk", 0));
}

void SubDocTest::TestAddMainSubDocWithMultiInMemSegments()
{
    SlowDumpSegmentContainerPtr slowDumpContainer(new SlowDumpSegmentContainer(
                    3600u * 24 * 30 * 1000, false, true));
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                              IndexPartitionResource(), slowDumpContainer);
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    slowDumpContainer->DisableSlowSleep();

    // full build
    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub2,substr1=s1^s2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "pk:hello", 
                                    "docid=0,pkstr=hello,main_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s2", 
                                    "docid=1,subpkstr=sub2,sub_join=0;"));

    // inc build
    string docString2 = "cmd=add,pkstr=hello2,ts=1,subpkstr=sub3^sub4,substr1=s3^s4;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString2, "pk:hello2", 
                                    "docid=1,pkstr=hello2,main_join=4;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s4", 
                                    "docid=3,subpkstr=sub4,sub_join=1;"));

    // dumping rt segment1
    slowDumpContainer->EnableSlowSleep();
    string docStringRt1 = "cmd=add,pkstr=hello3,ts=2,subpkstr=sub5,substr1=s5;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docStringRt1, "pk:hello3", 
                                    "docid=2,pkstr=hello3,main_join=5;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s5", 
                                    "docid=4,subpkstr=sub5,sub_join=2;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "__subdocid:4", 
                                    "docid=4,subpkstr=sub5,sub_join=2;"));
    
    // dumping rt segment2
    string docStringRt2 = "cmd=add,pkstr=hello4,ts=3,subpkstr=sub6^sub7,substr1=s6^s7;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, docStringRt2, "pk:hello4", 
                                    "docid=3,pkstr=hello4,main_join=7;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s7", 
                                    "docid=6,subpkstr=sub7,sub_join=3;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "__subdocid:5", 
                                    "docid=5,subpkstr=sub6,sub_join=3;"));

    // building segment
    string buildingRtDoc = "cmd=add,pkstr=hello5,ts=4,subpkstr=sub8,substr1=s8;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, buildingRtDoc, "pk:hello5", 
                                    "docid=4,pkstr=hello5,main_join=8;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s8", 
                                    "docid=7,subpkstr=sub8,sub_join=4;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "__subdocid:7", 
                                    "docid=7,subpkstr=sub8,sub_join=4;"));

    // delete doc in dumping segment
    string delDoc = "cmd=delete,pkstr=hello3,ts=5;"
                    "cmd=delete_sub,pkstr=hello4,subpkstr=sub6,ts=6;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, delDoc, "pk:hello3", "")); // del main, query main
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s5", ""));  // del main, query sub
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s6", ""));  // del sub, query sub
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s7", //del sub,query valid sub in same main
                                    "docid=6,subpkstr=sub7,sub_join=3;"));
    slowDumpContainer->DisableSlowSleep();
    slowDumpContainer->WaitEmpty();
    psm.Transfer(PE_REOPEN, "", "", "");

    // check again after dumping segment to built segment
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:hello3", "")); // del main, query main
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s5", ""));  // del main, query sub
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s6", ""));  // del sub, query sub
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s7", //del sub,query valid sub in same main
                                    "docid=6,subpkstr=sub7,sub_join=3;"));

    // inc cover one rt segment, include docStringRt1
    string newInc = docStringRt1 + 
                    "cmd=add,pkstr=new_hello,ts=3,subpkstr=sub_new,substr1=s_new;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, newInc, "pk:new_hello",
                                    "docid=3,pkstr=new_hello,main_join=6"));
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "pk:hello3", "")); // del main, query main
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s5", ""));  // del main, query sub
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s6", ""));  // del sub, query sub
    INDEXLIB_TEST_TRUE(psm.Transfer(QUERY, "", "subindex1:s7", //del sub,query valid sub in same main
                                    "docid=7,subpkstr=sub7,sub_join=4;")); // * 
}

void SubDocTest::TestMergeWithPackageFile()
{
    bool enableTag = std::get<0>(GET_CASE_PARAM());
    bool zeroCheckpoint = std::get<1>(GET_CASE_PARAM());
    bool debug = true;

    mOptions.GetOnlineConfig().onlineKeepVersionCount = 100;
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 1;
    mOptions.GetOfflineConfig().buildConfig.enablePackageFile = true;
    mOptions.GetOfflineConfig().buildConfig.keepVersionCount = 100;
    mOptions.GetMergeConfig().SetEnablePackageFile(true);
    if (enableTag)
    {
        mOptions.GetMergeConfig().mImpl->packageFileTagConfigList.TEST_PATCH();
    }

    mOptions.GetMergeConfig().mergeStrategyStr = "specific_segments";
    mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString("merge_segments=0");

    if (zeroCheckpoint) { mOptions.GetMergeConfig().SetCheckpointInterval(0); }

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    
    // output: BUILD[0] = [0], MERGE[1] = [1]
    string docString0 =
        "cmd=add,pkstr=mpk1,long1=11,ts=0,subpkstr=spk1^spk2,substr1=ss1^ss2";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString0, "pk:mpk1", 
                             "docid=0,pkstr=mpk1,long1=11,main_join=2;"));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "subindex1:ss2", 
                             "docid=1,subpkstr=spk2,sub_join=0;"));

    // output: BUILD[2] = [1,2,3], MERGE[3] = [4]
    MergeConfig mergeConfig = mOptions.GetMergeConfig();
    mergeConfig.mergeStrategyStr = "balance_tree";
    mergeConfig.mergeStrategyParameter.SetLegacyString(
            "base-doc-count=1;max-doc-count=2;conflict-segment-number=3;");
    psm.SetMergeConfig(mergeConfig);
    string docString1 =
        "cmd=add,pkstr=mpk2,long1=12,ts=1,subpkstr=spk3^spk4,substr1=ss3^ss4,sub_long=30^40;"
        "cmd=add,pkstr=mpk3,long1=13,ts=1";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString1, "pk:mpk1", 
                             "docid=0,pkstr=mpk1,long1=11,main_join=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:mpk2", 
                             "docid=1,pkstr=mpk2,long1=12,main_join=4;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:mpk3", 
                             "docid=2,pkstr=mpk3,long1=13,main_join=4;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "subindex1:ss4", 
                             "docid=3,subpkstr=spk4,sub_join=1;"));

    // output: BUILD[4] = [4,5], MERGE[5] = [4,6]
    mergeConfig.mergeStrategyStr = "specific_segments";
    mergeConfig.mergeStrategyParameter.SetLegacyString("merge_segments=5");
    psm.SetMergeConfig(mergeConfig);
    string docString2 =
        "cmd=update_field,pkstr=mpk2,long1=121,ts=2,subpkstr=spk4,sub_long=401;"
        "cmd=delete,pkstr=mpk3,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString2, "pk:mpk1", 
                             "docid=0,pkstr=mpk1,long1=11,main_join=2;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:mpk2", 
                             "docid=1,pkstr=mpk2,long1=121,main_join=4;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:mpk3", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "subindex1:ss4", 
                             "docid=3,subpkstr=spk4,sub_long=401,sub_join=1;"));

    // Final Check
    PackageFileMeta packageMeta6;
    PackageFileMeta subPackageMeta6;
    IndexFileList indexFileList6;

    string segment6 = PathUtil::JoinPath(mRootDir, "segment_6_level_0");
    ASSERT_TRUE(packageMeta6.Load(segment6));
    const auto& dataFileNames = packageMeta6.GetPhysicalFileNames();
    const auto& dataFileTags = packageMeta6.GetPhysicalFileTags();
    const auto& dataFileLengths = packageMeta6.GetPhysicalFileLengths();

    ASSERT_TRUE(subPackageMeta6.Load(PathUtil::JoinPath(segment6, "sub_segment")));
    const auto& subDataFileNames = subPackageMeta6.GetPhysicalFileNames();
    const auto& subDataFileTags = subPackageMeta6.GetPhysicalFileTags();
    const auto& subDataFileLengths = subPackageMeta6.GetPhysicalFileLengths();

    ASSERT_TRUE(SegmentFileListWrapper::Load(segment6, indexFileList6));
    const auto& indexFiles = indexFileList6.deployFileMetas;

    map<string, size_t> allIndxFiles;
    for (size_t i = 0; i < indexFiles.size(); ++i)
    {
        if (debug) {
            cout << "##[" << getpid() << "]IFL: "
                 << indexFiles[i].filePath << ":" << indexFiles[i].fileLength << endl;
        }
        allIndxFiles[indexFiles[i].filePath] = indexFiles[i].fileLength;
    }
    ASSERT_EQ(indexFiles.size(), allIndxFiles.size());

    map<string, size_t> allPackage;
    bool seenPatchTag = false;
    for (size_t i = 0; i < dataFileNames.size(); ++i)
    {
        if (debug) {
            cout << "##[" << getpid() << "]MPFM: "
                 << dataFileNames[i] << ":" << dataFileTags[i] << ":" << dataFileLengths[i] << endl;
        }
        ASSERT_TRUE(dataFileNames[i].find(dataFileTags[i]) != string::npos);
        allPackage[dataFileNames[i]] = dataFileLengths[i];
        ASSERT_TRUE(allIndxFiles.count(dataFileNames[i]) > 0);
        ASSERT_EQ(allIndxFiles[dataFileNames[i]], dataFileLengths[i]);
        if (dataFileNames[i].find("PATCH") != string::npos) {
            seenPatchTag = true;
        }
    }
    ASSERT_EQ(enableTag, seenPatchTag);

    seenPatchTag = false;
    for (size_t i = 0; i < subDataFileNames.size(); ++i)
    {
        if (debug) {
            cout << "##[" << getpid() << "]SPFM: "
                 <<  subDataFileNames[i] << ":" << subDataFileTags[i] << ":" << subDataFileLengths[i] << endl;
        }
        ASSERT_TRUE(subDataFileNames[i].find(subDataFileTags[i]) != string::npos);
        string name = "sub_segment/" + subDataFileNames[i];
        allPackage[name] = subDataFileLengths[i];
        ASSERT_TRUE(allIndxFiles.count(name) > 0) << name;
        ASSERT_EQ(allIndxFiles[name], subDataFileLengths[i]);
        if (subDataFileNames[i].find("PATCH") != string::npos) {
            seenPatchTag = true;
        }
    }
    ASSERT_EQ(enableTag, seenPatchTag);
    ASSERT_EQ(dataFileNames.size() + subDataFileNames.size(), allPackage.size());
    if (debug) { cout << endl; }
}

IE_NAMESPACE_END(partition);

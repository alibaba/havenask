#include <autil/StringUtil.h>
#include "indexlib/partition/test/online_use_path_meta_cache_intetest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index_base/version_loader.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlineUsePathMetaCacheTest);

OnlineUsePathMetaCacheTest::OnlineUsePathMetaCacheTest()
{
}

OnlineUsePathMetaCacheTest::~OnlineUsePathMetaCacheTest()
{
}

void OnlineUsePathMetaCacheTest::CaseSetUp()
{
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    mPrimaryPath = rootDirectory->MakeDirectory("primary")->GetPath();
    mSecondaryPath = rootDirectory->MakeDirectory("secondary")->GetPath();
    
    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(false).enablePackageFile = false;
    autil::legacy::FromJsonString(mOptions.GetOfflineConfig().mergeConfig, R"( {
        "truncate_strategy" : [ {
            "strategy_name" : "distinct_sort",
            "threshold" : 2,
            "limit" : 2,
	        "truncate_profiles": [
        		"trun_price"
    	    ]
        }]
    } )");
    
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "price:uint32;multi_long:uint64:true;";
    string index = "pk:primarykey64:pk;index1:number:price:true;pack1:pack:text1;";
    string attr = "price;multi_long;pk;";
    string summary = "string1;";
    string truncate = "trun_price:-price";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary, truncate);

    SchemaMaker::SetAdaptiveHighFrequenceDictionary(
        "index1", "DOC_FREQUENCY#2", hp_both, mSchema);
}

void OnlineUsePathMetaCacheTest::CaseTearDown()
{
    FileSystemWrapper::ClearError();
}

void OnlineUsePathMetaCacheTest::InitDocs()
{
    mFullDocs = "cmd=add,pk=1,string1=hello,price=1,multi_long=3;"
                "cmd=add,pk=2,string1=hello,price=2,multi_long=3;"
                "cmd=add,pk=3,string1=hello,price=3,multi_long=3;"
                "cmd=add,pk=4,string1=hello,price=4,multi_long=3;"
                "cmd=add,pk=5,string1=hello,price=4,multi_long=3;";
    mIncDocs1 = "cmd=add,pk=20,text1=t1 t2,price=20";
    mIncDocs2 = "cmd=add,pk=30,price=30";
}

void OnlineUsePathMetaCacheTest::CheckDocs(PartitionStateMachine& psm)
{
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:1", "string1=hello,price=1,multi_long=3;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pack1:t1", "price=20;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:30", "price=30;"));
}

void OnlineUsePathMetaCacheTest::TestIndexPartialRemote()
{
    InitDocs();
    string jsonStr = R"( {
            "online_keep_version_count": 1,
            "enable_validate_index": false,
            "need_read_remote_index" : true,
            "load_config" : [
                { "file_patterns" : ["segment_1_level_0/index/"], "remote": false, "deploy": true},
                { "file_patterns" : ["segment_3_level_0/index/"], "remote": false, "deploy": true},
                { "file_patterns" : [".*"], "remote": true }
            ]
        } )";
    OnlineConfig onlineConfig;
    FromJsonString(onlineConfig, jsonStr);
    mOptions.SetOnlineConfig(onlineConfig);
    mOptions.GetBuildConfig(true).usePathMetaCache = true;

    PartitionStateMachine psm;
    {
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mPrimaryPath, "psm", mSecondaryPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_REOPEN, mFullDocs, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "/segment_1_level_0/index");

    {  // seg [1] -> [3]
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs1, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "/segment_3_level_0/index");

    {  // seg [3] -> [3,4]
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, mIncDocs2, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "/segment_3_level_0/index");
    
    CheckDocs(psm);
}

void OnlineUsePathMetaCacheTest::TestIndexAllRemote()
{
    InitDocs();
    string jsonStr = R"( {
            "online_keep_version_count": 1,
            "enable_validate_index": false,
            "need_read_remote_index" : true,
            "load_config" : [
                { "file_patterns" : [".*"], "remote": true, "deploy": false }
            ]
        } )";
    OnlineConfig onlineConfig;
    FromJsonString(onlineConfig, jsonStr);
    mOptions.SetOnlineConfig(onlineConfig);
    mOptions.GetBuildConfig(true).usePathMetaCache = true;

    PartitionStateMachine psm;
    {
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mPrimaryPath, "psm", mSecondaryPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_REOPEN, mFullDocs, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "");

    {  // seg [1] -> [3]
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs1, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "");

    {  // seg [3] -> [3,4]
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, mIncDocs2, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "");
    
    CheckDocs(psm);
}

void OnlineUsePathMetaCacheTest::TestIndexAllLocal()
{
    InitDocs();
    string jsonStr = R"( {
            "online_keep_version_count": 1,
            "enable_validate_index": false,
            "need_read_remote_index" : false,
            "load_config" : [
                { "file_patterns" : [".*"], "remote": false, "deploy": true}
            ]
        } )";
    OnlineConfig onlineConfig;
    FromJsonString(onlineConfig, jsonStr);
    mOptions.SetOnlineConfig(onlineConfig);
    mOptions.GetBuildConfig(true).usePathMetaCache = true;

    PartitionStateMachine psm;
    {
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Init(mSchema, mOptions, mPrimaryPath, "psm", mSecondaryPath));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_REOPEN, mFullDocs, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "*");

    {  // seg [1] -> [3]
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs1, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "*");

    {  // seg [3] -> [3,4]
        SCOPED_LOG_LEVEL(ERROR);
        ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, mIncDocs2, "", ""));
    }
    CheckPathMetaCache(psm, GET_CASE_PARAM(), "*");
    
    CheckDocs(psm);
}

void OnlineUsePathMetaCacheTest::TestPackageIndexPartialRemote()
{
    for (bool isMMapLock : {true, false})
    {
        TearDown();
        SetUp();
        InitDocs();
        string jsonStr = R"( {
            "online_keep_version_count": 1,
            "enable_validate_index": false,
            "need_read_remote_index" : true,
            "load_config" : [
                { "file_patterns" : ["segment_1_level_0/package_file.__data__0"], "remote": false, "deploy": true},
                { "file_patterns" : ["segment_3_level_0/package_file.__data__0"], "remote": false, "deploy": true},
                { 
                    "file_patterns" : [".*"], "remote": true, 
                    "load_strategy": "mmap", 
                    "load_strategy_param": { "lock": __IS_MMAP_LOCK__ }
                }
            ]
        } )";
        autil::StringUtil::replaceAll(jsonStr, "__IS_MMAP_LOCK__", isMMapLock ? "true" : "false");
        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, jsonStr);
        mOptions.SetOnlineConfig(onlineConfig);
        mOptions.GetBuildConfig(true).usePathMetaCache = true;
        mOptions.GetBuildConfig(false).enablePackageFile = true;
        mOptions.GetMergeConfig().SetEnablePackageFile(true);
        
        PartitionStateMachine psm;
        {
            SCOPED_LOG_LEVEL(ERROR);
            ASSERT_TRUE(psm.Init(mSchema, mOptions, mPrimaryPath, "psm", mSecondaryPath));
            ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_REOPEN, mFullDocs, "", ""));
        }
        CheckPathMetaCache(psm, GET_CASE_PARAM(), "/segment_1_level_0/package_file.__data__0");
    
        {  // seg [1] -> [3]
            SCOPED_LOG_LEVEL(ERROR);
            ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_REOPEN, mIncDocs1, "", ""));
        }
        CheckPathMetaCache(psm, GET_CASE_PARAM(), "/segment_3_level_0/package_file.__data__0");
    
        {  // seg [3] -> [3,4]
            SCOPED_LOG_LEVEL(ERROR);
            ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, mIncDocs2, "", ""));
        }
        CheckPathMetaCache(psm, GET_CASE_PARAM(), "/segment_3_level_0/package_file.__data__0");
        
        CheckDocs(psm);
    }
}

// @param dpToLocalFile: actual use [move] instead of [copy] to make sure original files will not be visited, and will move back to recover at last. "*" means all, "" means nothing
void OnlineUsePathMetaCacheTest::CheckPathMetaCache(
    PartitionStateMachine& psm, OnlineUsePathMetaCacheTest_OnlinePartitionEvent event,
    string dpToLocalFile)
{
    if (dpToLocalFile == "*")  // all
    {
        FileSystemWrapper::DeleteDir(mPrimaryPath);
        FileSystemWrapper::Rename(mSecondaryPath, mPrimaryPath);
        FileSystemWrapper::MkDir(mSecondaryPath);
    }
    else if (dpToLocalFile == "")
    {
        // do nothing
    }
    else
    {
        FileSystemWrapper::Rename(PathUtil::JoinPath(mSecondaryPath, dpToLocalFile),
                                  PathUtil::JoinPath(mPrimaryPath, dpToLocalFile));
    }
    fslib::FileList versionFileList;
    VersionLoader::ListVersion(dpToLocalFile == "*" ? mPrimaryPath : mSecondaryPath, versionFileList);
    for (size_t i = versionFileList.size(); i < versionFileList.size(); ++i)
    {
        string dstPath = PathUtil::JoinPath(mPrimaryPath, versionFileList[i]);
        FileSystemWrapper::DeleteIfExist(dstPath);
        FileSystemWrapper::Copy(PathUtil::JoinPath(mSecondaryPath, versionFileList[i]), dstPath);
    }
    versionid_t lastVersion = VersionLoader::GetVersionId(versionFileList.back());
    
    auto assertMetaCached = [](const string& fileName) {
         FileSystemWrapper::SetError(FileSystemWrapper::IS_EXIST_IO_ERROR, fileName, UINT32_MAX);
         FileSystemWrapper::SetError(FileSystemWrapper::GET_FILE_META_ERROR, fileName, UINT32_MAX);
    };
    assertMetaCached("/secondary(/?)$");
    assertMetaCached("/secondary/segment_[0-9]{1,2}_level_0.*");
    assertMetaCached("/secondary/adaptive_bitmap_meta.*");
    assertMetaCached("/secondary/truncate_meta.*");
    assertMetaCached("/secondary/(schema.json|index_format_version)");
    FileSystemWrapper::SetError(FileSystemWrapper::LIST_DIR_ERROR, "/secondary(/?)$", UINT32_MAX);

    if (event == OnlineUsePathMetaCacheTest_OnlinePartitionEvent::OPE_OPEN
        || !psm.GetIndexPartition())
    {
        ASSERT_TRUE(psm.CreateIndexPartition(lastVersion));
    }
    if (event == OnlineUsePathMetaCacheTest_OnlinePartitionEvent::OPE_NORMAL_REOPEN ||
        event == OnlineUsePathMetaCacheTest_OnlinePartitionEvent::OPE_FORCE_REOPEN)
    {
        bool isForce = (event == OnlineUsePathMetaCacheTest_OnlinePartitionEvent::OPE_FORCE_REOPEN);
        ASSERT_EQ(IndexPartition::OS_OK, psm.GetIndexPartition()->ReOpen(isForce, lastVersion));
    }
    FileSystemWrapper::ClearError();
    

    if (dpToLocalFile == "*")  // all
    {
        FileSystemWrapper::DeleteDir(mSecondaryPath);
        FileSystemWrapper::Rename(mPrimaryPath, mSecondaryPath);
        FileSystemWrapper::MkDir(mPrimaryPath);
    }
    else if (dpToLocalFile == "")
    {
        // do nothing
    }
    else
    {
        FileSystemWrapper::Rename(PathUtil::JoinPath(mPrimaryPath, dpToLocalFile),
                                  PathUtil::JoinPath(mSecondaryPath, dpToLocalFile));
        FileSystemWrapper::DeleteDir(mPrimaryPath);
        FileSystemWrapper::MkDir(mPrimaryPath);
    }
}

IE_NAMESPACE_END(partition);

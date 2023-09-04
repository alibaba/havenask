#include "indexlib/table/kkv_table/index_task/KKVIndexMergeOperation.h"

#include "indexlib/config/TabletOptions.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

class KKVIndexMergeOperationTest : public TESTBASE
{
public:
    KKVIndexMergeOperationTest() {}
    ~KKVIndexMergeOperationTest() {}

public:
    void setUp() override {}
    void tearDown() override {}

private:
    AUTIL_LOG_DECLARE();
};

class FakeIndexTaskContext : public framework::IndexTaskContext
{
public:
    Status GetAllFenceRootInTask(std::vector<std::string>* fenceResults) const override
    {
        *fenceResults = _fenceDirs;
        return _status;
    }
    void AddFenceDir(std::string fenceDir) { _fenceDirs.push_back(fenceDir); }
    void SetGetFenceDirStatus(Status status) { _status = status; }

private:
    Status _status;
    std::vector<std::string> _fenceDirs;
};

TEST_F(KKVIndexMergeOperationTest, TestGetStoreTs)
{
    {
        std::string jsonStr = R"( {
    "offline_index_config": {
        "full_index_store_kkv_ts": false,
        "build_config": {
            "use_user_timestamp" : false
        }
    }
    } )";
        auto tabletOptions = std::make_shared<config::TabletOptions>();
        FromJsonString(*tabletOptions, jsonStr);
        FakeIndexTaskContext context;
        context.SetTabletOptions(tabletOptions);
        framework::IndexOperationDescription desc;
        KKVIndexMergeOperation mergeOperation(desc);
        auto [status, storeTs] = mergeOperation.Test_GetStoreTs(context);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(storeTs);
    }
    {
        std::string jsonStr = R"( {
    "offline_index_config": {
        "full_index_store_kkv_ts": false,
        "build_config": {
            "use_user_timestamp" : false
        }
    }
    } )";
        auto tabletOptions = std::make_shared<config::TabletOptions>();
        FromJsonString(*tabletOptions, jsonStr);
        FakeIndexTaskContext context;
        context.AddParameter<bool>("optimize_merge", true);
        context.SetTabletOptions(tabletOptions);
        framework::IndexOperationDescription desc;
        KKVIndexMergeOperation mergeOperation(desc);
        auto [status, storeTs] = mergeOperation.Test_GetStoreTs(context);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(storeTs);
    }
    {
        std::string jsonStr = R"( {
    "offline_index_config": {
        "build_config": {
            "use_user_timestamp" : true
        }
    }
    } )";
        auto tabletOptions = std::make_shared<config::TabletOptions>();
        FromJsonString(*tabletOptions, jsonStr);
        FakeIndexTaskContext context;
        context.SetTabletOptions(tabletOptions);
        context.AddParameter<bool>("optimize_merge", false);
        framework::IndexOperationDescription desc;
        KKVIndexMergeOperation mergeOperation(desc);
        auto [status, storeTs] = mergeOperation.Test_GetStoreTs(context);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(storeTs);
    }
    {
        std::string jsonStr = R"( {
    "offline_index_config": {
        "full_index_store_kkv_ts": 123,
        "build_config": {
            "use_user_timestamp" : true
        }
    }
    } )";
        auto tabletOptions = std::make_shared<config::TabletOptions>();
        FromJsonString(*tabletOptions, jsonStr);
        FakeIndexTaskContext context;
        context.SetTabletOptions(tabletOptions);
        context.AddParameter<bool>("optimize_merge", false);
        framework::IndexOperationDescription desc;
        KKVIndexMergeOperation mergeOperation(desc);
        auto [status, storeTs] = mergeOperation.Test_GetStoreTs(context);
        ASSERT_TRUE(status.IsConfigError());
    }
}
}} // namespace indexlibv2::table
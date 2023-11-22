#include "suez/drc/SourceUpdate2Add.h"

#include "indexlib/base/Constant.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/table/normal_table/test/NormalTableTestHelper.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "suez/drc/LogRecordBuilder.h"
#include "unittest/unittest.h"

namespace suez {

class SourceUpdate2AddTest : public TESTBASE {
protected:
    void prepareIndex() {
        _helper = std::make_unique<indexlibv2::table::NormalTableTestHelper>();
        auto schema = indexlibv2::table::NormalTabletSchemaMaker::Make(
            "pk:string;f2:int32;f3:int64:true", "pk:primarykey64:pk", "pk;f2;f3", "", "", "pk;f2;f3");
        ASSERT_TRUE(schema) << "make schema failed";
        auto tabletOptions = makeTabletOptions();
        ASSERT_TRUE(tabletOptions);

        indexlibv2::framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
        auto s = _helper->Open(indexRoot, std::move(schema), std::move(tabletOptions));
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        auto docsStr = "cmd=add,pk=a,f2=1,f3=111 1;"
                       "cmd=add,pk=b,f2=2,f3=222 2;"
                       "cmd=add,pk=c,f2=3,f3=333 3;"
                       "cmd=add,pk=d,f2=4,f3=444 4;"
                       "cmd=add,pk=e,f2=5,f3=555 5;";
        s = _helper->Build(docsStr);
        ASSERT_TRUE(s.IsOK()) << s.ToString();
    }

    std::shared_ptr<indexlibv2::config::TabletOptions> makeTabletOptions() const {
        std::string jsonStr = R"( {
        "online_index_config": {
            "build_config": {
                "sharding_column_num" : 1,
                "level_num" : 3,
                "building_memory_limit_mb" : 64
            },
            "need_deploy_index" : false,
            "need_read_remote_index" : true
        }
        } )";
        auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
        try {
            FastFromJsonString(*tabletOptions, jsonStr);
        } catch (...) { return nullptr; }
        tabletOptions->SetIsOnline(true);
        tabletOptions->SetIsLeader(true);
        tabletOptions->SetFlushRemote(true);
        tabletOptions->SetFlushLocal(false);
        return tabletOptions;
    }

protected:
    std::unique_ptr<indexlibv2::table::TableTestHelper> _helper;
};

TEST_F(SourceUpdate2AddTest, testSimple) {
    prepareIndex();
    ASSERT_TRUE(_helper);
    auto tablet = _helper->GetTablet();
    ASSERT_TRUE(tablet);

    SourceUpdate2Add rewriter;
    ASSERT_TRUE(rewriter.init(tablet.get()));

    ASSERT_TRUE(rewriter.createSnapshot());
    {
        LogRecord insert(1);
        ASSERT_TRUE(LogRecordBuilder::makeInsert({{"pk", "f"}, {"f2", "6"}}, insert));
        ASSERT_EQ(RC_OK, rewriter.rewrite(insert));
        ASSERT_EQ(LT_ADD, insert.getType());
        ASSERT_EQ(1, insert.getLogId());
    }
    {
        LogRecord d(1);
        ASSERT_TRUE(LogRecordBuilder::makeDelete({{"pk", "f"}, {"f2", "6"}}, d));
        ASSERT_EQ(RC_OK, rewriter.rewrite(d));
        ASSERT_EQ(LT_DELETE, d.getType());
        ASSERT_EQ(1, d.getLogId());
    }
    { // pk does not exist in index

        LogRecord u1(1);
        ASSERT_TRUE(LogRecordBuilder::makeUpdate({{"pk", "f"}, {"f2", "6"}}, u1));
        ASSERT_EQ(RC_IGNORE, rewriter.rewrite(u1));
    }
    { // normal case

        LogRecord u2(2);
        ASSERT_TRUE(LogRecordBuilder::makeUpdate({{"pk", "b"}, {"f2", "6"}}, u2));
        ASSERT_EQ(RC_OK, rewriter.rewrite(u2));
        ASSERT_EQ(2, u2.getLogId());
        ASSERT_EQ(LT_ADD, u2.getType());
        autil::StringView val;
        ASSERT_TRUE(u2.getField(autil::StringView("pk"), val));

        ASSERT_EQ("b", val.to_string());
        ASSERT_TRUE(u2.getField(autil::StringView("f2"), val));
        ASSERT_EQ("2", val.to_string());
        ASSERT_TRUE(u2.getField(autil::StringView("f3"), val));
        ASSERT_EQ("2222", val.to_string());
    }

    { // normal case with ha reserved timestamp
        LogRecord u3(3);
        ASSERT_TRUE(
            LogRecordBuilder::makeUpdate({{"pk", "b"}, {"f2", "6"}, {indexlib::HA_RESERVED_TIMESTAMP, "12"}}, u3));
        ASSERT_EQ(RC_OK, rewriter.rewrite(u3));
        ASSERT_EQ(3, u3.getLogId());
        ASSERT_EQ(LT_ADD, u3.getType());
        autil::StringView val;
        ASSERT_TRUE(u3.getField(autil::StringView("pk"), val));

        ASSERT_EQ("b", val.to_string());
        ASSERT_TRUE(u3.getField(autil::StringView("f2"), val));
        ASSERT_EQ("2", val.to_string());
        ASSERT_TRUE(u3.getField(autil::StringView("f3"), val));
        ASSERT_EQ("2222", val.to_string());
        ASSERT_TRUE(u3.getField(autil::StringView(indexlib::HA_RESERVED_TIMESTAMP), val));
        ASSERT_EQ("12", val.to_string());
    }
    { // no pk

        LogRecord u4(4);
        ASSERT_TRUE(LogRecordBuilder::makeUpdate({{"f3", "b"}, {"f2", "6"}}, u4));
        ASSERT_EQ(RC_IGNORE, rewriter.rewrite(u4));
    }
    { // alter log

        LogRecord u5(5);
        ASSERT_TRUE(LogRecordBuilder::makeAlter({}, u5));
        ASSERT_EQ(RC_IGNORE, rewriter.rewrite(u5));
    }
    rewriter.releaseSnapshot();
}

} // namespace suez

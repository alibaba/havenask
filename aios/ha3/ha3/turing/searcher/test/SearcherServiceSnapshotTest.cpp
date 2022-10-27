#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/searcher/SearcherServiceSnapshot.h>
#include <ha3/common/VersionCalculator.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/turing/searcher/SearcherBiz.h>
#include <ha3/turing/searcher/DefaultAggBiz.h>
#include <ha3/turing/searcher/ParaSearchBiz.h>
#include <typeinfo>

using namespace std;
using namespace testing;
using namespace suez;
using namespace suez::turing;
using namespace isearch;

BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(common);

class SearcherServiceSnapshotTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, SearcherServiceSnapshotTest);

void SearcherServiceSnapshotTest::setUp() {
}

void SearcherServiceSnapshotTest::tearDown() {
}

TEST_F(SearcherServiceSnapshotTest, testCalcVersion) {
    SearcherServiceSnapshot snapshot;
    auto biz = snapshot.doCreateBiz("bizName");
    snapshot._workerParam.workerConfigVersion = "workerConfigVersion";
    biz->_bizInfo._versionConfig._dataVersion = "dataVersion";
    biz->_bizMeta.setRemoteConfigPath("remote path");
    snapshot._fullVersionStr = "fullVersionStr";
    EXPECT_EQ(VersionCalculator::calcVersion("workerConfigVersion",
                                             "dataVersion", "remote path",
                                             "fullVersionStr"),
              snapshot.calcVersion(biz.get()));
    biz->_bizInfo._version = 100;
    EXPECT_EQ(100, snapshot.calcVersion(biz.get()));
}

TEST_F(SearcherServiceSnapshotTest, testAddExtraBizMetas) {
    {
        SearcherServiceSnapshot snapShot;
        BizMetas input;
        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(retMetas, input);
    }
    {
        SearcherServiceSnapshot snapShot;
        BizMetas input;
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        input["default"] = testMeta;
        BizMetas expect = input;
        expect[DEFAULT_SQL_BIZ_NAME] = testMeta;
        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {
        SearcherServiceSnapshot snapShot;
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input[DEFAULT_BIZ_NAME] = testMeta;
        BizMetas expect = input;
        expect[DEFAULT_SQL_BIZ_NAME] = testMeta;

        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {
        SearcherServiceSnapshot snapShot;
        snapShot._defaultAggStr = "xxx_agg_100";
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input["other_biz"] = testMeta;
        BizMetas expect = input;

        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {
        SearcherServiceSnapshot snapShot;
        snapShot._defaultAggStr = "default_agg_100";
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input["other_biz"] = testMeta;
        BizMetas expect = input;
        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {
        SearcherServiceSnapshot snapShot;
        snapShot._defaultAggStr = "xxxdefault_agg_100";
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input[DEFAULT_BIZ_NAME] = testMeta;
        BizMetas expect = input;
        expect[HA3_DEFAULT_AGG] = testMeta;
        expect[DEFAULT_SQL_BIZ_NAME] = testMeta;
        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {
        SearcherServiceSnapshot snapShot;
        snapShot._defaultAggStr = "default_agg_100";
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input[DEFAULT_BIZ_NAME] = testMeta;
        BizMetas expect = input;
        expect[HA3_DEFAULT_AGG] = testMeta;
        expect[DEFAULT_SQL_BIZ_NAME] = testMeta;
        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {
        SearcherServiceSnapshot snapShot;
        snapShot._paraWaysStr = "2,4,8,16";
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input["other_biz"] = testMeta;
        BizMetas expect = input;
        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {
        SearcherServiceSnapshot snapShot;
        snapShot._paraWaysStr = "2,4,8,16";
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input[DEFAULT_BIZ_NAME] = testMeta;
        BizMetas expect = input;
        expect[HA3_PARA_SEARCH_PREFIX+"2"] = testMeta;
        expect[HA3_PARA_SEARCH_PREFIX+"4"] = testMeta;
        expect[HA3_PARA_SEARCH_PREFIX+"8"] = testMeta;
        expect[HA3_PARA_SEARCH_PREFIX+"16"] = testMeta;
        expect[DEFAULT_SQL_BIZ_NAME] = testMeta;
        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {
        SearcherServiceSnapshot snapShot;
        snapShot._paraWaysStr = "2,4,8,16,32";
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input[DEFAULT_BIZ_NAME] = testMeta;
        BizMetas expect = input;
        expect[DEFAULT_SQL_BIZ_NAME] = testMeta;
        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
    {//both
        SearcherServiceSnapshot snapShot;
        snapShot._defaultAggStr = "default_agg_100";
        snapShot._paraWaysStr = "2,4";
        BizMeta testMeta("localConfPath", "remoteConfPath");
        testMeta.keepCount = 100;
        BizMetas input;
        input[DEFAULT_BIZ_NAME] = testMeta;
        BizMetas expect = input;
        expect[HA3_DEFAULT_AGG] = testMeta;
        expect[HA3_PARA_SEARCH_PREFIX+"2"] = testMeta;
        expect[HA3_PARA_SEARCH_PREFIX+"4"] = testMeta;
        expect[DEFAULT_SQL_BIZ_NAME] = testMeta;

        const BizMetas &retMetas = snapShot.addExtraBizMetas(input);
        ASSERT_EQ(expect, retMetas);
    }
}

TEST_F(SearcherServiceSnapshotTest, testDoCreateBiz) {
// HA3_DEFAULT_AGG_PREFIX = "default_agg_";
// HA3_DEFAULT_AGG = "default_agg";
// HA3_PARA_SEARCH_PREFIX = "para_search_";
    SearcherServiceSnapshot snapShot;
    snapShot._basicTuringBizNames.insert("default");
    {
        BizPtr retBiz = snapShot.doCreateBiz("default");
        ASSERT_EQ(string(typeid(Biz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("user_search");
        ASSERT_EQ(string(typeid(Biz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("vn.default");
        ASSERT_EQ(string(typeid(SearcherBiz).name()), string(typeid(*retBiz).name()))
            << string(typeid(SearcherBiz).name())<< "|" << string(typeid(*retBiz).name());
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("default_agg");
        ASSERT_EQ(string(typeid(DefaultAggBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("xx_default_agg");
        ASSERT_EQ(string(typeid(DefaultAggBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("default_agg_xx");
        ASSERT_EQ(string(typeid(SearcherBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("xx_default_agg_xx");
        ASSERT_EQ(string(typeid(SearcherBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("para_search_");
        ASSERT_EQ(string(typeid(ParaSearchBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("para_search_2");
        ASSERT_EQ(string(typeid(ParaSearchBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("para_search_xxx");
        ASSERT_EQ(string(typeid(ParaSearchBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("xxx_para_search_");
        ASSERT_EQ(string(typeid(ParaSearchBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("xxx_para_xx_search_");
         ASSERT_EQ(string(typeid(SearcherBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("");
         ASSERT_EQ(string(typeid(SearcherBiz).name()), string(typeid(*retBiz).name()));
    }

    snapShot._basicTuringBizNames.erase("default");
    {
        BizPtr retBiz = snapShot.doCreateBiz("default");
        ASSERT_EQ(string(typeid(SearcherBiz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("mainse_user_search.default");
        ASSERT_EQ(string(typeid(Biz).name()), string(typeid(*retBiz).name()));
    }
    {
        BizPtr retBiz = snapShot.doCreateBiz("user_search");
        ASSERT_EQ(string(typeid(Biz).name()), string(typeid(*retBiz).name()));
    }
}

TEST_F(SearcherServiceSnapshotTest, testSetParaWays) {
    SearcherServiceSnapshot snapshot;
    ASSERT_EQ(string(""), snapshot._paraWaysStr);
    snapshot.setParaWays("xxx");
    ASSERT_EQ(string("xxx"), snapshot._paraWaysStr);
    snapshot.setParaWays("2,4");
    ASSERT_EQ(string("2,4"), snapshot._paraWaysStr);
    snapshot.setParaWays("");
    ASSERT_EQ(string(""), snapshot._paraWaysStr);
}

END_HA3_NAMESPACE(turing);

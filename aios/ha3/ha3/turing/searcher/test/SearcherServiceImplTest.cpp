#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/turing/searcher/SearcherServiceImpl.h"
#include "ha3/turing/searcher/SearcherServiceSnapshot.h"
#include <ha3/config/TypeDefine.h>

using namespace std;
using namespace testing;
using namespace suez;
using namespace suez::turing;
using namespace isearch;

BEGIN_HA3_NAMESPACE(turing);

class SearcherServiceImplTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, SearcherServiceImplTest);

void SearcherServiceImplTest::setUp() {
}

void SearcherServiceImplTest::tearDown() {
}

TEST_F(SearcherServiceImplTest, testDoInit) {
    {
        const string &oldTbn = sap::EnvironUtil::getEnv("basicTuringBizNames", "");
        sap::EnvironUtil::setEnv("basicTuringBizNames", "");
        SearcherServiceImpl ssi;
        ASSERT_TRUE(ssi.doInit());
        ASSERT_TRUE(ssi._basicTuringBizNames.empty());

        sap::EnvironUtil::setEnv("basicTuringBizNames", "default,test");
        ASSERT_TRUE(ssi.doInit());
        ASSERT_EQ(2u, ssi._basicTuringBizNames.size());
        ASSERT_EQ(1u, ssi._basicTuringBizNames.count("default"));
        ASSERT_EQ(1u, ssi._basicTuringBizNames.count("test"));
        sap::EnvironUtil::setEnv("basicTuringBizNames", oldTbn);
    }
    {
        const string &oldDa = sap::EnvironUtil::getEnv("defaultAgg", "");
        sap::EnvironUtil::setEnv("defaultAgg", "");
        SearcherServiceImpl ssi;
        ASSERT_TRUE(ssi.doInit());
        ASSERT_EQ(string("default_agg_4"), ssi._defaultAggStr);

        sap::EnvironUtil::setEnv("defaultAgg", "test");
        ASSERT_TRUE(ssi.doInit());
        ASSERT_EQ(string(""), ssi._defaultAggStr);

        sap::EnvironUtil::setEnv("defaultAgg", "default_agg_40");
        ASSERT_TRUE(ssi.doInit());
        ASSERT_EQ(string("default_agg_40"), ssi._defaultAggStr);
        sap::EnvironUtil::setEnv("defaultAgg", oldDa);
    }
    {
        const string &oldPsw = sap::EnvironUtil::getEnv("paraSearchWays", "");
        sap::EnvironUtil::setEnv("paraSearchWays", "");
        SearcherServiceImpl ssi;
        ASSERT_TRUE(ssi.doInit());
        ASSERT_EQ(string("2,4"), ssi._paraWaysStr);

        sap::EnvironUtil::setEnv("paraSearchWays", "8");
        ASSERT_TRUE(ssi.doInit());
        ASSERT_EQ(string("8"), ssi._paraWaysStr);

        sap::EnvironUtil::setEnv("paraSearchWays", "xxx");
        ASSERT_TRUE(ssi.doInit());
        ASSERT_EQ(string(""), ssi._paraWaysStr);

        sap::EnvironUtil::setEnv("paraSearchWays", "8,32");
        ASSERT_TRUE(ssi.doInit());
        ASSERT_EQ(string(""), ssi._paraWaysStr);
        sap::EnvironUtil::setEnv("paraSearchWays", oldPsw);
    }
}

TEST_F(SearcherServiceImplTest, testDoCreateServiceSnapshot) {
    SearcherServiceImpl ssi;
    ssi._basicTuringBizNames.insert("default");
    ssi._defaultAggStr = "defalut_agg_x";
    ssi._paraWaysStr = "8,16";
    auto snapshotPtr = ssi.doCreateServiceSnapshot();
    auto searcherSnapshot = (SearcherServiceSnapshot *)snapshotPtr.get();
    ASSERT_EQ(1u, searcherSnapshot->_basicTuringBizNames.size());
    ASSERT_EQ(1u, searcherSnapshot->_basicTuringBizNames.count("default"));
    ASSERT_EQ(string("defalut_agg_x"), searcherSnapshot->_defaultAggStr);
    ASSERT_EQ(string("8,16"), searcherSnapshot->_paraWaysStr);
}

END_HA3_NAMESPACE(turing);

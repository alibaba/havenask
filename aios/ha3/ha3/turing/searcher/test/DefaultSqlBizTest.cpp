#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/searcher/DefaultSqlBiz.h>

using namespace std;
using namespace testing;
using namespace autil::legacy;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(turing);

class DefaultSqlBizTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(searcher, DefaultSqlBizTest);

void DefaultSqlBizTest::setUp() {
}

void DefaultSqlBizTest::tearDown() {
}

TEST_F(DefaultSqlBizTest, testGetRange) {
    proto::Range range;
    {
        ASSERT_TRUE(DefaultSqlBiz::getRange(2, 0, range));
        ASSERT_EQ(0, range.from());
        ASSERT_EQ(32767, range.to());
    }
    {
        ASSERT_TRUE(DefaultSqlBiz::getRange(2, 1, range));
        ASSERT_EQ(32768, range.from());
        ASSERT_EQ(65535, range.to());
    }
    {
        ASSERT_FALSE(DefaultSqlBiz::getRange(2, 2, range));
    }
}

TEST_F(DefaultSqlBizTest, testNeedLogQueryDefault) {
    DefaultSqlBiz biz;
    resource_reader::ResourceReaderPtr resource(
            new resource_reader::ResourceReader(
                    GET_TEST_DATA_PATH() + "/non_exists/"));
    biz._resourceReader = resource;
    ASSERT_TRUE(biz.preloadBiz().ok());
    ASSERT_TRUE(biz.loadBizInfo().ok());
    ASSERT_FALSE(biz.needLogQuery());
}

TEST_F(DefaultSqlBizTest, testInitSqlAggFuncManager) {
    DefaultSqlBiz biz;
    resource_reader::ResourceReaderPtr resource(
            new resource_reader::ResourceReader(
                    GET_TEST_DATA_PATH() + "/sql_agg_func2/"));
    SqlAggPluginConfig config;
    std::string content;
    resource->getFileContent("sql.json", content);
    FromJsonString(config, content);
    biz._resourceReader = resource;
    ASSERT_TRUE(biz.initSqlAggFuncManager(config));
}

TEST_F(DefaultSqlBizTest, testLoadUserPlugins) {
    DefaultSqlBiz biz;
    resource_reader::ResourceReaderPtr resource(
            new resource_reader::ResourceReader(
                    GET_TEST_DATA_PATH() + "/sql_agg_func2/"));
    biz._resourceReader = resource;
    biz._sqlConfigPtr.reset(new SqlConfig());
    ASSERT_EQ(tensorflow::Status::OK(), biz.loadUserPlugins());
}

TEST_F(DefaultSqlBizTest, testInitSqlTvfFuncManager) {
    DefaultSqlBiz biz;
    resource_reader::ResourceReaderPtr resource(
            new resource_reader::ResourceReader(
                    GET_TEST_DATA_PATH() + "/sql_agg_func2/"));
    SqlTvfPluginConfig config;
    std::string content;
    resource->getFileContent("sql.json", content);
    FromJsonString(config, content);
    biz._resourceReader = resource;
    ASSERT_TRUE(biz.initSqlTvfFuncManager(config));
}

END_HA3_NAMESPACE(turing);

#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/ServiceDegrade.h>
#include <ha3/service/test/Ha3GigQueryInfoMock.h>

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(service);

class ServiceDegradeTest : public TESTBASE {
public:
    ServiceDegradeTest();
    ~ServiceDegradeTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, ServiceDegradeTest);


ServiceDegradeTest::ServiceDegradeTest() {
}

ServiceDegradeTest::~ServiceDegradeTest() {
}

void ServiceDegradeTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void ServiceDegradeTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ServiceDegradeTest, testUpdateRequest) {
    {
        // session is null
        config::ServiceDegradationConfig config;
        config.request.rankSize = 30;
        ServiceDegrade serviceDegrade(config);
        autil::mem_pool::Pool pool;
        common::Request request(&pool);
        EXPECT_EQ(30, serviceDegrade._config.request.rankSize);
        EXPECT_FALSE(serviceDegrade.updateRequest(&request, multi_call::QueryInfoPtr()));
    }
    {
        // no degrade
        Ha3GigQueryInfoMock *mockSession = new Ha3GigQueryInfoMock();
        multi_call::QueryInfoPtr gigSession(mockSession);
        EXPECT_CALL(*mockSession, degradeLevel(1.0f))
            .WillOnce(Return(0.0f));
        autil::mem_pool::Pool pool;
        common::Request request(&pool);
        request.setConfigClause(new common::ConfigClause());
        config::ServiceDegradationConfig config;
        config.request.rankSize = 300;
        config.request.rerankSize = 300;
        request.getConfigClause()->setTotalRankSize(3000);
        request.getConfigClause()->setTotalRerankSize(4000);
        ServiceDegrade serviceDegrade(config);
        EXPECT_FALSE(serviceDegrade.updateRequest(&request, gigSession));
        EXPECT_EQ(3000, request.getConfigClause()->getTotalRankSize());
        EXPECT_EQ(4000, request.getConfigClause()->getTotalRerankSize());
        float level;
        uint32_t rankSize;
        uint32_t rerankSize;
        request.getDegradeLevel(level, rankSize, rerankSize);
        EXPECT_EQ(0.0f, level);
        EXPECT_EQ(300, rankSize);
        EXPECT_EQ(300, rerankSize);
    }
    {
        // degrade and rankSize/rerankSize == 0
        Ha3GigQueryInfoMock *mockSession = new Ha3GigQueryInfoMock();
        multi_call::QueryInfoPtr gigSession(mockSession);
        EXPECT_CALL(*mockSession, degradeLevel(1.0f))
            .WillOnce(Return(0.5f));
        autil::mem_pool::Pool pool;
        common::Request request(&pool);
        request.setConfigClause(new common::ConfigClause());
        config::ServiceDegradationConfig config;
        config.request.rankSize = 0;
        config.request.rerankSize = 0;
        request.getConfigClause()->setTotalRankSize(3000);
        request.getConfigClause()->setTotalRerankSize(4000);
        ServiceDegrade serviceDegrade(config);
        EXPECT_TRUE(serviceDegrade.updateRequest(&request, gigSession));
        EXPECT_EQ(3000, request.getConfigClause()->getTotalRankSize());
        EXPECT_EQ(4000, request.getConfigClause()->getTotalRerankSize());
        float level;
        uint32_t rankSize;
        uint32_t rerankSize;
        request.getDegradeLevel(level, rankSize, rerankSize);
        EXPECT_EQ(0.5f, level);
        EXPECT_EQ(0, rankSize);
        EXPECT_EQ(0, rerankSize);
    }
    {
        // degrade and rankSize/rerankSize > 0
        Ha3GigQueryInfoMock *mockSession = new Ha3GigQueryInfoMock();
        multi_call::QueryInfoPtr gigSession(mockSession);
        EXPECT_CALL(*mockSession, degradeLevel(1.0f))
            .WillOnce(Return(0.5f));
        autil::mem_pool::Pool pool;
        common::Request request(&pool);
        request.setConfigClause(new common::ConfigClause());
        config::ServiceDegradationConfig config;
        config.request.rankSize = 10;
        config.request.rerankSize = 20;
        request.getConfigClause()->setTotalRankSize(3000);
        request.getConfigClause()->setTotalRerankSize(4000);
        ServiceDegrade serviceDegrade(config);
        EXPECT_TRUE(serviceDegrade.updateRequest(&request, gigSession));
        EXPECT_EQ(0, request.getConfigClause()->getTotalRankSize());
        EXPECT_EQ(0, request.getConfigClause()->getTotalRerankSize());
        float level;
        uint32_t rankSize;
        uint32_t rerankSize;
        request.getDegradeLevel(level, rankSize, rerankSize);
        EXPECT_EQ(0.5f, level);
        EXPECT_EQ(10, rankSize);
        EXPECT_EQ(20, rerankSize);
    }
}

END_HA3_NAMESPACE(service);

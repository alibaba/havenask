#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/qrs/QrsBiz.h>

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(turing);

class QrsBizTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsBizTest);

void QrsBizTest::setUp() {
}

void QrsBizTest::tearDown() {
}

TEST_F(QrsBizTest, testGetBenchmarkBizName) {
    EXPECT_EQ("bizName:t", QrsBiz::getBenchmarkBizName("bizName"));
}

TEST_F(QrsBizTest, testUpdateFlowControl) {
    QrsBiz biz;
    biz._configAdapter.reset(new config::ConfigAdapter(TEST_DATA_CONF_PATH_VERSION(3)));
    biz._searchService.reset(new multi_call::SearchService());
    biz.updateFlowConfig("");
    auto searchService = biz._searchService;
    // config both
    {
        auto companyConfig = searchService->getFlowConfig("company.default");
        EXPECT_TRUE(companyConfig);
        EXPECT_EQ(10, companyConfig->beginDegradeLatency);
        EXPECT_TRUE(5 != companyConfig->fullDegradeLatency);
        const auto &compatibleFieldInfo = companyConfig->compatibleFieldInfo;
        EXPECT_EQ("gigRequestInfo", compatibleFieldInfo.requestInfoField);
        EXPECT_EQ("multicall_ec", compatibleFieldInfo.ecField);
        EXPECT_EQ("gigResponseInfo", compatibleFieldInfo.responseInfoField);
    }
    {
        auto companyConfigT = searchService->getFlowConfig(
                QrsBiz::getBenchmarkBizName("company.default"));
        EXPECT_TRUE(companyConfigT);
        EXPECT_TRUE(10 != companyConfigT->beginDegradeLatency);
        EXPECT_EQ(5, companyConfigT->fullDegradeLatency);
        const auto &compatibleFieldInfo = companyConfigT->compatibleFieldInfo;
        EXPECT_EQ("gigRequestInfo", compatibleFieldInfo.requestInfoField);
        EXPECT_EQ("multicall_ec", compatibleFieldInfo.ecField);
        EXPECT_EQ("gigResponseInfo", compatibleFieldInfo.responseInfoField);
    }
    // config none
    {
        auto companyConfig = searchService->getFlowConfig("auction.default");
        EXPECT_TRUE(companyConfig);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfig->beginDegradeLatency);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfig->fullDegradeLatency);
    }
    {
        auto companyConfigT = searchService->getFlowConfig(
                QrsBiz::getBenchmarkBizName("auction.default"));
        EXPECT_TRUE(companyConfigT);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfigT->beginDegradeLatency);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfigT->fullDegradeLatency);
    }
    // config normal one
    {
        auto companyConfig = searchService->getFlowConfig("human.default");
        EXPECT_TRUE(companyConfig);
        EXPECT_EQ(10, companyConfig->beginDegradeLatency);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfig->fullDegradeLatency);
    }
    {
        auto companyConfigT = searchService->getFlowConfig(
                QrsBiz::getBenchmarkBizName("human.default"));
        EXPECT_TRUE(companyConfigT);
        EXPECT_EQ(10, companyConfigT->beginDegradeLatency);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfigT->fullDegradeLatency);
    }
    // config T(abnormal) one
    {
        auto companyConfig = searchService->getFlowConfig("dog.default");
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfig->beginDegradeLatency);
        EXPECT_TRUE(companyConfig);
    }
    {
        auto companyConfigT = searchService->getFlowConfig(
                QrsBiz::getBenchmarkBizName("dog.default"));
        EXPECT_TRUE(companyConfigT);
        EXPECT_EQ(5, companyConfigT->beginDegradeLatency);
    }
}

TEST_F(QrsBizTest, testUpdateFlowControlWithTuring) {
    QrsBiz biz;
    biz._configAdapter.reset(new config::ConfigAdapter(TEST_DATA_CONF_PATH_VERSION(turing)));
    biz._searchService.reset(new multi_call::SearchService());
    biz.updateFlowConfig("");
    auto searchService = biz._searchService;
    // config both
    {
        auto config = searchService->getFlowConfig("company.default");
        EXPECT_TRUE(config);
        EXPECT_EQ(10, config->beginDegradeLatency);
        EXPECT_TRUE(5 != config->fullDegradeLatency);
        const auto &compatibleFieldInfo = config->compatibleFieldInfo;
        EXPECT_EQ("gigRequestInfo", compatibleFieldInfo.requestInfoField);
        EXPECT_EQ("multicall_ec", compatibleFieldInfo.ecField);
        EXPECT_EQ("gigResponseInfo", compatibleFieldInfo.responseInfoField);
    }
    {
        auto config = searchService->getFlowConfig("graph_search.user_graph_search");
        EXPECT_TRUE(config);
        EXPECT_EQ(11, config->beginDegradeLatency);
        EXPECT_TRUE(5 != config->fullDegradeLatency);
    }
    {
        auto config = searchService->getFlowConfig("turing_1.user_graph_search");
        EXPECT_TRUE(config);
        EXPECT_EQ(10, config->beginDegradeLatency);
        EXPECT_TRUE(5 != config->fullDegradeLatency);
    }
    {
        auto config = searchService->getFlowConfig("offer_gb.user_graph_search");
        EXPECT_TRUE(config);
        EXPECT_EQ(20, config->beginDegradeLatency);
        EXPECT_TRUE(5 != config->fullDegradeLatency);
    }

}

TEST_F(QrsBizTest, testDependService) {
    QrsBiz biz;
    biz._configAdapter.reset(
        new config::ConfigAdapter(TEST_DATA_CONF_PATH_VERSION(3)));
    biz._resourceReader.reset(new resource_reader::ResourceReader(""));
    EXPECT_EQ(tensorflow::Status::OK(), biz.loadBizInfo());
    EXPECT_EQ(std::vector<std::string>({ "biz1", "biz2" }),
              biz._bizInfo._dependServices._importantServices);
    EXPECT_EQ(std::vector<std::string>({ "biz3" }),
              biz._bizInfo._dependServices._trivialServices);
}

END_HA3_NAMESPACE();

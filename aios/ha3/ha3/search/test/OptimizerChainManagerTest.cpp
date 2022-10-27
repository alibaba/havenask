#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/util/Log.h>
#include <ha3/search/OptimizerChainManager.h>
#include <ha3/search/test/OptimizerChainManagerCreatorTest.h>
#include <ha3/common/OptimizerClause.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/test/test.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class OptimizerChainManagerTest : public TESTBASE {
public:
    OptimizerChainManagerTest();
    ~OptimizerChainManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, OptimizerChainManagerTest);


OptimizerChainManagerTest::OptimizerChainManagerTest() { 
}

OptimizerChainManagerTest::~OptimizerChainManagerTest() { 
}

void OptimizerChainManagerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void OptimizerChainManagerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(OptimizerChainManagerTest, testCreateOptimizerChain) { 
    string configFile = TEST_DATA_PATH"/optimizer_config_test/"
                        + CLUSTER_CONFIG_DIR_NAME + "/two_module/"
                        + DEFAULT_BIZ_NAME + CLUSTER_JSON_FILE_SUFFIX;
    OptimizerChainManagerPtr optimizerChainManager = 
        OptimizerChainManagerCreatorTest::createOptimizerChainManager(
                FileUtil::readFile(configFile));
    ASSERT_TRUE(optimizerChainManager != NULL);
    map<std::string, OptimizerPtr> &optimizers = optimizerChainManager->_optimizers;
    ASSERT_EQ(size_t(3), optimizers.size());
    ASSERT_TRUE(optimizers.find("AuxChain") != optimizers.end());
    ASSERT_TRUE(optimizers.find("test_optimizer1") != optimizers.end());
    ASSERT_TRUE(optimizers.find("test_optimizer2") != optimizers.end());
    {
        //invalid optimizer name
        OptimizerClause *optimizerClause = new OptimizerClause;
        optimizerClause->addOptimizerName("not_exixt");
        optimizerClause->addOptimizerOption("test");
        Request request;
        request.setOptimizerClause(optimizerClause);
        OptimizerChainPtr optimizerChain = optimizerChainManager->createOptimizerChain(&request);
        ASSERT_TRUE(optimizerChain == NULL);
    }
    {
        //invalid init option
        OptimizerClause *optimizerClause = new OptimizerClause;
        optimizerClause->addOptimizerName("AuxChain");
        optimizerClause->addOptimizerOption("invalid_init_option");
        Request request;
        request.setOptimizerClause(optimizerClause);
        OptimizerChainPtr optimizerChain = optimizerChainManager->createOptimizerChain(&request);
        ASSERT_TRUE(optimizerChain == NULL);
    }
    {
        OptimizerClause *optimizerClause = new OptimizerClause;
        optimizerClause->addOptimizerName("test_optimizer1");
        optimizerClause->addOptimizerOption("test");
        optimizerClause->addOptimizerName("test_optimizer2");
        optimizerClause->addOptimizerOption("test");
        Request request;
        request.setOptimizerClause(optimizerClause);
        OptimizerChainPtr optimizerChain = optimizerChainManager->createOptimizerChain(&request);
        ASSERT_TRUE(optimizerChain != NULL);
        ASSERT_EQ(size_t(2), optimizerChain->_optimizers.size());
        ASSERT_EQ(string("test_optimizer1"), optimizerChain->_optimizers[0]->getName());
        ASSERT_EQ(string("test_optimizer2"), optimizerChain->_optimizers[1]->getName());
    }

}

END_HA3_NAMESPACE(search);


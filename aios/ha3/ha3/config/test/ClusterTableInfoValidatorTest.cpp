#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <ha3/config/ClusterTableInfoValidator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/config/ConfigAdapter.h>

using namespace suez::turing;
using namespace std;
BEGIN_HA3_NAMESPACE(config);

class ClusterTableInfoValidatorTest : public TESTBASE {
public:
    ClusterTableInfoValidatorTest();
    ~ClusterTableInfoValidatorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, ClusterTableInfoValidatorTest);


ClusterTableInfoValidatorTest::ClusterTableInfoValidatorTest() { 
}

ClusterTableInfoValidatorTest::~ClusterTableInfoValidatorTest() { 
}

void ClusterTableInfoValidatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ClusterTableInfoValidatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ClusterTableInfoValidatorTest, testValidate) {
    HA3_LOG(DEBUG, "Begin Test!");

    string configRoot = TEST_DATA_CONF_PATH_HA3;
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    
    ClusterTableInfoManagerMapPtr clusterTableInfoManagerMapPtr 
        = configAdapterPtr->getClusterTableInfoManagerMap();
    ASSERT_TRUE(clusterTableInfoManagerMapPtr);
    ASSERT_TRUE(ClusterTableInfoValidator::validate(clusterTableInfoManagerMapPtr));
}

TEST_F(ClusterTableInfoValidatorTest, testValidateMainTableAgainstJoinTable) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr,
                        "id", true));
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "certification_level", true));
        ASSERT_TRUE(ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "company_id", true));
    }
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction1_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company1_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "company_id", true));
    }
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction2_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company2_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "company_id", true));
    }
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction3_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company3_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "company_id", true));
    }
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction4_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company4_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "company_id", true));
    }
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction5_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company5_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "company_id", true));
    }
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction6_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company6_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "company_id", true));
    }
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction8_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company8_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr, 
                        "company_id", true));
    }    
}

TEST_F(ClusterTableInfoValidatorTest, testValidateFieldInfosBetweenJoinTables) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company_schema.json");
        ASSERT_TRUE(!ClusterTableInfoValidator::validateFieldInfosBetweenJoinTables(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr));
    }
    {
        TableInfoPtr auctionTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/auction7_schema.json");
        TableInfoPtr companyTableInfoPtr = 
            TableInfoConfigurator::createFromFile(
                    TEST_DATA_PATH"/config_test/join_table_config/company7_schema.json");
        ASSERT_TRUE(ClusterTableInfoValidator::validateFieldInfosBetweenJoinTables(
                        auctionTableInfoPtr, 
                        companyTableInfoPtr));
    }
}

TEST_F(ClusterTableInfoValidatorTest, testValidateSingleClusterTableInfoManager) {
    {
        string configRoot = TEST_DATA_CONF_PATH_VERSION(3);
        ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
        ClusterTableInfoManagerPtr clusterTableInfoManagerPtr 
            = configAdapterPtr->getClusterTableInfoManager("auction");
        ASSERT_TRUE(clusterTableInfoManagerPtr);
        ASSERT_TRUE(!ClusterTableInfoValidator::validateSingleClusterTableInfoManager(
                        clusterTableInfoManagerPtr));
    }
    {
        string configRoot = TEST_DATA_CONF_PATH_VERSION(4);
        ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));

        ClusterTableInfoManagerPtr clusterTableInfoManagerPtr 
            = configAdapterPtr->getClusterTableInfoManager("auction");
        ASSERT_TRUE(clusterTableInfoManagerPtr);
        ASSERT_TRUE(!ClusterTableInfoValidator::validateSingleClusterTableInfoManager(
                        clusterTableInfoManagerPtr));   
    }
}

END_HA3_NAMESPACE(config);


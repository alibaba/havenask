#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <ha3/config/ClusterTableInfoManager.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>

using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class ClusterTableInfoManagerTest : public TESTBASE {
public:
    ClusterTableInfoManagerTest();
    ~ClusterTableInfoManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkAttributeInfo(const AttributeInfo *attributeInfo, 
                            const std::string &attributeName,
                            FieldType fieldType, bool isMulti) const;
protected:
    ClusterTableInfoManagerPtr _clusterTableInfoManagerPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, ClusterTableInfoManagerTest);


ClusterTableInfoManagerTest::ClusterTableInfoManagerTest() { 
}

ClusterTableInfoManagerTest::~ClusterTableInfoManagerTest() { 
}

void ClusterTableInfoManagerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    TableInfoPtr mainTableInfoPtr = 
        TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/main_schema.json");
    TableInfoPtr companyTableInfoPtr = 
        TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/company_schema.json");
    ASSERT_TRUE(mainTableInfoPtr);
    ASSERT_TRUE(companyTableInfoPtr);

    ClusterTableInfoManagerPtr clusterTableInfoManagerPtr(new ClusterTableInfoManager());
    clusterTableInfoManagerPtr->setMainTableInfo(mainTableInfoPtr);
    JoinConfigInfo joinConfigInfo;
    joinConfigInfo.joinField = "company_id";
    clusterTableInfoManagerPtr->addJoinTableInfo(companyTableInfoPtr, joinConfigInfo);
    _clusterTableInfoManagerPtr = clusterTableInfoManagerPtr;
}

void ClusterTableInfoManagerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ClusterTableInfoManagerTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    TableInfoPtr actualMainTableInfoPtr = _clusterTableInfoManagerPtr->getMainTableInfo();
    ASSERT_TRUE(actualMainTableInfoPtr);
    ASSERT_EQ(string("main"), actualMainTableInfoPtr->getTableName());
    TableInfoPtr actualCompanyTableInfoPtr = 
        _clusterTableInfoManagerPtr->getJoinTableInfoByTableName("company");
    string joinFieldName = _clusterTableInfoManagerPtr->getJoinFieldNameByTableName("company");
    ASSERT_TRUE(actualCompanyTableInfoPtr);
    ASSERT_EQ(string("company"), actualCompanyTableInfoPtr->getTableName());
    ASSERT_EQ(string("company_id"), joinFieldName);
    TableInfoPtr nonExistTableInfoPtr = 
        _clusterTableInfoManagerPtr->getJoinTableInfoByTableName("noexist");
    ASSERT_TRUE(!nonExistTableInfoPtr);
    ASSERT_EQ(string(""), 
                         _clusterTableInfoManagerPtr->getJoinFieldNameByTableName("nonexist_table"));

}

TEST_F(ClusterTableInfoManagerTest, testGetAttributeInfo) {
    {
        const AttributeInfo *attributeInfo =   
            _clusterTableInfoManagerPtr->getAttributeInfo("company_id");
        checkAttributeInfo(attributeInfo, "company_id", ft_integer, false);
    }
    {
        const AttributeInfo *attributeInfo =   
            _clusterTableInfoManagerPtr->getAttributeInfo("user_id");   
        checkAttributeInfo(attributeInfo, "user_id", ft_integer, false);
    }
    {
        const AttributeInfo *attributeInfo =   
            _clusterTableInfoManagerPtr->getAttributeInfo("account_balance");
        checkAttributeInfo(attributeInfo, "account_balance", ft_float, false);
    }
    {
        const AttributeInfo *attributeInfo =   
            _clusterTableInfoManagerPtr->getAttributeInfo("no_exist_attribute");
        ASSERT_TRUE(!attributeInfo);
    }
}

TEST_F(ClusterTableInfoManagerTest, testGetClusterTableInfo) {
    TableInfoPtr mergedTableInfoPtr = _clusterTableInfoManagerPtr->getClusterTableInfo();
    ASSERT_TRUE(mergedTableInfoPtr);
    FieldInfos *fieldInfos = mergedTableInfoPtr->getFieldInfos();
    AttributeInfos *attributeInfos = mergedTableInfoPtr->getAttributeInfos();
    SummaryInfo *summaryInfo = mergedTableInfoPtr->getSummaryInfo();
    ASSERT_TRUE(fieldInfos);
    ASSERT_TRUE(attributeInfos);
    ASSERT_TRUE(summaryInfo);
    ASSERT_EQ(uint32_t(14), fieldInfos->getFieldCount());
    ASSERT_TRUE(fieldInfos->getFieldInfo("id"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("company_id"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("user_id"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("subject"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("keywords"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("cat_id"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("multi_int32_value"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("company_name"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("city"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("address"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("postcode"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("telephone"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("account_balance"));
    ASSERT_TRUE(fieldInfos->getFieldInfo("certification_level"));

    const AttributeInfos::AttrVector &attrVect = attributeInfos->getAttrbuteInfoVector();
    ASSERT_EQ(size_t(11), attrVect.size());
    ASSERT_TRUE(attributeInfos->getAttributeInfo("id"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("company_id"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("user_id"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("cat_id"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("company_name"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("city"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("address"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("postcode"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("telephone"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("account_balance"));
    ASSERT_TRUE(attributeInfos->getAttributeInfo("certification_level"));
    ASSERT_TRUE(!attributeInfos->getAttributeInfo("subject"));
    
    ASSERT_EQ(size_t(7), summaryInfo->getFieldCount());
    ASSERT_EQ(string("multi_int32_value"), summaryInfo->getFieldName(0));
    ASSERT_EQ(string("id"), summaryInfo->getFieldName(1));
    ASSERT_EQ(string("company_id"), summaryInfo->getFieldName(2));
    ASSERT_EQ(string("user_id"), summaryInfo->getFieldName(3));
    ASSERT_EQ(string("subject"), summaryInfo->getFieldName(4));
    ASSERT_EQ(string("cat_id"), summaryInfo->getFieldName(5));
    ASSERT_EQ(string("keywords"), summaryInfo->getFieldName(6));
}

void ClusterTableInfoManagerTest::checkAttributeInfo(const AttributeInfo *attributeInfo, 
        const std::string &attributeName, FieldType fieldType, bool isMulti) const 
{
    ASSERT_TRUE(attributeInfo);
    ASSERT_EQ(attributeName, attributeInfo->getAttrName());
    ASSERT_EQ(fieldType, attributeInfo->getFieldType());
    ASSERT_EQ(isMulti, attributeInfo->getMultiValueFlag());
}


END_HA3_NAMESPACE(config);


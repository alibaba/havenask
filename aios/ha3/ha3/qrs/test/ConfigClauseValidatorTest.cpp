#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/QrsConfig.h>
#include <ha3/qrs/ConfigClauseValidator.h>
#include <ha3/qrs/ConfigClauseValidator.h>

using namespace std;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);

class ConfigClauseValidatorTest : public TESTBASE {
public:
    ConfigClauseValidatorTest();
    ~ConfigClauseValidatorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
    config::QRSRule *_pQRSRule;
    ConfigClauseValidator *_pValidator;
};

HA3_LOG_SETUP(qrs, ConfigClauseValidatorTest);


ConfigClauseValidatorTest::ConfigClauseValidatorTest() { 
}

ConfigClauseValidatorTest::~ConfigClauseValidatorTest() { 
}

void ConfigClauseValidatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _pValidator = new ConfigClauseValidator(QRS_RETURN_HITS_LIMIT);
}

void ConfigClauseValidatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _pValidator;
}

TEST_F(ConfigClauseValidatorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    common::ConfigClause *pConfigClause = NULL;
    _pValidator->validate(pConfigClause);
    ErrorCode errorCode = _pValidator->getErrorCode();
    ASSERT_TRUE(errorCode == ERROR_NO_CONFIG_CLAUSE);
}

TEST_F(ConfigClauseValidatorTest, testValidateRetHitsLimitOverflow)
{
    HA3_LOG(DEBUG, "Begin Test!");
    common::ConfigClause *pConfigClause = new common::ConfigClause();
    unique_ptr<ConfigClause> configClause(pConfigClause);
    pConfigClause->setStartOffset(-1);
    pConfigClause->setHitCount(2);
    bool ret = _pValidator->validateRetHitsLimit(pConfigClause);
    ASSERT_TRUE(false == ret);
    ASSERT_TRUE( ERROR_OVER_RETURN_HITS_LIMIT == _pValidator->getErrorCode());
}

TEST_F(ConfigClauseValidatorTest, testValidateRetHitsLimitOverLimit)
{
    HA3_LOG(DEBUG, "Begin Test!");
    common::ConfigClause *pConfigClause = new common::ConfigClause();
    unique_ptr<ConfigClause> configClause(pConfigClause);
    pConfigClause->setStartOffset(3000);
    pConfigClause->setHitCount(2001);
    bool ret = _pValidator->validateRetHitsLimit(pConfigClause);
    ASSERT_TRUE(false == ret);
    ASSERT_TRUE( ERROR_OVER_RETURN_HITS_LIMIT == _pValidator->getErrorCode());
}

TEST_F(ConfigClauseValidatorTest, testValidateSearcherReturnHitsNotOverLimit)
{
    HA3_LOG(DEBUG, "Begin Test!");
    ConfigClause pConfigClause;
    pConfigClause.setSearcherReturnHits(6000000);
    bool ret = _pValidator->validateRetHitsLimit(&pConfigClause);
    ASSERT_TRUE(ret);
}

TEST_F(ConfigClauseValidatorTest, testValidateRetHitsLimitNormal)
{
    HA3_LOG(DEBUG, "Begin Test!");
    common::ConfigClause *pConfigClause = new common::ConfigClause();
    unique_ptr<ConfigClause> configClause(pConfigClause);
    pConfigClause->setStartOffset(3000);
    pConfigClause->setHitCount(2000);
    bool ret = _pValidator->validateRetHitsLimit(pConfigClause);
    ASSERT_TRUE(true == ret);
    ASSERT_TRUE( ERROR_NONE == _pValidator->getErrorCode());
}

TEST_F(ConfigClauseValidatorTest, testValidateResultFormat)
{
    HA3_LOG(DEBUG, "Begin Test!");

    ConfigClause *pConfigClause = new ConfigClause();
    unique_ptr<ConfigClause> configClause(pConfigClause);
    bool ret = _pValidator->validateResultFormat(pConfigClause);
    ASSERT_TRUE(ret);
    
    pConfigClause->setResultFormatSetting(RESULT_FORMAT_PROTOBUF);
    ret = _pValidator->validateResultFormat(pConfigClause);
    ASSERT_TRUE(ret);

    pConfigClause->setResultFormatSetting(RESULT_FORMAT_FB_SUMMARY);
    ret = _pValidator->validateResultFormat(pConfigClause);
    ASSERT_TRUE(ret);
    
    pConfigClause->setResultFormatSetting("no_such_format");
    ret = _pValidator->validateResultFormat(pConfigClause);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_UNSUPPORT_RESULT_FORMAT, 
                         _pValidator->getErrorCode());
}

END_HA3_NAMESPACE(qrs);


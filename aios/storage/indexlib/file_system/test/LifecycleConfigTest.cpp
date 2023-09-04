#include "indexlib/file_system/LifecycleConfig.h"

#include "gtest/gtest.h"

#include "indexlib/util/Exception.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class LifecycleConfigTest : public INDEXLIB_TESTBASE
{
public:
    LifecycleConfigTest();
    ~LifecycleConfigTest();

    DECLARE_CLASS_NAME(LifecycleConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestEnableLocalDeployManifest();
    void TestJsonize();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, LifecycleConfigTest);

INDEXLIB_UNIT_TEST_CASE(LifecycleConfigTest, TestEnableLocalDeployManifest);

LifecycleConfigTest::LifecycleConfigTest() {}

LifecycleConfigTest::~LifecycleConfigTest() {}

void LifecycleConfigTest::CaseSetUp() {}

void LifecycleConfigTest::CaseTearDown() {}

void LifecycleConfigTest::TestEnableLocalDeployManifest()
{
    auto checkEnableLocalDeployMainifest = [](std::string jsonStr, bool expect) {
        LifecycleConfig lifecycleConfig;
        autil::legacy::FromJsonString(lifecycleConfig, jsonStr);
        EXPECT_EQ(expect, lifecycleConfig.EnableLocalDeployManifestChecking());
    };

    LifecycleConfig lifecycleConfig;
    EXPECT_FALSE(lifecycleConfig.EnableLocalDeployManifestChecking());

    checkEnableLocalDeployMainifest("{}", false);

    std::string jsonStr1 = R"({
            "strategy": "dynamic"
    })";
    checkEnableLocalDeployMainifest(jsonStr1, true);

    std::string jsonStr2 = R"({
            "strategy": "dynamic",
            "enable_local_deploy_manifest": true
    })";
    checkEnableLocalDeployMainifest(jsonStr2, true);

    std::string jsonStr3 = R"({
            "strategy": "static",
            "enable_local_deploy_manifest": false
    })";
    checkEnableLocalDeployMainifest(jsonStr3, false);

    std::string jsonStr4 = R"({
            "strategy": "static"
    })";
    checkEnableLocalDeployMainifest(jsonStr4, false);

    std::string jsonStr5 = R"({
        "patterns": [{"statistic_field" : "segment_group", "statistic_type" : "string", "lifecycle": "hot", "range" :
        ["hot_order"]}]
    })";
    checkEnableLocalDeployMainifest(jsonStr5, false);

    std::string jsonStr6 = R"({
            "strategy": "static",
            "enable_local_deploy_manifest": true
    })";
    checkEnableLocalDeployMainifest(jsonStr6, true);

    std::string jsonStr7 = R"({
            "strategy": "dynamic",
            "enable_local_deploy_manifest": false
    })";
    checkEnableLocalDeployMainifest(jsonStr7, false);
}

void LifecycleConfigTest::TestJsonize()
{
    {
        std::string jsonStr = R"({
            "strategy": "dynamic"
            })";
        LifecycleConfig lifecycleConfig;
        ASSERT_NO_THROW(autil::legacy::FromJsonString(lifecycleConfig, jsonStr));
    }
    {
        std::string jsonStr = R"({
            "strategy": "static",
            "patterns": [
                {
                    "statistic_field" : "event-time",
                    "statistic_type" : "integer",
                    "lifecycle": "hot",
                    "is_offset": true
                }]
           })";
        LifecycleConfig lifecycleConfig;
        ASSERT_THROW(autil::legacy::FromJsonString(lifecycleConfig, jsonStr), util::BadParameterException);
    }
    {
        std::string jsonStr = R"({
            "strategy": "dynamic"
            })";
        LifecycleConfig lifecycleConfig;
        ASSERT_NO_THROW(autil::legacy::FromJsonString(lifecycleConfig, jsonStr));
    }
    {
        std::string jsonStr = R"({
            "strategy": "dynamic",
            "patterns": [
                {
                    "statistic_field" : "event-time",
                    "statistic_type" : "integer",
                    "lifecycle": "hot",
                    "is_offset": true
                }]
           })";
        LifecycleConfig lifecycleConfig;
        ASSERT_NO_THROW(autil::legacy::FromJsonString(lifecycleConfig, jsonStr));
    }
    {
        std::string jsonStr = R"({
            "strategy": "bad-strategy-type"
            })";
        LifecycleConfig lifecycleConfig;
        ASSERT_THROW(autil::legacy::FromJsonString(lifecycleConfig, jsonStr), util::BadParameterException);
    }
}

}} // namespace indexlib::file_system

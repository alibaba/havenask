#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/SummaryProfileInfo.h>

using namespace std;
BEGIN_HA3_NAMESPACE(config);

class SummaryProfileInfoTest : public TESTBASE {
public:
    SummaryProfileInfoTest();
    ~SummaryProfileInfoTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkSummaryProfileInfo(SummaryProfileInfo &summaryProfileInfo);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, SummaryProfileInfoTest);


SummaryProfileInfoTest::SummaryProfileInfoTest() { 
}

SummaryProfileInfoTest::~SummaryProfileInfoTest() { 
}

void SummaryProfileInfoTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SummaryProfileInfoTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SummaryProfileInfoTest, testSerilizeAndDeserilize) { 
    string configStr =  "{\
        \"summary_profile_name\": \"DefaultProfile\",\
        \"extractor\" :{\
            \"extractor_name\" : \"extractor_1\",\
            \"module_name\" : \"fake_extractor\",\
            \"parameters\" : {\
                \"key\": \"value\"\
            }\
        },\
        \"field_configs\" : {\
            \"title\" : {\
                \"max_snippet\" : 1,\
                \"max_summary_length\" : 150,\
                \"highlight_prefix\": \"<tag>\",\
                \"highlight_suffix\": \"</tag>\",\
                \"snippet_delimiter\": \"...\"\
            }\
        }\
    }";
    SummaryProfileInfo summaryProfileInfo;
    FromJsonString(summaryProfileInfo, configStr);
    checkSummaryProfileInfo(summaryProfileInfo);

    string tmpString = ToJsonString(summaryProfileInfo);
    SummaryProfileInfo summaryProfileInfo2;
    FromJsonString(summaryProfileInfo2, tmpString);
    checkSummaryProfileInfo(summaryProfileInfo2);
}

TEST_F(SummaryProfileInfoTest, testSerilizeAndDeserilizeWithoutSomeSection) { 
    string configStr =  "{\
        \"summary_profile_name\": \"DefaultProfile\",\
        \"extractor\" :{\
            \"extractor_name\" : \"extractor_1\",\
            \"module_name\" : \"fake_extractor\"\
        }\
    }";

    SummaryProfileInfo summaryProfileInfo;
    FromJsonString(summaryProfileInfo, configStr);

    ASSERT_EQ(string("DefaultProfile"), summaryProfileInfo._summaryProfileName);
    ASSERT_EQ((size_t)1, summaryProfileInfo._extractorInfos.size());
    ExtractorInfo &extractorInfo = summaryProfileInfo._extractorInfos[0];
    ASSERT_EQ(string("extractor_1"), extractorInfo._extractorName);
    ASSERT_EQ(string("fake_extractor"), extractorInfo._moduleName);

    KeyValueMap &parameters = extractorInfo._parameters;
    ASSERT_EQ((size_t)0, parameters.size());

    FieldSummaryConfigMap fieldSummaryConfigMap = summaryProfileInfo._fieldSummaryConfigMap;
    ASSERT_EQ((size_t)0, fieldSummaryConfigMap.size());
}

TEST_F(SummaryProfileInfoTest, testForExtractorChain) {
    string configStr =  "{\
        \"summary_profile_name\": \"DefaultProfile\",\
        \"extractors\" :[\
            {\
              \"extractor_name\" : \"extractor_1\",\
              \"module_name\" : \"fake_extractor\"\
            },\
            {\
              \"extractor_name\" : \"extractor_2\",\
              \"module_name\" : \"fake_extractor_2\"\
            }\
        ],\
        \"extractor\" :{\
            \"extractor_name\" : \"extractor_3\",\
            \"module_name\" : \"fake_extractor_3\"\
        }\
    }";
    SummaryProfileInfo summaryProfileInfo;
    FromJsonString(summaryProfileInfo, configStr);

    ASSERT_EQ(string("DefaultProfile"), summaryProfileInfo._summaryProfileName);
    ASSERT_EQ((size_t)2, summaryProfileInfo._extractorInfos.size());
    ExtractorInfo &extractorInfo1 = summaryProfileInfo._extractorInfos[0];
    ASSERT_EQ(string("extractor_1"), extractorInfo1._extractorName);
    ASSERT_EQ(string("fake_extractor"), extractorInfo1._moduleName);
    ExtractorInfo &extractorInfo2 = summaryProfileInfo._extractorInfos[1];
    ASSERT_EQ(string("extractor_2"), extractorInfo2._extractorName);
    ASSERT_EQ(string("fake_extractor_2"), extractorInfo2._moduleName);
}

void SummaryProfileInfoTest::checkSummaryProfileInfo(SummaryProfileInfo &summaryProfileInfo) {
    ASSERT_EQ(string("DefaultProfile"), summaryProfileInfo._summaryProfileName);
    ASSERT_EQ((size_t)1, summaryProfileInfo._extractorInfos.size());
    ExtractorInfo &extractorInfo = summaryProfileInfo._extractorInfos[0];
    ASSERT_EQ(string("extractor_1"), extractorInfo._extractorName);
    ASSERT_EQ(string("fake_extractor"), extractorInfo._moduleName);
    KeyValueMap &parameters = extractorInfo._parameters;
    ASSERT_EQ((size_t)1, parameters.size());
    ASSERT_EQ(string("value"), parameters["key"]);
    FieldSummaryConfigMap fieldSummaryConfigMap = summaryProfileInfo._fieldSummaryConfigMap;
    ASSERT_EQ((size_t)1, fieldSummaryConfigMap.size());
     FieldSummaryConfig fieldSummaryConfig = fieldSummaryConfigMap["title"];
    ASSERT_EQ(1u, fieldSummaryConfig._maxSnippet);
    ASSERT_EQ(150u, fieldSummaryConfig._maxSummaryLength);
    ASSERT_EQ(string("<tag>"), fieldSummaryConfig._highlightPrefix);
    ASSERT_EQ(string("</tag>"), fieldSummaryConfig._highlightSuffix);
    ASSERT_EQ(string("..."), fieldSummaryConfig._snippetDelimiter);
}


END_HA3_NAMESPACE(config);


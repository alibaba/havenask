#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ProcessorInfo.h>

using namespace std;
BEGIN_HA3_NAMESPACE(config);

class ProcessorInfoTest : public TESTBASE {
public:
    ProcessorInfoTest();
    ~ProcessorInfoTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestJsonize(const std::string &jsonStr);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, ProcessorInfoTest);


ProcessorInfoTest::ProcessorInfoTest() { 
}

ProcessorInfoTest::~ProcessorInfoTest() { 
}

void ProcessorInfoTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ProcessorInfoTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ProcessorInfoTest, testJsonize) { 
    HA3_LOG(DEBUG, "Begin Test!");

    // qrs processor config
    string jsonStr = "{\n\
\"processor_name\" : \"fakeProcessor\",\n\
\"module_name\" : \"fakeModule\" \n\
}";
    innerTestJsonize(jsonStr);

    // document processor config
    jsonStr = "{\n\
\"class_name\" : \"fakeProcessor\",\n\
\"module_name\" : \"fakeModule\" \n\
}";
    innerTestJsonize(jsonStr);

    // if both exist, use class_name to override processor_name
    jsonStr = "{\n\
\"class_name\" : \"fakeProcessor\",\n\
\"processor_name\" : \"fakeProcessor1\",\n\
\"module_name\" : \"fakeModule\" \n\
}";
    innerTestJsonize(jsonStr);
}

void ProcessorInfoTest::innerTestJsonize(const string &jsonStr) {
    ProcessorInfo info;
    FromJsonString(info, jsonStr);
    ASSERT_EQ(string("fakeProcessor"), info._processorName);
    ASSERT_EQ(string("fakeModule"), info._moduleName);
    string serializedString = ToJsonString(info);
    ProcessorInfo checkInfo;
    FromJsonString(checkInfo, serializedString);
    ASSERT_TRUE(info == checkInfo);
}

END_HA3_NAMESPACE(config);


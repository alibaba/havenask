#include "suez/admin/SuezProtoJsonizer.h"

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "suez/admin/Admin.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace suez {

class SuezProtoJsonizerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SuezProtoJsonizerTest::setUp() {}

void SuezProtoJsonizerTest::tearDown() {}

TEST_F(SuezProtoJsonizerTest, testSimple) {
    // string
    // {
    //     Json::Value str(string("test"));
    //     Json::Value formatedStr = SuezProtoJsonizer::formatJson(str);
    //     cout << formatedStr << endl;
    //     // ASSERT_EQ("\"test\"", ToJsonString(formatedStr));
    // }
    // // arrary
    // {
    //     JsonArray array;
    //     array.push_back(Any(string("a")));
    //     array.push_back(Any(string("b")));
    //     JsonMap jsonMap;
    //     jsonMap["test"] = Any(ToJsonString(array));
    //     JsonMap expectedJson;
    //     expectedJson["test"] = Any(array);
    //     cout << ToJsonString(jsonMap) << endl;
    //     cout << "===formated===" << endl;
    //     cout << ToJsonString(SuezProtoJsonizer::formatJson(jsonMap)) << endl;
    //     cout << "==============" << endl;
    //     ASSERT_EQ(ToJsonString(expectedJson),
    //             ToJsonString(SuezProtoJsonizer::formatJson(jsonMap)));
    // }
    // // map
    // {
    //     JsonMap test;
    //     test["a"] = Any(string("b"));
    //     JsonMap jsonMap;
    //     jsonMap["test"] = Any(ToJsonString(test));
    //     JsonMap expectedJson;
    //     expectedJson["test"] = Any(test);
    //     cout << ToJsonString(jsonMap) << endl;
    //     cout << "===formated===" << endl;
    //     cout << ToJsonString(SuezProtoJsonizer::formatJson(jsonMap)) << endl;
    //     cout << "==============" << endl;
    //     ASSERT_EQ(ToJsonString(expectedJson),
    //             ToJsonString(SuezProtoJsonizer::formatJson(jsonMap)));
    // }
    // // bool
    // {
    //     Any test = Any(false);
    //     JsonMap jsonMap;
    //     jsonMap["test"] = Any(ToJsonString(test));
    //     JsonMap expectedJson;
    //     expectedJson["test"] = Any(test);
    //     cout << ToJsonString(jsonMap) << endl;
    //     cout << "===formated===" << endl;
    //     cout << ToJsonString(SuezProtoJsonizer::formatJson(jsonMap)) << endl;
    //     cout << "==============" << endl;
    //     ASSERT_EQ(ToJsonString(expectedJson),
    //             ToJsonString(SuezProtoJsonizer::formatJson(jsonMap)));
    // }
    // // int
    // {
    //     Any test = Any(1024);
    //     JsonMap jsonMap;
    //     jsonMap["test"] = Any(ToJsonString(test));
    //     JsonMap expectedJson;
    //     expectedJson["test"] = Any(test);
    //     cout << ToJsonString(jsonMap) << endl;
    //     cout << "===formated===" << endl;
    //     cout << ToJsonString(SuezProtoJsonizer::formatJson(jsonMap)) << endl;
    //     cout << "==============" << endl;
    //     ASSERT_EQ(ToJsonString(expectedJson),
    //             ToJsonString(SuezProtoJsonizer::formatJson(jsonMap)));
    // }
    {
        SuezProtoJsonizer suezProtoJsonizer;
        ifstream ifs("/tmp/xxx");
        string str((istreambuf_iterator<char>(ifs)), istreambuf_iterator<char>());
        ReadResponse readResponse;
        readResponse.set_nodevalue(str);
        readResponse.set_format(true);
    }
}

} // namespace suez

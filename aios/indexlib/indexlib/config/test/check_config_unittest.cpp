#include "indexlib/test/test.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include <fstream>
#include "indexlib/config/schema_configurator.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/json.h"
#include "indexlib/test/test.h"

IE_NAMESPACE_BEGIN(config);
using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace indexlib;
using namespace autil::legacy::json;

class CheckConfigTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(CheckConfigTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForCheckConfig()
    {
        // read config file
        std::string confFile = string(TEST_DATA_PATH) + "check_me.json";

        ifstream in(confFile.c_str());
        string line;
        string jsonString;
        while(getline(in, line))
        {
            jsonString += line;
        }
        
        Any any = ParseJson(jsonString);
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
    }

    void TestCaseForCheckConfigWithTruncate()
    {
        // read config file
        std::string confFile = string(TEST_DATA_PATH) + "check_me_with_truncate.json";

        ifstream in(confFile.c_str());
        string line;
        string jsonString;
        while(getline(in, line))
        {
            jsonString += line;
        }
        
        Any any = ParseJson(jsonString);
        IndexPartitionSchemaPtr schema (new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
    }
private:
};

INDEXLIB_UNIT_TEST_CASE(CheckConfigTest, TestCaseForCheckConfig);
INDEXLIB_UNIT_TEST_CASE(CheckConfigTest, TestCaseForCheckConfigWithTruncate);

IE_NAMESPACE_END(config);

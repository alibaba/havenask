#include <fslib/fslib.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "indexlib/config/attribute_config.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h" 
#include "indexlib/test/unittest.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(index);

class SingleValueAttributePatchReaderTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH() + "/";
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSequentiallyReadPatch()
    {
        string dataStr("1:20,2:30,3:45;1:48;2:40");
        MakePatchData(dataStr);

        SingleValueAttributePatchReader<int32_t> reader(MakeAttributeConfig());
        string patchFileName = string(".") + ATTRIBUTE_PATCH_FILE_NAME;
        reader.AddPatchFile(mRootDir + "0" + patchFileName, 0);
        reader.AddPatchFile(mRootDir + "1" + patchFileName, 1);
        reader.AddPatchFile(mRootDir + "2" + patchFileName, 2);

        int32_t value;
        INDEXLIB_TEST_TRUE(!reader.ReadPatch(0, value));
        INDEXLIB_TEST_TRUE(reader.ReadPatch(1, value));
        INDEXLIB_TEST_EQUAL(48, value);
        INDEXLIB_TEST_TRUE(reader.ReadPatch(2, value));
        INDEXLIB_TEST_EQUAL(40, value);
        INDEXLIB_TEST_TRUE(reader.ReadPatch(3, value));
        INDEXLIB_TEST_EQUAL(45, value);
        INDEXLIB_TEST_TRUE(!reader.ReadPatch(4, value));
    }

    void TestCaseForUnSequentiallyReadPatchWithoutLastValidDoc()
    {
        string dataStr("1:20,2:30,3:45;1:48;2:40");
        MakePatchData(dataStr);

        SingleValueAttributePatchReader<int32_t> reader(MakeAttributeConfig());
        string patchFileName = string(".") + ATTRIBUTE_PATCH_FILE_NAME;
        reader.AddPatchFile(mRootDir + "0" + patchFileName, 0);
        reader.AddPatchFile(mRootDir + "1" + patchFileName, 1);
        reader.AddPatchFile(mRootDir + "2" + patchFileName, 2);

        int32_t value;
        INDEXLIB_TEST_TRUE(!reader.ReadPatch(0, value));
        INDEXLIB_TEST_TRUE(reader.ReadPatch(1, value));
        INDEXLIB_TEST_EQUAL(48, value);
        INDEXLIB_TEST_TRUE(reader.ReadPatch(2, value));
        INDEXLIB_TEST_EQUAL(40, value);
        INDEXLIB_TEST_TRUE(!reader.ReadPatch(4, value));
    }    

    void TestCaseForUnSequentiallyReadPatch()
    {
        string dataStr("1:20,2:30,3:45;1:48;2:40");
        MakePatchData(dataStr);

        SingleValueAttributePatchReader<int32_t> reader(MakeAttributeConfig());
        string patchFileName = string(".") + ATTRIBUTE_PATCH_FILE_NAME;
        reader.AddPatchFile(mRootDir + "0" + patchFileName, 0);
        reader.AddPatchFile(mRootDir + "1" + patchFileName, 1);
        reader.AddPatchFile(mRootDir + "2" + patchFileName, 2);

        int32_t value;
        INDEXLIB_TEST_TRUE(!reader.ReadPatch(0, value));
        INDEXLIB_TEST_TRUE(reader.ReadPatch(1, value));
        INDEXLIB_TEST_EQUAL(48, value);
        INDEXLIB_TEST_TRUE(reader.ReadPatch(3, value));
        INDEXLIB_TEST_EQUAL(45, value);
        INDEXLIB_TEST_TRUE(!reader.ReadPatch(4, value));
    }


private:
    config::AttributeConfigPtr MakeAttributeConfig()
    {
        config::FieldConfigPtr fieldConfig(new config::FieldConfig("field", ft_int32, false));
        config::AttributeConfigPtr attrConfig(new config::AttributeConfig);
        attrConfig->Init(fieldConfig);
        return attrConfig;
    }
    
    void MakePatchData(const string& dataStr)
    {
        StringTokenizer st1(dataStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st1.getNumTokens(); ++i)
        {
            StringTokenizer st2(st1[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                    | StringTokenizer::TOKEN_TRIM);
            stringstream ss;
            ss << mRootDir << i << '.' << ATTRIBUTE_PATCH_FILE_NAME;
            File* file = FileSystem::openFile(ss.str(), WRITE);
            for (size_t j = 0; j < st2.getNumTokens(); ++j)
            {
                StringTokenizer st3(st2[j], ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
                assert(st3.getNumTokens() == 2);
                docid_t docId;
                StringUtil::strToInt32(st3[0].c_str(), docId);
                int32_t value;
                StringUtil::strToInt32(st3[1].c_str(), value);
                file->write((void*)&docId, sizeof(docId));
                file->write((void*)&value, sizeof(value));
            }
            file->close();
            delete file;
        }
    }

private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributePatchReaderTest, TestCaseForSequentiallyReadPatch);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributePatchReaderTest, TestCaseForUnSequentiallyReadPatchWithoutLastValidDoc);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributePatchReaderTest, TestCaseForUnSequentiallyReadPatch);

IE_NAMESPACE_END(index);


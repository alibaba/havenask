#include "indexlib/common_define.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;

namespace indexlib { namespace index {

class VarLenDataParamHelperTest : public INDEXLIB_TESTBASE
{
public:
    VarLenDataParamHelperTest();
    ~VarLenDataParamHelperTest();

    DECLARE_CLASS_NAME(VarLenDataParamHelperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForSummary();
    void TestCaseForSource();

private:
    void CheckParam(const VarLenDataParam& lft, const VarLenDataParam& rht)
    {
        ASSERT_EQ(lft.enableAdaptiveOffset, rht.enableAdaptiveOffset);
        ASSERT_EQ(lft.equalCompressOffset, rht.equalCompressOffset);
        ASSERT_EQ(lft.dataItemUniqEncode, rht.dataItemUniqEncode);
        ASSERT_EQ(lft.appendDataItemLength, rht.appendDataItemLength);
        ASSERT_EQ(lft.disableGuardOffset, rht.disableGuardOffset);
        ASSERT_EQ(lft.offsetThreshold, rht.offsetThreshold);
        ASSERT_EQ(lft.dataCompressorName, rht.dataCompressorName);
        ASSERT_EQ(lft.dataCompressBufferSize, rht.dataCompressBufferSize);
    }

    /* paramStr= adaptiveOffset[1000]|equal|uniq|appendLen|noGuardOffset|compressor=zlib[2048] */
    VarLenDataParam CreateParam(const string& paramStr)
    {
        VarLenDataParam param;
        vector<string> tmpVec;
        StringUtil::fromString(paramStr, tmpVec, "|");
        for (auto str : tmpVec) {
            if (str == "equal") {
                param.equalCompressOffset = true;
            }
            if (str == "uniq") {
                param.dataItemUniqEncode = true;
            }
            if (str == "appendLen") {
                param.appendDataItemLength = true;
            }
            if (str == "noGuardOffset") {
                param.disableGuardOffset = true;
            }
            if (str.find("adaptiveOffset") == 0) {
                param.enableAdaptiveOffset = true;
                string value = str.substr(14);
                if (value[0] == '[') {
                    assert(value[value.length() - 1] == ']');
                    StringUtil::fromString(value.substr(1, value.length() - 2), param.offsetThreshold);
                }
            }
            if (str.find("compressor=") == 0) {
                string value = str.substr(11);
                size_t pos = value.find("[");
                if (pos == string::npos) {
                    param.dataCompressorName = value;
                } else {
                    param.dataCompressorName = value.substr(0, pos);
                    assert(value[value.length() - 1] == ']');
                    StringUtil::fromString(value.substr(pos + 1, value.length() - pos - 2),
                                           param.dataCompressBufferSize);
                }
            }
        }
        return param;
    }

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarLenDataParamHelperTest, TestCaseForSource);
INDEXLIB_UNIT_TEST_CASE(VarLenDataParamHelperTest, TestCaseForSummary);

IE_LOG_SETUP(index, VarLenDataParamHelperTest);

VarLenDataParamHelperTest::VarLenDataParamHelperTest() {}

VarLenDataParamHelperTest::~VarLenDataParamHelperTest() {}

void VarLenDataParamHelperTest::CaseSetUp() {}

void VarLenDataParamHelperTest::CaseTearDown() {}

void VarLenDataParamHelperTest::TestCaseForSummary()
{
    string paramStr = R"({
        "compress_type" : "uniq|equal",
        "doc_compressor" : "zlib",
        "file_compressor" : "snappy",
        "file_compress_buffer_size" : 8192
    })";

    GroupDataParameter param;
    SummaryGroupConfigPtr config(new SummaryGroupConfig);

    {
        config->SetSummaryGroupDataParam(param);
        auto ret = VarLenDataParamHelper::MakeParamForSummary(config);
        auto expect = CreateParam("noGuardOffset");
        CheckParam(expect, ret);
    }

    {
        FromJsonString(param, paramStr);
        config->SetEnableAdaptiveOffset(true);
        config->SetSummaryGroupDataParam(param);
        auto ret = VarLenDataParamHelper::MakeParamForSummary(config);
        auto expect = CreateParam("adaptiveOffset|equal|uniq|appendLen|noGuardOffset|compressor=snappy[8192]");
        CheckParam(expect, ret);
    }
}

void VarLenDataParamHelperTest::TestCaseForSource()
{
    string paramStr = R"({
        "compress_type" : "uniq|equal",
        "doc_compressor" : "zlib",
        "file_compressor" : "snappy",
        "file_compress_buffer_size" : 8192
    })";

    GroupDataParameter param;
    SourceGroupConfigPtr config(new SourceGroupConfig);

    {
        config->SetParameter(param);
        auto ret = VarLenDataParamHelper::MakeParamForSourceData(config);
        auto expect = CreateParam("adaptiveOffset");
        CheckParam(expect, ret);
    }

    {
        FromJsonString(param, paramStr);
        config->SetParameter(param);
        auto ret = VarLenDataParamHelper::MakeParamForSourceData(config);
        auto expect = CreateParam("adaptiveOffset|equal|uniq|appendLen|compressor=snappy[8192]");
        CheckParam(expect, ret);
    }
}
}} // namespace indexlib::index

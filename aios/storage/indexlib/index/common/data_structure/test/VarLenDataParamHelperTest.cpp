#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"

#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace indexlib::config;

namespace indexlibv2::index {

class VarLenDataParamHelperTest : public TESTBASE
{
public:
    VarLenDataParamHelperTest();
    ~VarLenDataParamHelperTest();

public:
    void setUp() override {}
    void tearDown() override {}

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

    /* para_str= adaptiveOffset[1000]|equal|uniq|appendLen|noGuardOffset|compressor=zlib[2048] */
    VarLenDataParam CreateParam(const std::string& para_str)
    {
        VarLenDataParam param;
        std::vector<std::string> tmpVec;
        StringUtil::fromString(para_str, tmpVec, "|");
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
                std::string value = str.substr(14);
                if (value[0] == '[') {
                    assert(value[value.length() - 1] == ']');
                    StringUtil::fromString(value.substr(1, value.length() - 2), param.offsetThreshold);
                }
            }
            if (str.find("compressor=") == 0) {
                std::string value = str.substr(11);
                size_t pos = value.find("[");
                if (pos == std::string::npos) {
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
    AUTIL_LOG_DECLARE();
};

VarLenDataParamHelperTest::VarLenDataParamHelperTest() {}

VarLenDataParamHelperTest::~VarLenDataParamHelperTest() {}

TEST_F(VarLenDataParamHelperTest, TestCaseForSummary)
{
    std::string para_str = R"({
        "compress_type" : "uniq|equal",
        "doc_compressor" : "zlib",
        "file_compressor" : "snappy",
        "file_compress_buffer_size" : 8192
    })";

    GroupDataParameter param;
    std::shared_ptr<indexlibv2::config::SummaryGroupConfig> config(new indexlibv2::config::SummaryGroupConfig);

    {
        config->SetSummaryGroupDataParam(param);
        auto ret = VarLenDataParamHelper::MakeParamForSummary(config);
        auto expect = CreateParam("noGuardOffset");
        CheckParam(expect, ret);
    }

    {
        autil::legacy::FromJsonString(param, para_str);
        config->SetEnableAdaptiveOffset(true);
        config->SetSummaryGroupDataParam(param);
        auto ret = VarLenDataParamHelper::MakeParamForSummary(config);
        auto expect = CreateParam("adaptiveOffset|equal|uniq|appendLen|noGuardOffset|compressor=snappy[8192]");
        CheckParam(expect, ret);
    }
}

TEST_F(VarLenDataParamHelperTest, TestCaseForSource)
{
    std::string para_str = R"({
        "compress_type" : "uniq|equal",
        "doc_compressor" : "zlib",
        "file_compressor" : "snappy",
        "file_compress_buffer_size" : 8192
    })";

    GroupDataParameter param;
    std::shared_ptr<indexlib::config::SourceGroupConfig> config(new SourceGroupConfig);

    {
        config->SetParameter(param);
        auto ret = VarLenDataParamHelper::MakeParamForSourceData(config);
        auto expect = CreateParam("adaptiveOffset");
        CheckParam(expect, ret);
    }

    {
        FromJsonString(param, para_str);
        config->SetParameter(param);
        auto ret = VarLenDataParamHelper::MakeParamForSourceData(config);
        auto expect = CreateParam("adaptiveOffset|equal|uniq|appendLen|compressor=snappy[8192]");
        CheckParam(expect, ret);
    }
}
} // namespace indexlibv2::index

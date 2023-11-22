#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_fs_writer_param_decider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class AttributeFsWriterParamDeciderTest : public INDEXLIB_TESTBASE
{
public:
    AttributeFsWriterParamDeciderTest();
    ~AttributeFsWriterParamDeciderTest();

    DECLARE_CLASS_NAME(AttributeFsWriterParamDeciderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMakeParamForSingleAttribute();
    void TestMakeParamForVarNumAttribute();
    void TestMakeParamForUpdatableVarNumAttribute();
    void TestMakeParamForSingleString();
    void TestMakeParamForMultiString();

private:
    void InnerTestMakeParam(bool isOnline, bool needFlushRt, const config::AttributeConfigPtr& attrConfig,
                            config::AttributeConfig::ConfigType confType, const std::string& fileName,
                            const file_system::WriterOption& expectParam);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForSingleAttribute);
INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForVarNumAttribute);
INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForUpdatableVarNumAttribute);
INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForSingleString);
INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForMultiString);
}} // namespace indexlib::index

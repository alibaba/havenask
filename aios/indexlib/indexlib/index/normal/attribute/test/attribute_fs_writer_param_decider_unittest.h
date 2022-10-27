#ifndef __INDEXLIB_ATTRIBUTEFSWRITERPARAMDECIDERTEST_H
#define __INDEXLIB_ATTRIBUTEFSWRITERPARAMDECIDERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/attribute_fs_writer_param_decider.h"

IE_NAMESPACE_BEGIN(index);

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
    void InnerTestMakeParam(bool isOnline, bool needFlushRt,
                            const config::AttributeConfigPtr& attrConfig,
                            config::AttributeConfig::ConfigType confType,
                            const std::string& fileName,
                            const file_system::FSWriterParam& expectParam);
                            
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForSingleAttribute);
INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForVarNumAttribute);
INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForUpdatableVarNumAttribute);
INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForSingleString);
INDEXLIB_UNIT_TEST_CASE(AttributeFsWriterParamDeciderTest, TestMakeParamForMultiString);


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTEFSWRITERPARAMDECIDERTEST_H

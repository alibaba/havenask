#ifndef __INDEXLIB_SINGLEVALUEATTRIBUTEWRITERGTESTTEST_H
#define __INDEXLIB_SINGLEVALUEATTRIBUTEWRITERGTESTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"

IE_NAMESPACE_BEGIN(index);

class MockAttributeWriter : public SingleValueAttributeWriter<uint32_t>
{
public:
    MockAttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : SingleValueAttributeWriter<uint32_t>(attrConfig)
    {}
};

class AttributeWriterTest : public INDEXLIB_TESTBASE {
public:
    AttributeWriterTest();
    ~AttributeWriterTest();
public:
    void SetUp();
    void TearDown();
private:
    std::string mRootDir;
    
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLEVALUEATTRIBUTEWRITERGTESTTEST_H

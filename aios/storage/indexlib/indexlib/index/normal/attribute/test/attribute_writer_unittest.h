#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class MockAttributeWriter : public SingleValueAttributeWriter<uint32_t>
{
public:
    MockAttributeWriter(const config::AttributeConfigPtr& attrConfig) : SingleValueAttributeWriter<uint32_t>(attrConfig)
    {
    }
};

class AttributeWriterTest : public INDEXLIB_TESTBASE
{
public:
    AttributeWriterTest();
    ~AttributeWriterTest();

public:
    void CaseSetUp();
    void CaseTearDown();

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

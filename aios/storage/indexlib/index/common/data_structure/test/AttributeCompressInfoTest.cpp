#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class AttributeCompressInfoTest : public TESTBASE
{
public:
    AttributeCompressInfoTest() = default;
    ~AttributeCompressInfoTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void AttributeCompressInfoTest::setUp() {}

void AttributeCompressInfoTest::tearDown() {}

TEST_F(AttributeCompressInfoTest, TestNeedCompressData)
{
    {
        std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig);
        std::shared_ptr<config::FieldConfig> fieldConfig(
            new config::FieldConfig("field", ft_string, false, false, false));

        ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
        ASSERT_TRUE(attrConfig->SetCompressType("equal|uniq").IsOK());
        ASSERT_TRUE(!AttributeCompressInfo::NeedCompressData(attrConfig));
    }

    {
        std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig);
        std::shared_ptr<config::FieldConfig> fieldConfig(
            new config::FieldConfig("field", ft_integer, true, false, false));

        ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
        ASSERT_TRUE(attrConfig->SetCompressType("equal|uniq").IsOK());
        ASSERT_TRUE(!AttributeCompressInfo::NeedCompressData(attrConfig));
    }

    {
        std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig);
        std::shared_ptr<config::FieldConfig> fieldConfig(
            new config::FieldConfig("field", ft_integer, false, false, false));

        ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
        ASSERT_TRUE(attrConfig->SetCompressType("equal|uniq").IsOK());
        ASSERT_TRUE(AttributeCompressInfo::NeedCompressData(attrConfig));
    }
}

TEST_F(AttributeCompressInfoTest, TestNeedCompressOffset)
{
    {
        std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig);
        std::shared_ptr<config::FieldConfig> fieldConfig(
            new config::FieldConfig("field", ft_string, false, false, false));

        ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
        ASSERT_TRUE(attrConfig->SetCompressType("equal|uniq").IsOK());
        ASSERT_TRUE(AttributeCompressInfo::NeedCompressOffset(attrConfig));
    }

    {
        std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig);
        std::shared_ptr<config::FieldConfig> fieldConfig(
            new config::FieldConfig("field", ft_integer, true, false, false));

        ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
        ASSERT_TRUE(attrConfig->SetCompressType("equal|uniq").IsOK());
        ASSERT_TRUE(AttributeCompressInfo::NeedCompressOffset(attrConfig));
    }

    {
        std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig);
        std::shared_ptr<config::FieldConfig> fieldConfig(
            new config::FieldConfig("field", ft_integer, false, false, false));

        ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
        ASSERT_TRUE(attrConfig->SetCompressType("equal|uniq").IsOK());
        ASSERT_FALSE(AttributeCompressInfo::NeedCompressOffset(attrConfig));
    }
}
} // namespace indexlibv2::index

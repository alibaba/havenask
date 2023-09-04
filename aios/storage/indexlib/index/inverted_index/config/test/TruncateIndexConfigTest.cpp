#include "indexlib/index/inverted_index/config/TruncateIndexConfig.h"

#include <string>

#include "indexlib/index/inverted_index/config/TruncateIndexProperty.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class TruncateIndexConfigTest : public TESTBASE
{
    TruncateIndexConfigTest() = default;
    ~TruncateIndexConfigTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.config, TruncateIndexConfigTest);
TEST_F(TruncateIndexConfigTest, TestIndexName)
{
    auto config = std::make_unique<TruncateIndexConfig>();
    ASSERT_TRUE(config->GetIndexName().empty());
    const std::string indexName("ut");
    config->SetIndexName(indexName);
    ASSERT_EQ(indexName, config->GetIndexName());
    const std::string indexName1("ut1");
    config->SetIndexName(indexName1);
    // test override
    ASSERT_EQ(indexName1, config->GetIndexName());
}

TEST_F(TruncateIndexConfigTest, TestTruncateIndexProperty)
{
    auto config = std::make_unique<TruncateIndexConfig>();
    ASSERT_EQ(0, config->GetTruncateIndexPropertySize());

    TruncateIndexProperty property1;
    property1.truncateIndexName = std::string("ut1");
    config->AddTruncateIndexProperty(property1);
    ASSERT_EQ(1, config->GetTruncateIndexPropertySize());
    ASSERT_EQ("ut1", config->GetTruncateIndexProperty(0).truncateIndexName);

    TruncateIndexProperty property2;
    property2.truncateIndexName = std::string("ut2");
    config->AddTruncateIndexProperty(property2);
    ASSERT_EQ(2, config->GetTruncateIndexPropertySize());
    ASSERT_EQ("ut1", config->GetTruncateIndexProperty(0).truncateIndexName);
    ASSERT_EQ("ut2", config->GetTruncateIndexProperty(1).truncateIndexName);
}

} // namespace indexlibv2::config

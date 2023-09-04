#include "indexlib/index/operation_log/OperationLogConfig.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "unittest/unittest.h"
using namespace indexlibv2::config;

namespace indexlib::index {
class OperationLogConfigTest : public TESTBASE
{
public:
    OperationLogConfigTest() = default;
    ~OperationLogConfigTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void OperationLogConfigTest::setUp() {}

void OperationLogConfigTest::tearDown() {}

TEST_F(OperationLogConfigTest, testNoOperation)
{
    OperationLogConfig config(100, true);
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    fieldConfigs.push_back(std::make_shared<FieldConfig>("field0", ft_int32, false));
    fieldConfigs.push_back(std::make_shared<FieldConfig>("field1", ft_int32, false));
    fieldConfigs.push_back(std::make_shared<FieldConfig>("field2", ft_int32, false));
    config.SetFieldConfigs(fieldConfigs);

    auto makeIndexConfig = [](std::shared_ptr<FieldConfig> fieldConfig) {
        static std::string str;

        class DummyConfig : public IIndexConfig
        {
        public:
            DummyConfig(std::shared_ptr<FieldConfig> config) : _config(config) {}
            const std::string& GetIndexType() const override { return str; }
            const std::string& GetIndexName() const override { return str; }
            const std::string& GetIndexCommonPath() const override { return str; }
            std::vector<std::string> GetIndexPath() const override { return {str}; }
            std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override { return {_config}; }
            void Check() const override {}
            void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                             const IndexConfigDeserializeResource& resource) override
            {
            }
            void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override {}

            Status CheckCompatible(const IIndexConfig* other) const override { return Status::OK(); }
            bool IsDisabled() const override { return false; }

        private:
            std::shared_ptr<FieldConfig> _config;
        };
        return std::make_shared<DummyConfig>(fieldConfig);
    };

    auto getUsingFields = [&]() {
        std::vector<std::string> usingFieldNames;
        auto usingFields = config.GetFieldConfigs();
        for (auto field : usingFields) {
            usingFieldNames.push_back(field->GetFieldName());
        }
        return usingFieldNames;
    };

    EXPECT_THAT(getUsingFields(), testing::UnorderedElementsAre());

    config.AddIndexConfigs("type0", {makeIndexConfig(fieldConfigs[0])});
    EXPECT_THAT(getUsingFields(), testing::UnorderedElementsAre());

    config.AddIndexConfigs("type1", {makeIndexConfig(fieldConfigs[1])});
    ASSERT_THAT(getUsingFields(), testing::UnorderedElementsAre());

    config.AddIndexConfigs("type2", {makeIndexConfig(fieldConfigs[2])});
    ASSERT_THAT(getUsingFields(), testing::UnorderedElementsAre());

    auto attrConfig0 = std::make_shared<indexlibv2::index::AttributeConfig>();
    ASSERT_TRUE(attrConfig0->Init(fieldConfigs[0]).IsOK());
    attrConfig0->SetUpdatable(false);
    auto attrConfig1 = std::make_shared<indexlibv2::index::AttributeConfig>();
    ASSERT_TRUE(attrConfig1->Init(fieldConfigs[1]).IsOK());
    attrConfig1->SetUpdatable(true);
    config.AddIndexConfigs(ATTRIBUTE_INDEX_TYPE_STR, {attrConfig0, attrConfig1});
    ASSERT_THAT(getUsingFields(), testing::UnorderedElementsAre("field1"));

    auto invertIndexConfig0 = std::make_shared<SingleFieldIndexConfig>("name1", it_number_int32);
    auto invertIndexConfig1 = std::make_shared<SingleFieldIndexConfig>("name2", it_number_int32);
    ASSERT_TRUE(invertIndexConfig0->SetFieldConfig(fieldConfigs[0]).IsOK());
    ASSERT_TRUE(invertIndexConfig1->SetFieldConfig(fieldConfigs[2]).IsOK());
    invertIndexConfig0->TEST_SetIndexUpdatable(false);
    invertIndexConfig1->TEST_SetIndexUpdatable(true);
    config.AddIndexConfigs(INVERTED_INDEX_TYPE_STR, {invertIndexConfig0, invertIndexConfig1});
    ASSERT_THAT(getUsingFields(), testing::UnorderedElementsAre("field1", "field2"));
}

} // namespace indexlib::index

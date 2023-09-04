#include "indexlib/index/inverted_index/truncate/DocCollectorCreator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/truncate/DocInfo.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/NoSortTruncateCollector.h"
#include "indexlib/index/inverted_index/truncate/ReferenceTyped.h"
#include "indexlib/index/inverted_index/truncate/SortTruncateCollector.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"
#include "indexlib/index/inverted_index/truncate/test/TruncateIndexUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DocCollectorCreatorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<indexlibv2::config::TruncateStrategy> GetTruncateStrategy(std::string& content) const;
    std::shared_ptr<indexlibv2::config::TruncateStrategy> GetDefaultTruncateStrategy() const;
    std::shared_ptr<indexlibv2::config::DiversityConstrain> GetDiversityConstrain(std::string& content) const;
    std::shared_ptr<indexlibv2::config::ITabletSchema> GetDefaultSchema() const;
};

std::shared_ptr<indexlibv2::config::TruncateStrategy>
DocCollectorCreatorTest::GetTruncateStrategy(std::string& content) const
{
    std::shared_ptr<indexlibv2::config::TruncateStrategy> strategy =
        std::make_shared<indexlibv2::config::TruncateStrategy>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, strategy.get()).Status();
    assert(ec.IsOK());
    return strategy;
}

std::shared_ptr<indexlibv2::config::TruncateStrategy> DocCollectorCreatorTest::GetDefaultTruncateStrategy() const
{
    std::string strategyStr = R"({
        "strategy_name": "price_filter_by_meta",
        "truncate_profiles": [
            "desc_price"
        ],
        "threshold": 10,
        "memory_optimize_threshold": 20,
        "limit": 5,
        "diversity_constrain": {
            "filter_field": "price",
            "filter_min_value": 50,
            "filter_max_value": 100,
            "distinct_field": "price",
            "distinct_expand_limit": 10,
            "distinct_count": 3
        }
    })";
    return GetTruncateStrategy(strategyStr);
}

std::shared_ptr<indexlibv2::config::DiversityConstrain>
DocCollectorCreatorTest::GetDiversityConstrain(std::string& content) const
{
    auto constrain = std::make_shared<indexlibv2::config::DiversityConstrain>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, constrain.get()).Status();
    assert(ec.IsOK());
    return constrain;
}

std::shared_ptr<indexlibv2::config::ITabletSchema> DocCollectorCreatorTest::GetDefaultSchema() const
{
    std::string schemaStr = R"({
        "attributes": [
            "price"
        ],
        "fields": [
            {
                "field_name": "key1",
                "field_type": "STRING"
            },
            {
                "field_name": "price",
                "field_type": "UINT32"
            }
        ],
        "indexs": [
            {
                "has_truncate": true,
                "use_truncate_profiles" : "desc_price",
                "index_fields": "key1",
                "index_type": "STRING",
                "index_name": "key1"
            },
            {
                "has_primary_key_attribute": true,
                "index_fields": "price",
                "index_name": "pk",
                "index_type": "PRIMARYKEY64"
            }
        ],
        "truncate_profiles" : [
            {
                "truncate_profile_name": "desc_price",
                "sort_descriptions" : "-price"
            }
        ],
        "table_name": "test"
    })";

    auto tabletSchema = std::make_shared<indexlibv2::config::TabletSchema>();
    assert(tabletSchema->Deserialize(schemaStr));
    auto fieldConfig = tabletSchema->GetFieldConfig("price");
    auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        assert(false);
        return nullptr;
    }
    auto ret = tabletSchema->_impl->AddIndexConfig(attrConfig);
    if (!ret.IsOK()) {
        return nullptr;
    }
    return tabletSchema;
}

TEST_F(DocCollectorCreatorTest, TestGetMaxDocCountToReserve)
{
    auto strategy = GetDefaultTruncateStrategy();
    ASSERT_EQ(10, DocCollectorCreator::GetMaxDocCountToReserve(strategy));

    // fake invalid distinctor, return min doc count
    strategy->_diversityConstrain._distField.clear();
    ASSERT_EQ(5, DocCollectorCreator::GetMaxDocCountToReserve(strategy));
}

TEST_F(DocCollectorCreatorTest, TestDoCreateTruncatorCollector)
{
    auto strategy = GetDefaultTruncateStrategy();

    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    auto docColloctor = DocCollectorCreator::DoCreateTruncatorCollector(strategy, /*docDistinctor*/ nullptr,
                                                                        /*docFilter*/ nullptr, /*bucketMap*/ nullptr,
                                                                        /*bucketVecAllocator*/ nullptr, profile,
                                                                        /*truncateAttrReaderCreator*/ nullptr);
    ASSERT_TRUE(dynamic_cast<NoSortTruncateCollector*>(docColloctor.get()) != nullptr);

    auto bucketMap = std::make_shared<BucketMap>(/*name*/ "ut_name", /*type*/ "ut_type");
    docColloctor = DocCollectorCreator::DoCreateTruncatorCollector(strategy, /*docDistinctor*/ nullptr,
                                                                   /*docFilter*/ nullptr, bucketMap,
                                                                   /*bucketVecAllocator*/ nullptr, profile, nullptr);
    ASSERT_TRUE(dynamic_cast<SortTruncateCollector*>(docColloctor.get()) != nullptr);
}

TEST_F(DocCollectorCreatorTest, TestCreateFilter)
{
    std::string str1 = R"({
        "filter_field" : "DOC_PAYLOAD",
        "distinct_count": 20000,
        "distinct_expand_limit": 120000
    })";
    std::shared_ptr<indexlibv2::config::DiversityConstrain> diversityConstrain = GetDiversityConstrain(str1);

    auto docPayloadFilter = DocCollectorCreator::CreateFilter(*diversityConstrain, /*truncateAttrCreator*/ nullptr,
                                                              /*docAllocator*/ nullptr, /*evaluator*/ nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<DocPayloadFilterProcessor>(docPayloadFilter) != nullptr);

    // auto allocator = std::make_shared<DocInfoAllocator>();
    // [[maybe_unused]]auto docPayloadRefer = allocator->DeclareReference("DOC_PAYLOAD", ft_uint16, /*supportNull*/
    // false);
    auto tabletSchema = GetDefaultSchema();
    ASSERT_TRUE(tabletSchema != nullptr);

    auto mergeInfos = TruncateIndexUtil::GetSegmentMergeInfos("price");
    auto creator = std::make_shared<TruncateAttributeReaderCreator>(tabletSchema, mergeInfos, /*docMapper*/ nullptr);
    std::string str2 = R"({
        "filter_field" : "NOT_EXIST_FIELD",
        "distinct_count": 20000,
        "distinct_expand_limit": 120000
    })";
    diversityConstrain = GetDiversityConstrain(str2);
    docPayloadFilter = DocCollectorCreator::CreateFilter(*diversityConstrain, creator, /*docAllocator*/ nullptr,
                                                         /*evaluator*/ nullptr);
    ASSERT_TRUE(docPayloadFilter == nullptr);
}

TEST_F(DocCollectorCreatorTest, TestCreateDistinctor)
{
    std::string str = R"({
        "filter_field" : "DOC_PAYLOAD",
        "distinct_count": 20000,
        "distinct_expand_limit": 120000
    })";

    auto tabletSchema = GetDefaultSchema();
    ASSERT_TRUE(tabletSchema != nullptr);

    auto mergeInfos = TruncateIndexUtil::GetSegmentMergeInfos("price");
    auto creator = std::make_shared<TruncateAttributeReaderCreator>(tabletSchema, mergeInfos, /*docMapper*/ nullptr);

    auto diversityConstrain = GetDiversityConstrain(str);
    auto docPayloadFilter =
        DocCollectorCreator::CreateDistinctor(*diversityConstrain, creator, /*docAllocator*/ nullptr);
    ASSERT_TRUE(docPayloadFilter == nullptr);
}

TEST_F(DocCollectorCreatorTest, TestSimpleProcess)
{
    auto schema = GetDefaultSchema();
    ASSERT_TRUE(schema != nullptr);

    auto truncateStrategy = GetDefaultTruncateStrategy();

    auto truncateProfile = std::make_shared<indexlibv2::config::TruncateProfile>();
    truncateProfile->truncateProfileName = "desc_price";

    indexlibv2::config::TruncateIndexProperty property;
    property.truncateStrategy = truncateStrategy;
    property.truncateProfile = truncateProfile;

    auto allocator = std::make_shared<DocInfoAllocator>();
    [[maybe_unused]] auto refer = allocator->DeclareReference(/*fieldName*/ "price", ft_int32, false);
    [[maybe_unused]] auto docInfo = allocator->Allocate();

    BucketMaps bucketMaps;
    auto bucketMap = std::make_shared<BucketMap>(/*name*/ "bucketmap", /*type*/ "bucketmap");
    bucketMaps["desc_price"] = bucketMap;

    auto mergeInfos = TruncateIndexUtil::GetSegmentMergeInfos("price");
    auto truncateAttributeCreator =
        std::make_shared<TruncateAttributeReaderCreator>(schema, mergeInfos, /*docMapper*/ nullptr);

    auto bucketVecCollector = std::make_shared<BucketVectorAllocator>();
    auto metaReader = std::shared_ptr<TruncateMetaReader>();

    auto docCollectorCreator = std::make_shared<DocCollectorCreator>();
    auto docCollector = docCollectorCreator->Create(property, allocator, bucketMaps, truncateAttributeCreator,
                                                    bucketVecCollector, metaReader, /*evaluator*/ nullptr);

    ASSERT_NE(docCollector, nullptr);
}

} // namespace indexlib::index

#include "indexlib/index/inverted_index/truncate/EvaluatorCreator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/PostingIteratorImpl.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/truncate/AttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/DocInfo.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/DocPayloadEvaluator.h"
#include "indexlib/index/inverted_index/truncate/MultiAttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/ReferenceTyped.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"
#include "indexlib/index/inverted_index/truncate/test/TruncateIndexUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class EvaluatorCreatorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<indexlibv2::config::DiversityConstrain> GetDiversityConstrain(std::string& content) const;
    std::shared_ptr<indexlibv2::config::ITabletSchema> GetDefaultSchema() const;
};
std::shared_ptr<indexlibv2::config::DiversityConstrain>
EvaluatorCreatorTest::GetDiversityConstrain(std::string& content) const
{
    auto constrain = std::make_shared<indexlibv2::config::DiversityConstrain>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, constrain.get()).Status();
    assert(ec.IsOK());
    return constrain;
}
std::shared_ptr<indexlibv2::config::ITabletSchema> EvaluatorCreatorTest::GetDefaultSchema() const
{
    std::string schemaStr = R"({
        "attributes": [
            "uvsum"
        ],
        "fields": [
            {
                "field_name": "nid",
                "field_type": "INTEGER"
            },
            {
                "field_name": "uvsum",
                "field_type": "INTEGER"
            }
        ],
        "indexs": [
            {
                "has_primary_key_attribute": true,
                "index_fields": "nid",
                "index_name": "pk",
                "index_type": "PRIMARYKEY64"
            }
        ],
        "table_name": "test"
    })";
    auto tabletSchema = std::make_shared<indexlibv2::config::TabletSchema>();
    assert(tabletSchema->Deserialize(schemaStr));
    auto fieldConfig = tabletSchema->GetFieldConfig("uvsum");
    auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        return nullptr;
    }
    auto ret = tabletSchema->_impl->AddIndexConfig(attrConfig);
    if (!ret.IsOK()) {
        return nullptr;
    }
    return tabletSchema;
}

TEST_F(EvaluatorCreatorTest, TestSimple)
{
    // "distinct_field": "uvsum",
    std::string str = R"({
        "filter_field" : "DOC_PAYLOAD",
        "distinct_count": 20000,
        "distinct_expand_limit": 120000
    })";
    std::shared_ptr<indexlibv2::config::DiversityConstrain> diversityConstrain = GetDiversityConstrain(str);

    auto tabletSchema = GetDefaultSchema();
    ASSERT_TRUE(tabletSchema != nullptr);

    auto mergeInfos = TruncateIndexUtil::GetSegmentMergeInfos("uvsum");
    auto creator = std::make_shared<TruncateAttributeReaderCreator>(tabletSchema, mergeInfos, /*docMapper*/ nullptr);

    indexlibv2::config::TruncateProfile profile;
    auto allocator = std::make_shared<DocInfoAllocator>();
    [[maybe_unused]] auto refer1 = allocator->DeclareReference("uvsum", ft_int32, /*isNull*/ false);
    [[maybe_unused]] auto refer2 = allocator->DeclareReference("DOC_PAYLOAD", ft_int32, /*isNull*/ false);
    // auto evaluator = EvaluatorCreator::Create(profile, *diversityConstrain, creator.get(), allocator);

    // auto multiAttrEvaluator = std::dynamic_pointer_cast<MultiAttributeEvaluator>(evaluator);
    // ASSERT_TRUE(multiAttrEvaluator != nullptr);
    // ASSERT_EQ(1, multiAttrEvaluator->_evaluators.size());

    // evaluator = multiAttrEvaluator->_evaluators[0];
    // auto docPayloadEvaluator = std::dynamic_pointer_cast<DocPayloadEvaluator>(evaluator);
    // ASSERT_TRUE(docPayloadEvaluator != nullptr);

    // evaluator = multiAttrEvaluator->_evaluators[1];
    // auto attrEvaluator = std::dynamic_pointer_cast<AttributeEvaluator<int32_t>>(evaluator);
    // ASSERT_TRUE(attrEvaluator != nullptr);
}

} // namespace indexlib::index

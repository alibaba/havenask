#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class TruncateAttributeReaderCreatorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<indexlibv2::config::ITabletSchema> GetDefaultSchema() const;
    // return 2 src-segments, one empty segment(doc count = 0), another is valid segment
    indexlibv2::index::IIndexMerger::SegmentMergeInfos GetSegmentMergeInfos() const;
};

std::shared_ptr<indexlibv2::config::ITabletSchema> TruncateAttributeReaderCreatorTest::GetDefaultSchema() const
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

indexlibv2::index::IIndexMerger::SegmentMergeInfos TruncateAttributeReaderCreatorTest::GetSegmentMergeInfos() const
{
    auto createSegment = []() {
        auto segmentInfo = std::make_shared<indexlibv2::framework::SegmentInfo>();
        segmentInfo->docCount = 100;

        indexlibv2::framework::SegmentMeta meta;
        meta.segmentInfo = segmentInfo;

        auto segment = std::make_shared<indexlibv2::framework::FakeSegment>(
            indexlibv2::framework::Segment::SegmentStatus::ST_BUILT);
        segment->_segmentMeta = std::move(meta);
        return segment;
    };

    indexlibv2::index::IIndexMerger::SegmentMergeInfos mergeInfos;
    auto emptySegment = createSegment();
    emptySegment->_segmentMeta.segmentInfo->docCount = 0;
    mergeInfos.srcSegments.push_back({/*baseDocId*/ 0, std::move(emptySegment)});

    auto segment = createSegment();
    mergeInfos.srcSegments.push_back({/*baseDocId*/ 0, std::move(segment)});
    return mergeInfos;
}

TEST_F(TruncateAttributeReaderCreatorTest, TestCreateAttributeReaderAbnormal)
{
    auto tabletSchema = GetDefaultSchema();
    ASSERT_TRUE(tabletSchema != nullptr);

    auto mergeInfos = GetSegmentMergeInfos();
    auto creator = std::make_shared<TruncateAttributeReaderCreator>(tabletSchema, mergeInfos, /*docMapper*/ nullptr);
    // nid not an attribute
    ASSERT_EQ(nullptr, creator->CreateAttributeReader("nid"));
    // get indexer from segment failed
    ASSERT_EQ(nullptr, creator->CreateAttributeReader("uvsum"));
}

TEST_F(TruncateAttributeReaderCreatorTest, TestCreateAlreadyExist)
{
    auto tabletSchema = GetDefaultSchema();
    ASSERT_TRUE(tabletSchema != nullptr);

    auto mergeInfos = GetSegmentMergeInfos();
    auto creator = std::make_shared<TruncateAttributeReaderCreator>(tabletSchema, mergeInfos, /*docMapper*/ nullptr);
    creator->_truncateAttributeReaders["nid"] =
        std::make_shared<TruncateAttributeReader>(/*docMapper*/ nullptr, /*indexConfig*/ nullptr);

    ASSERT_NE(nullptr, creator->Create("nid"));
}

} // namespace indexlib::index

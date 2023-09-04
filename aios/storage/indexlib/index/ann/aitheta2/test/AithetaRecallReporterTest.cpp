#include "indexlib/index/ann/aitheta2/AithetaRecallReporter.h"

#include "indexlib/util/testutil/unittest.h"

namespace indexlibv2::index::ann {
class AithetaRecallReporterTest : public TESTBASE
{
public:
    AithetaRecallReporterTest() {}
    ~AithetaRecallReporterTest() {}
};

TEST_F(AithetaRecallReporterTest, TestInit)
{
    {
        AithetaIndexConfig aithetaConfig;
        aithetaConfig.recallConfig.enable = false;
        AithetaRecallReporter recallReporter(aithetaConfig, nullptr, nullptr);
        ASSERT_FALSE(recallReporter.Init().IsOK());
    }
    {
        AithetaIndexConfig aithetaConfig;
        aithetaConfig.recallConfig.sampleRatio = 1.1;
        AithetaRecallReporter recallReporter(aithetaConfig, nullptr, nullptr);
        ASSERT_FALSE(recallReporter.Init().IsOK());
    }
}

TEST_F(AithetaRecallReporterTest, TestGetTag) {
    AithetaIndexConfig aithetaConfig;
    auto metricReporter = std::make_shared<MetricReporter>(nullptr, "embedding_index");
    AithetaRecallReporter recallReporter(aithetaConfig, metricReporter, nullptr);

    {
        AithetaQueries aithetaQueries;
        auto kmonTags = recallReporter.GetKmonTags(aithetaQueries);
        auto tagsMap = kmonTags->GetTagsMap();
        ASSERT_EQ(2, tagsMap.size());
        ASSERT_EQ("embedding_index", tagsMap["index"]);
        ASSERT_EQ("aitheta2", tagsMap["index_type"]);
    }
    {
        AithetaQueries aithetaQueries;
        (*aithetaQueries.mutable_querytags())["biz_name"] = "biz1";
        (*aithetaQueries.mutable_querytags())["src"] = "src1";
        ASSERT_TRUE(recallReporter._kmonTagMap.find("biz1") == recallReporter._kmonTagMap.end());
        auto kmonTags = recallReporter.GetKmonTags(aithetaQueries);
        auto tagsMap = kmonTags->GetTagsMap();
        ASSERT_EQ(4, tagsMap.size());
        ASSERT_EQ("biz1", tagsMap["biz_name"]);
        ASSERT_EQ("src1", tagsMap["src"]);
        ASSERT_EQ("embedding_index", tagsMap["index"]);
        ASSERT_EQ("aitheta2", tagsMap["index_type"]);

        auto tags = metricReporter->getTags()->GetTagsMap();
        ASSERT_EQ(2, tags.size());
        ASSERT_EQ("embedding_index", tags["index"]);
        ASSERT_EQ("aitheta2", tags["index_type"]);
    }
}

} // namespace indexlibv2::index::ann
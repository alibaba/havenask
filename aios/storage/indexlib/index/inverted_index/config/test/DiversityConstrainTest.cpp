#include "indexlib/index/inverted_index/config/DiversityConstrain.h"

#include <string>

#include "indexlib/file_system/JsonUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class DiversityConstrainTest : public TESTBASE
{
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<DiversityConstrain> GetDiversityConstrain(std::string& content) const;
};

std::shared_ptr<DiversityConstrain> DiversityConstrainTest::GetDiversityConstrain(std::string& content) const
{
    std::shared_ptr<DiversityConstrain> constrain = std::make_shared<DiversityConstrain>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, constrain.get()).Status();
    assert(ec.IsOK());
    return constrain;
}

TEST_F(DiversityConstrainTest, TestSimpleProcess)
{
    std::string str = R"({
        "distinct_field": "user_id",
        "distinct_count": 20000,
        "distinct_expand_limit": 120000
    })";
    std::shared_ptr<DiversityConstrain> diversityConstrain = GetDiversityConstrain(str);
    ASSERT_EQ("user_id", diversityConstrain->GetDistField());
    ASSERT_EQ(20000, diversityConstrain->GetDistCount());
    ASSERT_EQ(120000, diversityConstrain->GetDistExpandLimit());
}

TEST_F(DiversityConstrainTest, TestInit)
{
    {
        std::shared_ptr<DiversityConstrain> diversityConstrain = std::make_shared<DiversityConstrain>();
        diversityConstrain->TEST_SetFilterType("Default");
        diversityConstrain->TEST_SetFilterParameter(
            "fieldName=ends_1;filterMask=0xFFFF;filterMinValue=1;filterMaxValue=100");
        diversityConstrain->Init();
        ASSERT_EQ("ends_1", diversityConstrain->GetFilterField());
        ASSERT_EQ((uint64_t)0xFFFF, diversityConstrain->GetFilterMask());
        ASSERT_EQ(1, diversityConstrain->GetFilterMinValue());
        ASSERT_EQ(100, diversityConstrain->GetFilterMaxValue());
    }
    {
        std::shared_ptr<DiversityConstrain> diversityConstrain = std::make_shared<DiversityConstrain>();
        diversityConstrain->TEST_SetFilterType("FilterByMeta");
        diversityConstrain->TEST_SetFilterParameter("fieldName=ends_1");
        diversityConstrain->Init();
        ASSERT_EQ("ends_1", diversityConstrain->GetFilterField());
        ASSERT_EQ("FilterByMeta", diversityConstrain->GetFilterType());
        ASSERT_EQ(false, diversityConstrain->IsFilterByTimeStamp());
        ASSERT_EQ(true, diversityConstrain->IsFilterByMeta());
    }
}

TEST_F(DiversityConstrainTest, TestCheck)
{
    // check distinct
    {
        std::string str = R"({
            "distinct_field": "user_id",
            "distinct_count": 120000,
            "distinct_expand_limit": 20000
        })";
        std::shared_ptr<DiversityConstrain> diversityConstrain = GetDiversityConstrain(str);
        ASSERT_EQ("user_id", diversityConstrain->GetDistField());
        ASSERT_EQ(120000, diversityConstrain->GetDistCount());
        ASSERT_EQ(20000, diversityConstrain->GetDistExpandLimit());
        ASSERT_FALSE(diversityConstrain->Check());
    }
    // check filter
    {
        std::string str = R"({
            "filter_field": "user_id",
            "filter_max_value": 20000,
            "filter_min_value": 120000
        })";
        std::shared_ptr<DiversityConstrain> diversityConstrain = GetDiversityConstrain(str);
        ASSERT_EQ("user_id", diversityConstrain->GetFilterField());
        ASSERT_EQ(20000, diversityConstrain->GetFilterMaxValue());
        ASSERT_EQ(120000, diversityConstrain->GetFilterMinValue());
        ASSERT_FALSE(diversityConstrain->Check());
    }
}

TEST_F(DiversityConstrainTest, TestParseTimeStampFilter)
{
    {
        std::shared_ptr<DiversityConstrain> diversityConstrain = std::make_shared<DiversityConstrain>();
        diversityConstrain->TEST_SetFilterType("FilterByTimeStamp");
        diversityConstrain->TEST_SetFilterParameter("fieldName=ends_1;beginTime=18:00:00;adviseEndTime=23:00:00");
        // begin time -> 3:0:0
        ASSERT_TRUE(diversityConstrain->ParseTimeStampFilter(10800, 0));
        ASSERT_EQ(64800, diversityConstrain->GetFilterMinValue());
        ASSERT_EQ(82800, diversityConstrain->GetFilterMaxValue());
        ASSERT_EQ("ends_1", diversityConstrain->GetFilterField());
        ASSERT_EQ("FilterByTimeStamp", diversityConstrain->GetFilterType());
        ASSERT_TRUE(diversityConstrain->IsFilterByTimeStamp());
        ASSERT_FALSE(diversityConstrain->IsFilterByMeta());
    }
    {
        std::shared_ptr<DiversityConstrain> diversityConstrain = std::make_shared<DiversityConstrain>();
        diversityConstrain->TEST_SetFilterType("FilterByTimeStamp");
        diversityConstrain->TEST_SetFilterParameter(
            "fieldName=ends_1;beginTime=now;adviseEndTime=10:00:00;minInvertal=6");
        // begin time -> 3:0:0
        ASSERT_TRUE(diversityConstrain->ParseTimeStampFilter(10800, 0));
        ASSERT_EQ(10800, diversityConstrain->GetFilterMinValue());
        ASSERT_EQ(36000, diversityConstrain->GetFilterMaxValue());
        ASSERT_EQ("ends_1", diversityConstrain->GetFilterField());
        ASSERT_EQ("FilterByTimeStamp", diversityConstrain->GetFilterType());
        ASSERT_TRUE(diversityConstrain->IsFilterByTimeStamp());
        ASSERT_FALSE(diversityConstrain->IsFilterByMeta());
    }
}

} // namespace indexlibv2::config
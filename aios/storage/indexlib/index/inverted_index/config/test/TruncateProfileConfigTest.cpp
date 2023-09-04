#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"

#include <string>

#include "indexlib/config/SortDescription.h"
#include "indexlib/file_system/JsonUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class TruncateProfileConfigTest : public TESTBASE
{
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<TruncateProfileConfig> GetTruncateProfileConfig(std::string& content) const;
};

std::shared_ptr<TruncateProfileConfig> TruncateProfileConfigTest::GetTruncateProfileConfig(std::string& content) const
{
    std::shared_ptr<TruncateProfileConfig> profile = std::make_shared<TruncateProfileConfig>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, profile.get()).Status();
    assert(ec.IsOK());
    return profile;
}

TEST_F(TruncateProfileConfigTest, TestSimpleProcess)
{
    std::string str = R"({
        "sort_descriptions": "-uvsum;+nid",
        "truncate_profile_name": "desc_uvsum"
    })";
    std::shared_ptr<TruncateProfileConfig> truncateProfileConfig = GetTruncateProfileConfig(str);
    ASSERT_EQ("desc_uvsum", truncateProfileConfig->GetTruncateProfileName());
    auto sortParams = truncateProfileConfig->GetTruncateSortParams();
    ASSERT_EQ("uvsum", sortParams[0].GetSortField());
    ASSERT_EQ(indexlibv2::config::sp_desc, sortParams[0].GetSortPattern());
    ASSERT_EQ("nid", sortParams[1].GetSortField());
    ASSERT_EQ(indexlibv2::config::sp_asc, sortParams[1].GetSortPattern());
}

} // namespace indexlibv2::config

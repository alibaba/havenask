#include "indexlib/config/SortDescription.h"

#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class SortDescriptionTest : public TESTBASE
{
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SortDescriptionTest, TestTransForm)
{
    ASSERT_EQ(config::sp_desc, SortDescription::SortPatternFromString(""));
    ASSERT_EQ(config::sp_desc, SortDescription::SortPatternFromString("aaa"));

    ASSERT_EQ(config::sp_asc, SortDescription::SortPatternFromString("asc"));
    ASSERT_EQ(config::sp_asc, SortDescription::SortPatternFromString("ASC"));
    ASSERT_EQ(config::sp_asc, SortDescription::SortPatternFromString("Asc"));

    ASSERT_EQ(config::sp_desc, SortDescription::SortPatternFromString("desc"));
    ASSERT_EQ(config::sp_desc, SortDescription::SortPatternFromString("DESC"));
    ASSERT_EQ(config::sp_desc, SortDescription::SortPatternFromString("deSc"));
}

}} // namespace indexlibv2::config

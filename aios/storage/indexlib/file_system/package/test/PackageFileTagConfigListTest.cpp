#include "indexlib/file_system/package/PackageFileTagConfigList.h"

#include "indexlib/util/testutil/unittest.h"

namespace indexlib ::file_system {

class PackageFileTagConfigListTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PackageFileTagConfigListTest);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, PackageFileTagConfigListTest);

TEST_F(PackageFileTagConfigListTest, TestCopyConstructor)
{
    // PackageFileTagConfig has default configs with patterns, refer to its init() function for details.
    std::string jsonStr = R"(
    {
        "package_file_tag_config":
        [
            {"file_patterns": ["_PATCH_"], "tag": "PATCH" }
        ]        
    }
    )";
    PackageFileTagConfigList list1;
    autil::legacy::FromJsonString(list1, jsonStr);
    EXPECT_EQ(list1.Match("index/index_name/123_2.patch", /*defaultTag=*/"default"), "default");
    EXPECT_EQ(list1.Match("__PATCH__", /*defaultTag=*/"default"), "default");
    EXPECT_EQ(list1.Match("segment_123/index/index_name/321_1.patch", /*defaultTag=*/"default"), "PATCH");
    EXPECT_EQ(list1.Match("segment_0_level_0/index/index_name/321_1.patch", /*defaultTag=*/"default"), "PATCH");
    EXPECT_EQ(list1.Match("segment_0_level_0/index/index_name/123_2.random", /*defaultTag=*/"default"), "default");

    PackageFileTagConfigList list2(list1);
    EXPECT_EQ(list2.Match("index/index_name/123_2.patch", /*defaultTag=*/"default"), "default");
    EXPECT_EQ(list2.Match("__PATCH__", /*defaultTag=*/"default"), "default");
    EXPECT_EQ(list2.Match("segment_123/index/index_name/321_1.patch", /*defaultTag=*/"default"), "PATCH");
    EXPECT_EQ(list2.Match("segment_0_level_0/index/index_name/321_1.patch", /*defaultTag=*/"default"), "PATCH");
    EXPECT_EQ(list2.Match("segment_0_level_0/index/index_name/123_2.random", /*defaultTag=*/"default"), "default");
}

} // namespace indexlib::file_system

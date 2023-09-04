#include "indexlib/base/PathUtil.h"

#include "autil/Log.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "unittest/unittest.h"

namespace indexlibv2 {

class PathUtilTest : public TESTBASE
{
};

TEST_F(PathUtilTest, IsValidSegmentDirNameTest)
{
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("_segment_0"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("_segment_0_level"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("_segment_01_level"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("_segment_0_level_0"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("_segment_0"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("_segment_0"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("_segment_0"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("segment0_level_0_"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("segment_0level_0_"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("segment_0level_0"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("segment_0_level0"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("segment_0_level0_"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("segment_0_level_0_"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("segment_0_levl_0_"));
    ASSERT_FALSE(PathUtil::IsValidSegmentDirName("segment1_0_levl_0_"));

    ASSERT_TRUE(PathUtil::IsValidSegmentDirName("segment_0_level_0"));
    ASSERT_TRUE(PathUtil::IsValidSegmentDirName("segment_1_level_1"));
    ASSERT_TRUE(PathUtil::IsValidSegmentDirName("segment_1_level_10"));
    ASSERT_TRUE(PathUtil::IsValidSegmentDirName("segment_100_level_10"));
}

TEST_F(PathUtilTest, SegmentNameTest)
{
    ASSERT_EQ(INVALID_SEGMENTID, PathUtil::GetSegmentIdByDirName("segment"));
    ASSERT_EQ(INVALID_SEGMENTID, PathUtil::GetSegmentIdByDirName("segmen"));
    ASSERT_EQ(INVALID_SEGMENTID, PathUtil::GetSegmentIdByDirName("segment_"));
    ASSERT_EQ(0, PathUtil::GetSegmentIdByDirName("segment_0"));
    ASSERT_EQ(1, PathUtil::GetSegmentIdByDirName("segment_1"));
    ASSERT_EQ(0, PathUtil::GetSegmentIdByDirName("segment_00"));
    ASSERT_EQ(1, PathUtil::GetSegmentIdByDirName("segment_01"));
    ASSERT_EQ(0, PathUtil::GetSegmentIdByDirName("segment_0_"));
    ASSERT_EQ(1, PathUtil::GetSegmentIdByDirName("segment_1_"));
    ASSERT_EQ(0, PathUtil::GetSegmentIdByDirName("segment_0_le"));
    ASSERT_EQ(1, PathUtil::GetSegmentIdByDirName("segment_1_lve"));
}

TEST_F(PathUtilTest, TestExtractFenceName)
{
    auto [root, fence] = PathUtil::ExtractFenceName("/table/generation_0/partition_0_65535/__FENCE__1655191761088304");
    ASSERT_EQ("/table/generation_0/partition_0_65535", root);
    ASSERT_EQ("__FENCE__1655191761088304", fence);
    std::tie(root, fence) = PathUtil::ExtractFenceName("/table/generation_0/partition_0_65535");
    ASSERT_EQ("/table/generation_0/partition_0_65535", root) << "no fence";
    ASSERT_EQ("", fence);
    std::tie(root, fence) =
        PathUtil::ExtractFenceName("/table/generation_0/partition_0_65535/__FENCE__1655191761088304/segment_0_level_0");
    ASSERT_EQ("/table/generation_0/partition_0_65535/__FENCE__1655191761088304/segment_0_level_0", root)
        << "undeder fence";
    ASSERT_EQ("", fence);
    std::tie(root, fence) = PathUtil::ExtractFenceName("/table/generation_0/partition_0_65535/__FENCE_");
    ASSERT_EQ("/table/generation_0/partition_0_65535/__FENCE_", root) << "no fence";
    ASSERT_EQ("", fence);
}

TEST_F(PathUtilTest, GetFileNameFromPath)
{
    EXPECT_EQ("xyz.1", PathUtil::GetFileNameFromPath("abc/def/xyz.1"));
    EXPECT_EQ("xyz.2", PathUtil::GetFileNameFromPath("abc/xyz.2"));
    EXPECT_EQ("xyz.3", PathUtil::GetFileNameFromPath("xyz.3"));
    EXPECT_EQ("xyz", PathUtil::GetFileNameFromPath("xyz"));
    EXPECT_EQ("xyz.4", PathUtil::GetFileNameFromPath("/absolute/abc/def/xyz.4"));
    EXPECT_EQ("xyz", PathUtil::GetFileNameFromPath("//absolute/abc/def/xyz"));
    EXPECT_EQ("xyz", PathUtil::GetFileNameFromPath("://absolute/abc/def/xyz"));
}

TEST_F(PathUtilTest, GetDirName)
{
    EXPECT_EQ("abc/def", PathUtil::GetDirName("abc/def/xyz.1"));
    EXPECT_EQ("abc", PathUtil::GetDirName("abc/xyz.2"));
    EXPECT_EQ("", PathUtil::GetDirName("xyz.3"));
    EXPECT_EQ("", PathUtil::GetDirName("xyz"));
    EXPECT_EQ("", PathUtil::GetDirName("/"));
    EXPECT_EQ("/absolute/abc/def", PathUtil::GetDirName("/absolute/abc/def/xyz.4"));
    EXPECT_EQ("//absolute/abc/def", PathUtil::GetDirName("//absolute/abc/def/xyz"));
    EXPECT_EQ("://absolute/abc/def", PathUtil::GetDirName("://absolute/abc/def/xyz"));
}

TEST_F(PathUtilTest, GetFirstLevelDirName)
{
    EXPECT_EQ("abc", PathUtil::GetFirstLevelDirName("abc/def/xyz.1"));
    EXPECT_EQ("abc", PathUtil::GetFirstLevelDirName("abc/xyz.2"));
    EXPECT_EQ("", PathUtil::GetFirstLevelDirName("xyz.3"));
    EXPECT_EQ("", PathUtil::GetFirstLevelDirName("xyz"));
    EXPECT_EQ("", PathUtil::GetFirstLevelDirName("/"));
    EXPECT_EQ("", PathUtil::GetFirstLevelDirName("/absolute/abc/def/xyz.4"));
    EXPECT_EQ("", PathUtil::GetFirstLevelDirName("//absolute/abc/def/xyz"));
    EXPECT_EQ("", PathUtil::GetFirstLevelDirName("://absolute/abc/def/xyz"));
}

} // namespace indexlibv2

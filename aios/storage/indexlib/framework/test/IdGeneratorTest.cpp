#include "indexlib/framework/IdGenerator.h"

#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace framework {

class IdGeneratorTest : public TESTBASE
{
public:
    IdGeneratorTest() = default;
    ~IdGeneratorTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(IdGeneratorTest, testSimple)
{
    {
        IdGenerator mgr(IdMaskType::BUILD_PUBLIC);
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870912);
        mgr.CommitNextSegmentId();
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870913);
    }
    {
        IdGenerator mgr(IdMaskType::BUILD_PRIVATE);
        ASSERT_EQ(mgr.GetNextSegmentId(), 1073741824);
        mgr.CommitNextSegmentId();
        ASSERT_EQ(mgr.GetNextSegmentId(), 1073741825);
    }
}

TEST_F(IdGeneratorTest, testWithBaseVersion)
{
    {
        IdGenerator mgr(IdMaskType::BUILD_PUBLIC);
        Version version(/*versionId=*/0);
        version.AddSegment(536870914);
        mgr.UpdateBaseVersion(version);
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870915);
        mgr.CommitNextSegmentId();
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870916);
        mgr.CommitNextSegmentId();
        version.AddSegment(536870915);
        mgr.UpdateBaseVersion(version);
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870917);
    }
}

TEST_F(IdGeneratorTest, testUpdateBasedVersionId)
{
    {
        IdGenerator mgr(IdMaskType::BUILD_PUBLIC);
        ASSERT_EQ(mgr.GenerateVersionId(), 536870912);
        ASSERT_EQ(mgr.GenerateVersionId(), 536870913);
        ASSERT_FALSE(mgr.UpdateBaseVersionId(536870912).IsOK());
        ASSERT_FALSE(mgr.UpdateBaseVersionId(536870913).IsOK());
        ASSERT_TRUE(mgr.UpdateBaseVersionId(536870914).IsOK());
        ASSERT_EQ(mgr.GenerateVersionId(), 536870915);
        ASSERT_FALSE(mgr.UpdateBaseVersionId(0).IsOK());
    }
}

}} // namespace indexlibv2::framework

#include "build_service/util/ParallelIdGenerator.h"

#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;
using namespace indexlibv2;
using namespace indexlibv2::framework;

namespace build_service { namespace util {

class ParallelIdGeneratorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void ParallelIdGeneratorTest::setUp() {}

void ParallelIdGeneratorTest::tearDown() {}

TEST_F(ParallelIdGeneratorTest, testInvalidArgs)
{
    ParallelIdGenerator mgr(IdMaskType::BUILD_PUBLIC, IdMaskType::BUILD_PRIVATE);
    ASSERT_FALSE(mgr.Init(/*instanceId=*/4, /*parallelNum=*/3).IsOK());
    ASSERT_FALSE(mgr.Init(/*instanceId=*/-2, /*parallelNum=*/-1).IsOK());
}

TEST_F(ParallelIdGeneratorTest, testSimple)
{
    {
        ParallelIdGenerator mgr(IdMaskType::BUILD_PUBLIC, IdMaskType::BUILD_PRIVATE);
        ASSERT_TRUE(mgr.Init(/*instanceId=*/1, /*parallelNum=*/3).IsOK());
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870913);
        ASSERT_EQ(mgr.GenerateVersionId(), 1073741825);
        mgr.CommitNextSegmentId();
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870916);
        ASSERT_EQ(mgr.GenerateVersionId(), 1073741828);
    }
    {
        ParallelIdGenerator mgr(IdMaskType::BUILD_PRIVATE, IdMaskType::BUILD_PRIVATE);
        ASSERT_TRUE(mgr.Init(/*instanceId=*/0, /*parallelNum=*/1).IsOK());
        ASSERT_EQ(mgr.GetNextSegmentId(), 1073741824);
        ASSERT_EQ(mgr.GenerateVersionId(), 1073741824);
        mgr.CommitNextSegmentId();
        ASSERT_EQ(mgr.GetNextSegmentId(), 1073741825);
        ASSERT_EQ(mgr.GenerateVersionId(), 1073741825);
    }
}

TEST_F(ParallelIdGeneratorTest, testWithBaseVersion)
{
    {
        ParallelIdGenerator mgr(IdMaskType::BUILD_PUBLIC, IdMaskType::BUILD_PRIVATE);
        ASSERT_TRUE(mgr.Init(/*instanceId=*/1, /*parallelNum=*/3).IsOK());
        Version version(/*versionId=*/1073741824);
        version.AddSegment(536870914);
        mgr.UpdateBaseVersion(version);
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870916);
        ASSERT_EQ(mgr.GenerateVersionId(), 1073741825);
        mgr.CommitNextSegmentId();
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870919);
        mgr.CommitNextSegmentId();
        version.AddSegment(536870918);
        mgr.UpdateBaseVersion(version);
        ASSERT_EQ(mgr.GetNextSegmentId(), 536870922);
        ASSERT_EQ(mgr.GenerateVersionId(), 1073741828);
    }
}

TEST_F(ParallelIdGeneratorTest, testCalculateInstanceId)
{
    {
        ParallelIdGenerator mgr(IdMaskType::BUILD_PUBLIC, IdMaskType::BUILD_PRIVATE);
        ASSERT_TRUE(mgr.Init(/*instanceId=*/1, /*parallelNum=*/3).IsOK());
        ASSERT_EQ(mgr.CalculateInstanceIdBySegId(536870916), 1);
        ASSERT_EQ(mgr.CalculateInstanceIdBySegId(536870917), 2);
        ASSERT_EQ(mgr.CalculateInstanceIdBySegId(536870918), 0);
        ASSERT_EQ(mgr.CalculateInstanceIdBySegId(1073741825), -1);
        ASSERT_EQ(mgr.CalculateInstanceIdByVersionId(1073741825), 1);
    }
}

}} // namespace build_service::util

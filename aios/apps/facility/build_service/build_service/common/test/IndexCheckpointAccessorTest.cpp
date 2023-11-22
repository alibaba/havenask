#include "build_service/common/IndexCheckpointAccessor.h"

#include <iosfwd>

#include "build_service/common/CheckpointAccessor.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace common {

class IndexCheckpointAccessorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void IndexCheckpointAccessorTest::setUp() {}

void IndexCheckpointAccessorTest::tearDown() {}

TEST_F(IndexCheckpointAccessorTest, testSetIndexInfoVisibility)
{
    CheckpointAccessorPtr ckptAccessorPtr(new CheckpointAccessor);
    IndexCheckpointAccessor accessor(ckptAccessorPtr);
    ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos1, indexInfos2, indexInfos;
    proto::IndexInfo indexInfo1, indexInfo2;
    indexInfo1.set_clustername("cluster1");
    *indexInfos1.Add() = indexInfo1;
    indexInfo2.set_clustername("cluster2");
    *indexInfos2.Add() = indexInfo2;
    accessor.updateIndexInfo(false, "cluster1", indexInfos1);
    accessor.updateIndexInfo(false, "cluster2", indexInfos1);
    ASSERT_TRUE(accessor.getIndexInfo(false, "cluster1", indexInfos));
    ASSERT_TRUE(accessor.getIndexInfo(false, "cluster2", indexInfos));
    accessor.setIndexVisiable("cluster1", false);
    ASSERT_FALSE(accessor.getIndexInfo(false, "cluster1", indexInfos));
    ASSERT_TRUE(accessor.getIndexInfo(false, "cluster2", indexInfos));
    accessor.setIndexVisiable("cluster1", true);
    ASSERT_TRUE(accessor.getIndexInfo(false, "cluster1", indexInfos));
    ASSERT_TRUE(accessor.getIndexInfo(false, "cluster2", indexInfos));
}

}} // namespace build_service::common

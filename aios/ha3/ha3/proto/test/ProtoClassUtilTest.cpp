#include "ha3/proto/ProtoClassUtil.h"

#include <iosfwd>
#include <string>

#include "ha3/proto/BasicDefs.pb.h"
#include "unittest/unittest.h"

using namespace std;
namespace isearch {
namespace proto {

class ProtoClassUtilTest : public TESTBASE {
public:
    ProtoClassUtilTest();
    ~ProtoClassUtilTest();

public:
    void setUp();
    void tearDown();

protected:
};

ProtoClassUtilTest::ProtoClassUtilTest() {}

ProtoClassUtilTest::~ProtoClassUtilTest() {}

void ProtoClassUtilTest::setUp() {}

void ProtoClassUtilTest::tearDown() {}

TEST_F(ProtoClassUtilTest, testPartitionIdToString) {

    PartitionID pid = ProtoClassUtil::createPartitionID("daogou", ROLE_SEARCHER, 0, 999, 17, 79);

    ASSERT_EQ(string("ROLE_SEARCHER_daogou_79_17_0_999"), ProtoClassUtil::partitionIdToString(pid));

    ASSERT_EQ(string("ROLE_QRS__0_0_0_65535"),
              ProtoClassUtil::partitionIdToString(ProtoClassUtil::getQrsPartitionID()));
}

TEST_F(ProtoClassUtilTest, testSimplifyPartitionIdStr) {

    string str = "ROLE_SEARCHER_daogou_79_17_0_999";
    ASSERT_EQ(string("SEARCHER_daogou_79_17_0_999"), ProtoClassUtil::simplifyPartitionIdStr(str));

    str = "SEARCHER_daogou_79_17_0_999";
    ASSERT_EQ(string("SEARCHER_daogou_79_17_0_999"), ProtoClassUtil::simplifyPartitionIdStr(str));
}

} // namespace proto
} // namespace isearch

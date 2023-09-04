#include "unittest/unittest.h"
#include "navi/engine/PartInfo.h"

using namespace std;
using namespace testing;

namespace navi {

class PartInfoTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void PartInfoTest::setUp() {
}

void PartInfoTest::tearDown() {
}

TEST_F(PartInfoTest, testSimple) {
    PartInfoDef partInfoDef;
    partInfoDef.set_part_count(5);
    partInfoDef.add_part_ids(0);
    partInfoDef.add_part_ids(2);
    partInfoDef.add_part_ids(4);

    PartInfo partInfo;
    partInfo.init(partInfoDef.part_count(), partInfoDef);

    ASSERT_TRUE(partInfo.isUsed(0));
    ASSERT_FALSE(partInfo.isUsed(1));
    ASSERT_TRUE(partInfo.isUsed(2));
    ASSERT_FALSE(partInfo.isUsed(3));
    ASSERT_TRUE(partInfo.isUsed(4));

    size_t i = 0;
    for (auto partId : partInfo) {
        ASSERT_EQ(partId, partInfoDef.part_ids(i++));
    }
}

TEST_F(PartInfoTest, testFillDef) {
    PartInfoDef partInfoDef;
    partInfoDef.set_part_count(5);
    partInfoDef.add_part_ids(0);
    partInfoDef.add_part_ids(2);
    partInfoDef.add_part_ids(4);

    PartInfo partInfo;
    partInfo.init(partInfoDef.part_count(), partInfoDef);

    ASSERT_TRUE(partInfo.isUsed(0));
    ASSERT_FALSE(partInfo.isUsed(1));
    ASSERT_TRUE(partInfo.isUsed(2));
    ASSERT_FALSE(partInfo.isUsed(3));
    ASSERT_TRUE(partInfo.isUsed(4));

    size_t i = 0;
    for (auto partId : partInfo) {
        ASSERT_EQ(partId, partInfoDef.part_ids(i++));
    }

    PartInfoDef partInfoDef2;
    partInfo.fillDef(partInfoDef2);
    ASSERT_EQ(partInfoDef.ShortDebugString(), partInfoDef2.ShortDebugString());
}

TEST_F(PartInfoTest, testGetUsedPartId) {
    PartInfoDef partInfoDef;
    partInfoDef.set_part_count(5);
    partInfoDef.add_part_ids(0);
    partInfoDef.add_part_ids(2);
    partInfoDef.add_part_ids(4);

    PartInfo partInfo;
    partInfo.init(partInfoDef.part_count(), partInfoDef);

    ASSERT_EQ(0 ,partInfo.getUsedPartId(0));
    ASSERT_EQ(2 ,partInfo.getUsedPartId(1));
    ASSERT_EQ(4 ,partInfo.getUsedPartId(2));
}

TEST_F(PartInfoTest, testGetUsedPartIndex) {
    PartInfoDef partInfoDef;
    partInfoDef.set_part_count(5);
    partInfoDef.add_part_ids(0);
    partInfoDef.add_part_ids(2);
    partInfoDef.add_part_ids(4);

    PartInfo partInfo;
    partInfo.init(partInfoDef.part_count(), partInfoDef);

    EXPECT_EQ(0 ,partInfo.getUsedPartIndex(0));
    EXPECT_EQ(-1 ,partInfo.getUsedPartIndex(1));
    EXPECT_EQ(1 ,partInfo.getUsedPartIndex(2));
    EXPECT_EQ(-1 ,partInfo.getUsedPartIndex(3));
    EXPECT_EQ(2 ,partInfo.getUsedPartIndex(4));
    EXPECT_EQ(-1 ,partInfo.getUsedPartIndex(5));
}

TEST_F(PartInfoTest, testIterator) {
    // simple iterator
    {
        PartInfoDef partInfoDef;
        partInfoDef.set_part_count(1000);
        for (size_t i = 0; i < 10 ; ++i) {
            partInfoDef.add_part_ids(i);
        }
        partInfoDef.add_part_ids(999);

        PartInfo partInfo;
        partInfo.init(partInfoDef.part_count(), partInfoDef);

        size_t i = 0;
        for (auto partId : partInfo) {
            ASSERT_EQ(partId, partInfoDef.part_ids(i++));
        }
    }

    // empty iterator
    {
        PartInfo partInfo;
        partInfo._ids.init(0);
        for (auto partId : partInfo) {
            ASSERT_FALSE(true);
            ASSERT_EQ(-1, partId);
        }
    }

    // used ids empty
    {
        PartInfo partInfo;
        partInfo._ids.init(2);
        for (auto partId : partInfo) {
            ASSERT_FALSE(true);
            ASSERT_EQ(-1, partId);
        }
    }
}



}

#include "suez/sdk/PartitionId.h"

#include "unittest/unittest.h"

namespace suez {

class PartitionIdTest : public TESTBASE {
protected:
    void makePid(
        PartitionId &pid, const std::string &name, int32_t fullVersion, int32_t from, int32_t to, int32_t index = -1) {
        pid.from = from;
        pid.to = to;
        pid.index = index;
        pid.tableId.tableName = name;
        pid.tableId.fullVersion = fullVersion;
    }
};

TEST_F(PartitionIdTest, testConstructor) {
    PartitionId pid;
    ASSERT_EQ(-1, pid.from);
    ASSERT_EQ(-1, pid.to);
    ASSERT_EQ(-1, pid.index);
    ASSERT_EQ(-1, pid.tableId.fullVersion);
    ASSERT_TRUE(pid.tableId.tableName.empty());
}

TEST_F(PartitionIdTest, testSetPartitionIndex) {
    {
        PartitionId pid;
        makePid(pid, "t", 0, 0, 65535);
        pid.setPartitionIndex(1);
        ASSERT_EQ(0, pid.index);
    }
    {
        PartitionId pid;
        makePid(pid, "t", 0, 0, 32767);
        pid.setPartitionIndex(1);
        ASSERT_EQ(-1, pid.index);
    }
    {
        PartitionId pid;
        makePid(pid, "t", 0, 0, 32767);
        pid.setPartitionIndex(2);
        ASSERT_EQ(0, pid.index);
    }
}

TEST_F(PartitionIdTest, testOperator) {
    PartitionId pid1, pid2;
    ASSERT_EQ(pid1, pid2);

    makePid(pid1, "t1", 0, 0, 32767, 0);
    makePid(pid2, "t1", 0, 0, 32767, 0);
    ASSERT_EQ(pid1, pid2);

    makePid(pid2, "t1", 1, 0, 32767, 0);
    ASSERT_NE(pid1, pid2);
}

} // namespace suez

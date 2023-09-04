#include "swift/broker/storage/InnerPartMetric.h"

#include <iostream>
#include <limits>
#include <stdint.h>

#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace storage {

class InnerPartMetricTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}

private:
    void expect(uint64_t writeSize,
                uint64_t readSize,
                uint64_t writeRequestNum,
                uint64_t readRequestNum,
                const InnerPartMetricStat &stat,
                bool debug = false) {
        if (debug) {
            cout << stat.writeSize << " " << stat.readSize << " " << stat.writeRequestNum << " " << stat.readRequestNum
                 << endl;
        }
        EXPECT_EQ(writeSize, stat.writeSize);
        EXPECT_EQ(readSize, stat.readSize);
        EXPECT_EQ(writeRequestNum, stat.writeRequestNum);
        EXPECT_EQ(readRequestNum, stat.readRequestNum);
    }
    void setPoint(uint64_t writeSize,
                  uint64_t readSize,
                  uint64_t writeRequestNum,
                  uint64_t readRequestNum,
                  InnerPartMetricStat &stat) {
        stat.writeSize = writeSize;
        stat.readSize = readSize;
        stat.writeRequestNum = writeRequestNum;
        stat.readRequestNum = readRequestNum;
    }
};

TEST_F(InnerPartMetricTest, testSimple) {
    InnerPartMetric ipm(5);
    InnerPartMetricStat stat, point;
    ipm.getMetric1min(stat);
    expect(0, 0, 0, 0, stat);
    ipm.getMetric5min(stat);
    expect(0, 0, 0, 0, stat);

    for (int i = 0; i < 100; i++) {
        setPoint(i + 1, i + 2, i + 3, i + 4, point);
        ipm.update(point);
        ipm.getMetric1min(stat);
        if (i < 12) {
            expect(i + 1, i + 2, i + 3, i + 4, stat);
        } else {
            expect(12, 12, 12, 12, stat);
        }
        ipm.getMetric5min(stat);
        if (i < 60) {
            expect(i + 1, i + 2, i + 3, i + 4, stat);
        } else {
            expect(60, 60, 60, 60, stat);
        }
    }
}

TEST_F(InnerPartMetricTest, testEdge) {
    InnerPartMetric ipm(5);
    InnerPartMetricStat stat, point;
    uint64_t maxu64 = numeric_limits<uint64_t>::max();
    setPoint(maxu64 - 1, maxu64, maxu64 + 1, maxu64 + 2, point);
    ipm.update(point);
    ipm.getMetric1min(stat);
    expect(maxu64 - 1, maxu64, 0, 1, stat, true);
    ipm.getMetric5min(stat);
    expect(maxu64 - 1, maxu64, 0, 1, stat, true);
    for (int i = 0; i < 12; i++) {
        setPoint(1, 1, 1, 1, point);
        ipm.update(point);
    }

    uint64_t res = 0 - maxu64;
    cout << "0 - maxu64 = " << res << endl; // 1
    res = 1 - maxu64;
    cout << "1 - maxu64 = " << res << endl; // 2
    ipm.getMetric1min(stat);
    expect(3, 2, 1, 0, stat);
    ipm.getMetric5min(stat);
    expect(1, 1, 1, 1, stat);

    for (int i = 0; i < 48; i++) {
        setPoint(1, 1, 1, 1, point);
        ipm.update(point);
    }
    ipm.getMetric1min(stat);
    expect(0, 0, 0, 0, stat);
    ipm.getMetric5min(stat);
    expect(3, 2, 1, 0, stat);

    setPoint(1, 1, 1, 1, point);
    ipm.update(point);
    ipm.getMetric5min(stat);
    expect(0, 0, 0, 0, stat);
}

} // namespace storage
} // namespace swift

#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/MemoryResourceChecker.h>
#include <sys/sysinfo.h>

BEGIN_HA3_NAMESPACE(util);

class MemoryResourceCheckerTest : public TESTBASE {
public:
    MemoryResourceCheckerTest();
    ~MemoryResourceCheckerTest();
public:
    void setUp();
    void tearDown();
protected:
    size_t getPhyMemSize() {
        struct sysinfo s_info;
        int error = 0;
        error = sysinfo(&s_info);
        assert(error == 0);
        return s_info.totalram;
    }
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, MemoryResourceCheckerTest);


class FakeMemoryResourceChecker : public MemoryResourceChecker
{
public:
    FakeMemoryResourceChecker(int64_t resSize, int64_t freeSize) {
        _procResMemSize = resSize;
        _sysFreeMemSize = freeSize;
    }
private:
    /*override*/ int64_t getProcResMemSize() {
        return _procResMemSize;
    }
    /*override*/ int64_t getSysFreeMemSize() {
        return _sysFreeMemSize;
    }
private:
    int64_t _procResMemSize;
    int64_t _sysFreeMemSize;
};

MemoryResourceCheckerTest::MemoryResourceCheckerTest() { 
}

MemoryResourceCheckerTest::~MemoryResourceCheckerTest() { 
}

void MemoryResourceCheckerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void MemoryResourceCheckerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(MemoryResourceCheckerTest, testGetSysFreeMemSize) {
    //test rough
    size_t phyMemSize = getPhyMemSize();
    MemoryResourceChecker checker;
    size_t freeMemSize = checker.getSysFreeMemSize();
    size_t procResMemSize = checker.getProcResMemSize();
    HA3_LOG(INFO, "phyMemSize %lu KB, freeMemSize %lu KB, procResMemSize %lu KB",
            phyMemSize / 1024, freeMemSize / 1024, procResMemSize / 1024);
    ASSERT_TRUE(phyMemSize > freeMemSize + procResMemSize);
}

TEST_F(MemoryResourceCheckerTest, testGetFreeMemSize) {
    int64_t safeMemGuard = 20l;
    {
        FakeMemoryResourceChecker checker(0, 1);
        int64_t maxAvailaleMemSize = 0;
        ASSERT_TRUE(!checker.getFreeMemSize(0, safeMemGuard, 
                        maxAvailaleMemSize));
    }
    {
        FakeMemoryResourceChecker checker(1, 0);
        int64_t maxAvailaleMemSize = 0;
        ASSERT_TRUE(checker.getFreeMemSize(0, safeMemGuard, 
                        maxAvailaleMemSize));
        ASSERT_EQ(-1L, maxAvailaleMemSize);
    }
    {
        FakeMemoryResourceChecker checker(40, 50);
        int64_t maxAvailaleMemSize = 0;
        ASSERT_TRUE(checker.getFreeMemSize(100, safeMemGuard, 
                        maxAvailaleMemSize));
        ASSERT_EQ(60L, maxAvailaleMemSize);
    }
    {
        FakeMemoryResourceChecker checker(4000, 50);
        int64_t maxAvailaleMemSize = 0;
        ASSERT_TRUE(checker.getFreeMemSize(1000, safeMemGuard, 
                        maxAvailaleMemSize));
        ASSERT_EQ(-3000L, maxAvailaleMemSize);
    }
}

END_HA3_NAMESPACE(util);

#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ObjectAllocator.h>
#include <autil/ShortString.h>
#include <autil/TimeUtility.h>

using namespace std;
BEGIN_HA3_NAMESPACE(util);

class ObjectAllocatorPerfTest : public TESTBASE {
public:
    ObjectAllocatorPerfTest();
    ~ObjectAllocatorPerfTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

 HA3_LOG_SETUP(util, ObjectAllocatorPerfTest);


ObjectAllocatorPerfTest::ObjectAllocatorPerfTest() { 
}

ObjectAllocatorPerfTest::~ObjectAllocatorPerfTest() { 
}

void ObjectAllocatorPerfTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ObjectAllocatorPerfTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ObjectAllocatorPerfTest, testAllocateManyShortString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    uint64_t startTime = TimeUtility::currentTime();

    ObjectAllocator<ShortString> allocator;
    vector<ShortString *> pVec;
    pVec.reserve(1024 * 1024);
    size_t count = 0;
    bool result = true;
    for (size_t i = 0; i < 1024 * 1024; ++i) {
        if (rand() % 3) {
            ShortString *p = allocator.allocate();
            ++count;
            result = result && (count == allocator.getCount());
            result = result && (p->empty());
            result = result && (p->begin() == p->end());
            *p = "sdfsdfwefrwer";
            pVec.push_back(p);
        } else if (rand() % 2) {
            size_t pos = (size_t(rand()) *rand()) % pVec.size();
            if (pVec[pos]) {
                --count;
                allocator.free(pVec[pos]);
                result = result && (count == allocator.getCount());
                pVec[pos] = NULL;
            }
        } else {
            size_t pos = (size_t(rand()) *rand()) % pVec.size();
            if (pVec[pos]) {
                pVec[pos]->assign("xxxxxx");
            }
        }
    }

    ASSERT_TRUE(result);

    for (size_t i = 0; i < pVec.size(); ++i) {
        if (pVec[i]) {
            --count;
            allocator.free(pVec[i]);
            result = result && (count == allocator.getCount());
            pVec[i] = NULL;
        }
    }
    
    ASSERT_TRUE(result);
    uint64_t endTime = TimeUtility::currentTime();
    HA3_LOG(ERROR, "ObjectAllocator performance time cost: (%lu us)", endTime - startTime);
}

END_HA3_NAMESPACE(util);

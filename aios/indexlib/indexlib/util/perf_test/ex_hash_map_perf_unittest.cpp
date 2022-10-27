#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/hash_map.h"
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(util);

class ExHashMapPerfTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ExHashMapPerfTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForExHashMap()
    {
        IE_LOG(INFO, "begin HashMap");
        // HashMap<uint64_t, uint64_t> hashMap(-1, -1, HASHMAP_INIT_SIZE);
        HashMap<uint64_t, uint64_t> hashMap(HASHMAP_INIT_SIZE);
        TestCaseForExHashMapTemplate(hashMap);
        IE_LOG(INFO, "end HashMap");
    }

    template <class T>
    void TestCaseForExHashMapTemplate(T &hashMap)
    {
        uint64_t n = 10 * 1000 * 1000;

        int64_t beginTime = TimeUtility::currentTime();
        for (uint64_t i = 0; i < n; ++i) {
            uint64_t key = rand();
            hashMap.Insert(key, key);
        }
        int64_t endTime = TimeUtility::currentTime();
        cout << "build map time passed: " << (endTime - beginTime) / 1000 << endl;

        uint64_t findTimes = 10000;
        beginTime = TimeUtility::currentTime();
        for (uint64_t i = 0; i < findTimes; ++i) {
            uint64_t key = rand();
            hashMap.Find(key);
        }
        endTime = TimeUtility::currentTime();
        cout << "find map time passed: " << (endTime - beginTime) / 1000 << " ms" << endl;
        cout << "avg find map time: " << (double)(endTime - beginTime) / findTimes << endl;
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(util, ExHashMapPerfTest);

INDEXLIB_UNIT_TEST_CASE(ExHashMapPerfTest, TestCaseForExHashMap);

IE_NAMESPACE_END(util);

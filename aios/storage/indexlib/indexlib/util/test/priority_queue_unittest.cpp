#include <queue>

#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/priority_queue.h"

using namespace std;

namespace indexlib { namespace util {

class PriorityQueueTest : public INDEXLIB_TESTBASE
{
private:
    template <typename T>
    struct Comparator {
    public:
        Comparator(bool desc = true) : mDesc(desc) {}

        bool operator()(const T& left, const T& right) const
        {
            if (mDesc) {
                return left > right;
            } else {
                return left < right;
            }
        }

    private:
        bool mDesc;
    };

public:
    DECLARE_CLASS_NAME(PriorityQueueTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForPush()
    {
        InnerTestForPush<int32_t>(100, true);
        InnerTestForPush<int32_t>(100, false);
        InnerTestForPush<int32_t>(10000, true);
        InnerTestForPush<int32_t>(10000, false);
        InnerTestForPush<double>(10000, false);
        InnerTestForPush<double>(10000, true);
        InnerTestForPush<float>(10000, false);
        InnerTestForPush<float>(10000, true);
        InnerTestForPush<uint64_t>(10000, true);
        InnerTestForPush<uint64_t>(10000, false);
    }

    void TestCaseForPushBack()
    {
        InnerTestForPushBack<int32_t>(7);
        InnerTestForPushBack<int32_t>(15);
        InnerTestForPushBack<int32_t>(31);
        InnerTestForPushBack<int32_t>(1025);
    }

    void TestCaseForCapaticy()
    {
        Comparator<int32_t> cmp;
        PriorityQueue<int32_t, Comparator<int32_t>> q(cmp);
        q.Init();

        INDEXLIB_TEST_EQUAL((size_t)0, q.Size());
        INDEXLIB_TEST_EQUAL((size_t)2, q.Capacity());

        q.Push(1);
        INDEXLIB_TEST_EQUAL((size_t)1, q.Size());
        INDEXLIB_TEST_EQUAL((size_t)2, q.Capacity());

        q.Push(2);
        INDEXLIB_TEST_EQUAL((size_t)2, q.Size());
        INDEXLIB_TEST_EQUAL((size_t)4, q.Capacity());

        for (size_t i = 3; i <= 1024; i++) {
            q.Push(i);
        }
        INDEXLIB_TEST_EQUAL((size_t)1024, q.Size());
        INDEXLIB_TEST_EQUAL((size_t)2048, q.Capacity());
    }

    void TestCaseForPopAndPush()
    {
        InnerTestForPopAndPush<int32_t>(1000, true);
        InnerTestForPopAndPush<int32_t>(1000, false);
    }

private:
    template <typename T>
    void InnerTestForPush(size_t itemCount, bool desc)
    {
        Comparator<T> cmp(desc);

        PriorityQueue<T, Comparator<T>> queue1(cmp);
        queue1.Init();

        priority_queue<T, vector<T>, Comparator<T>> queue2(cmp);

        // make data
        for (size_t i = 0; i < itemCount; i++) {
            T value = (T)rand();
            queue1.Push(value);
            queue2.push(value);
        }

        // check data
        INDEXLIB_TEST_EQUAL(queue2.size(), queue2.size());
        size_t size = queue2.size();
        for (size_t i = 0; i < size; i++) {
            T v1 = queue1.Top();
            T v2 = queue2.top();
            INDEXLIB_TEST_EQUAL(v2, v1);
            queue1.Pop();
            queue2.pop();
        }
    }

    template <typename T>
    void InnerTestForPushBack(size_t itemCount)
    {
        Comparator<T> cmp;
        PriorityQueue<T, Comparator<T>> queue1(cmp);
        queue1.Init();
        vector<T> queue2;

        for (size_t i = 0; i < itemCount; i++) {
            T value = (T)rand();
            queue1.PushBack(value);
            queue2.push_back(value);
        }

        INDEXLIB_TEST_EQUAL(queue2.size(), queue1.Size());
        INDEXLIB_TEST_EQUAL(queue2.capacity(), queue1.Capacity());
        for (size_t i = 0; i < queue2.size(); i++) {
            INDEXLIB_TEST_EQUAL(queue2[i], queue1[i]);
        }
    }

    template <typename T>
    void InnerTestForPopAndPush(size_t itemCount, bool desc)
    {
        Comparator<T> cmp(desc);
        PriorityQueue<T, Comparator<T>> q(cmp);
        q.Init();

        T min = numeric_limits<T>::max();
        T max = numeric_limits<T>::min();
        for (size_t i = 0; i < itemCount; i++) {
            T value = (T)rand();
            q.Push(value);
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }

        T expectedValue = T(0);
        T actualValue = T(0);
        if (desc) {
            expectedValue = min;
            actualValue = q.PopAndPush(min);
        } else {
            expectedValue = max;
            actualValue = q.PopAndPush(max);
        }

        INDEXLIB_TEST_EQUAL(expectedValue, actualValue);
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, PriorityQueueTest);

INDEXLIB_UNIT_TEST_CASE(PriorityQueueTest, TestCaseForPush);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueTest, TestCaseForPushBack);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueTest, TestCaseForCapaticy);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueTest, TestCaseForPopAndPush);
}} // namespace indexlib::util

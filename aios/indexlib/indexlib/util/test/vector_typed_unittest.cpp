#include <autil/Thread.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/vector_typed.h"
#include "indexlib/util/simple_pool.h"

using namespace std;
using namespace std::tr1;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_BEGIN(util);

class VectorTypedTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(VectorTypedTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForOperatorBracket()
    {
        vector<int32_t> answer;
        VectorTyped<int32_t> vect(&mSimplePool);
        static const uint32_t TEST_DATA_SIZE = 1000;
        MakeData(answer, vect, TEST_DATA_SIZE);
        CheckData<int32_t>(answer, vect, TEST_DATA_SIZE);
    }

    void TestCaseForReserve()
    {
        VectorTyped<int32_t> vect(&mSimplePool);
        INDEXLIB_TEST_EQUAL((size_t)0, vect.Size());

        vect.Reserve(8);
        for (int32_t i = 0; i < 8; i++) 
        {
            vect.PushBack(i);
        }
        INDEXLIB_TEST_EQUAL((size_t)8, vect.Size());

        vect.Reserve(6);
        INDEXLIB_TEST_EQUAL((size_t)8, vect.Size());
    }

    void TestCaseForResize()
    {
        vector<int32_t> answer;
        VectorTyped<int32_t> vect(&mSimplePool);
        static const uint32_t TEST_DATA_SIZE = 1000;
        MakeData(answer, vect, TEST_DATA_SIZE);
        CheckData<int32_t>(answer, vect, TEST_DATA_SIZE);

        vect.Resize(TEST_DATA_SIZE * 2);
        answer.resize(TEST_DATA_SIZE * 2);
        CheckData<int32_t>(answer, vect, TEST_DATA_SIZE);
    }

    void TestCaseForCopy()
    {
        vector<int32_t> answer;
        VectorTyped<int32_t> srcVect(&mSimplePool);
        static const uint32_t TEST_DATA_SIZE = 1000;
        MakeData(answer, srcVect, TEST_DATA_SIZE);

        VectorTyped<int32_t> copiedVect(&mSimplePool);
        copiedVect = srcVect;
        CheckData<int32_t>(answer, copiedVect, TEST_DATA_SIZE);
    }

    void TestCaseForClear()
    {
        vector<int32_t> answer;
        VectorTyped<int32_t> vect(&mSimplePool);
        static const uint32_t TEST_DATA_SIZE = 1000;
        MakeData(answer, vect, TEST_DATA_SIZE);

        vect.Clear();
        INDEXLIB_TEST_EQUAL((size_t)0, vect.Size());
        answer.clear();

        MakeData(answer, vect, TEST_DATA_SIZE);
        CheckData<int32_t>(answer, vect, TEST_DATA_SIZE);
    }
    
    void TestCaseForIterator()
    {
        vector<int32_t> answer;
        VectorTyped<int32_t> vect(&mSimplePool);
        static const uint32_t TEST_DATA_SIZE = 1000;
        MakeData(answer, vect, TEST_DATA_SIZE);

        size_t pos = 0;
        for (VectorTyped<int32_t>::Iterator iter = vect.Begin();
             iter != vect.End(); ++iter, ++pos)
        {
            ASSERT_EQ(answer[pos], *iter);
        }
    }
    
    void TestCaseForMultiThread()
    {
        vector<int32_t> answer;
        VectorTyped<int32_t> srcVect(&mSimplePool);
        static const uint32_t TEST_DATA_SIZE = 100000;
        MakeData(answer, srcVect, TEST_DATA_SIZE);

        const uint32_t READER_THREAD_COUNT = 32;
        ThreadPtr readingThreads[READER_THREAD_COUNT]; 

        VectorTyped<int32_t> toWriteVect(&mSimplePool);
        mReadingThreadParam.addedCount = 0;
        mReadingThreadParam.answerVect = &answer;
        mReadingThreadParam.toReadVect = &toWriteVect;
        mReadingThreadParam.done = false;

        for (uint32_t i = 0; i < READER_THREAD_COUNT; ++i)
        {
            readingThreads[i] = Thread::createThread(tr1::bind(&VectorTypedTest::ReadingThreadProc, this));
        }
        
        for (uint32_t i = 0; i< (uint32_t)TEST_DATA_SIZE; ++i)
        {
            if (i % 50 == 0)
            {
                toWriteVect.Resize(i + 50);
            }

            toWriteVect[i] = answer[i];
            mReadingThreadParam.addedCount++;
        }

        mReadingThreadParam.done = true;
        for (uint32_t i = 0; i < READER_THREAD_COUNT; ++i)
        {
            readingThreads[i]->join();
        }
    }

private:
    template<typename T>
    void MakeData(vector<T>& answer, VectorTyped<T>& data, size_t dataSize)
    {
        for (size_t i = 0; i < dataSize; ++i)
        {
            T val = (T)(i * 39 + i % 17);
            answer.push_back(val);
            data.PushBack(val);
        }
    }

    template<typename T>
    void CheckData(const vector<T>& answer, const VectorTyped<T>& data, size_t dataSize)
    {
        INDEXLIB_TEST_EQUAL(answer.size(), data.Size());
        for (size_t i = 0; i < dataSize; ++i)
        {
            INDEXLIB_TEST_EQUAL(answer[i], data[i]);
        }
    }

    int32_t ReadingThreadProc()
    {
        while (!mReadingThreadParam.done)
        {
            uint32_t addedCount = mReadingThreadParam.addedCount;
            VectorTyped<int32_t> copiedVect(*mReadingThreadParam.toReadVect);
            for (uint32_t i = 0; i < addedCount; ++i)
            {
                assert((*mReadingThreadParam.answerVect)[i] == copiedVect[i]);
                if ((*mReadingThreadParam.answerVect)[i] != copiedVect[i])
                {
                    throw;
                }

            }
        }
        return 0;
    }


private:
    struct ReadingThreadParam
    {
        vector<int32_t>* answerVect;
        VectorTyped<int32_t>* toReadVect;
        volatile uint32_t addedCount;
        volatile bool done;
    };
    ReadingThreadParam mReadingThreadParam;
    SimplePool mSimplePool;

};

INDEXLIB_UNIT_TEST_CASE(VectorTypedTest, TestCaseForOperatorBracket);
INDEXLIB_UNIT_TEST_CASE(VectorTypedTest, TestCaseForReserve);
INDEXLIB_UNIT_TEST_CASE(VectorTypedTest, TestCaseForResize);
INDEXLIB_UNIT_TEST_CASE(VectorTypedTest, TestCaseForCopy);
INDEXLIB_UNIT_TEST_CASE(VectorTypedTest, TestCaseForClear);
INDEXLIB_UNIT_TEST_CASE(VectorTypedTest, TestCaseForIterator);
INDEXLIB_UNIT_TEST_CASE(VectorTypedTest, TestCaseForMultiThread);

IE_NAMESPACE_END(util);

#define APSARA_UNIT_TEST_MAIN

#include "indexlib/test/unittest.h"
#include "indexlib/misc/exception.h"
#include "indexlib/common_define.h"
#include <unordered_map>
#include "autil/TimeUtility.h"

using namespace std;
using namespace __gnu_cxx;
using namespace autil;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);

class TestException : public misc::ExceptionBase
{
    public:
};

class Example1Test : public INDEXLIB_TESTBASE
{
public:

    DECLARE_CLASS_NAME(Example1Test);
    void CaseSetUp() override
    {
        number1 = 5;
        number2 = 5;
    }

    void CaseTearDown() override
    {
        //do nothing here
    }

    void TestCaseForTestTrue()
    {
        INDEXLIB_TEST_TRUE(number1 == number2);
    }

    void AnotherTestCaseForTestTrue()
    {
        INDEXLIB_TEST_TRUE(true);
    }

    void TestCaseForTestCaseId()
    {
        INDEXLIB_TEST_TRUE(true) << "test desc";
    }

    void TestCaseForTestEqual()
    {
        INDEXLIB_TEST_EQUAL(4, 4);
        INDEXLIB_TEST_EQUAL(4, 4) << "test desc";
    }
    
    void ExceptionFunction()
    {
        throw TestException();
    }

    void TestCaseForException()
    {
        INDEXLIB_EXPECT_EXCEPTION(ExceptionFunction(), TestException);
    }


    class Comparator
    {
    public:
        bool operator() (unordered_map<int, int>::iterator it1, 
                       unordered_map<int, int>::iterator it2)
        {
            return it1->first < it2->first;
        }
    };

    void TestCaseForHashMap()
    {
        int i1;

        unordered_map<int,int> map1;
        int elem_num  = 1000000;
        vector<int> vect;

        for (int i = 0; i < elem_num; i++)
        {
            map1.find(2000 - i);
            map1.find(2000 - i);

            map1[2000 - i]  = 0;
            vect.push_back(2000 - i);
        }
        sort(vect.begin(), vect.end());

        vector<int>::iterator iter;
        unordered_map<int,int>::iterator mapIter;
        for (iter = vect.begin(); iter != vect.end(); iter++)
        {
            mapIter = map1.find(*iter);
        }
        
        map<int, int> map2;
        for (int i = 0; i < elem_num; i++)
        {
            map2.find(2000 - i);
            map2.find(2000 - i);

            map2[2000 - i]  = 0;
        } 

        map<int,int>::iterator iter1;
        for (iter1 = map2.begin(); iter1 != map2.end(); iter1++)
        {
            i1 = iter1->first;
        }

        unordered_map<int,int>::iterator mapIter1;
        for (mapIter1 = map1.begin(); mapIter1 != map1.end(); mapIter1++)
        {
            i1 = mapIter1->first;
        }

        map<int,int>::iterator hashIter;
        for (hashIter = map2.begin(); hashIter != map2.end(); hashIter++)
        {
            i1 = hashIter->first;
        }
	(void)i1;
    }

private:
    int number1, number2;
};

INDEXLIB_UNIT_TEST_CASE(Example1Test, TestCaseForTestTrue);
INDEXLIB_UNIT_TEST_CASE(Example1Test, AnotherTestCaseForTestTrue);
INDEXLIB_UNIT_TEST_CASE(Example1Test, TestCaseForTestCaseId);
INDEXLIB_UNIT_TEST_CASE(Example1Test, TestCaseForHashMap);
INDEXLIB_UNIT_TEST_CASE(Example1Test, TestCaseForTestEqual);
INDEXLIB_UNIT_TEST_CASE(Example1Test, TestCaseForException);

IE_NAMESPACE_END(util);

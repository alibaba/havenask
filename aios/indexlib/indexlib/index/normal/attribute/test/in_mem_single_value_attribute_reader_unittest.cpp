#include <autil/mem_pool/Pool.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_single_value_attribute_reader.h"
#include "indexlib/util/slice_array/aligned_slice_array.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

class InMemSingleValueAttributeReaderTest : public INDEXLIB_TESTBASE
{
private:
    autil::mem_pool::Pool mPool;
public:
    typedef InMemSingleValueAttributeReader<uint32_t> UInt32Reader;

    void CaseSetUp() override
    {
        srand(8888);
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForRead()
    {
        common::TypedSliceList<uint32_t> data(4, 2, &mPool);
        vector<uint32_t> answer;
        BuildData(data, answer);
        
        UInt32Reader reader(&data);
        Check(reader, answer);
    }

private:
    void BuildData(common::TypedSliceList<uint32_t>& data, 
                   vector<uint32_t>& answer)
    {
        for (uint32_t i = 0; i < 200; ++i)
        {
            uint32_t value = rand() % 100;
            data.PushBack(value);
            answer.push_back(value);
        }
    }
    
    void Check(InMemSingleValueAttributeReader<uint32_t>& reader, 
               const vector<uint32_t>& answer)
    {
        INDEXLIB_TEST_TRUE(reader.IsInMemory());
        for (size_t docId = 0; docId < answer.size(); ++docId)
        {
            uint32_t value;
            INDEXLIB_TEST_EQUAL(sizeof(uint32_t), reader.GetDataLength((docId)));
            reader.Read((uint64_t)docId, value);
            INDEXLIB_TEST_EQUAL(answer[docId], value);
        }
    }
};

INDEXLIB_UNIT_TEST_CASE(InMemSingleValueAttributeReaderTest, TestCaseForRead);

IE_NAMESPACE_END(index);

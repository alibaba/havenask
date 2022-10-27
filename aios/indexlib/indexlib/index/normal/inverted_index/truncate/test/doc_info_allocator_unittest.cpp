#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class DocInfoAllocatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(DocInfoAllocatorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForAllocateDocIdReference()
    {
        DocInfoAllocator allocator;
        Reference* refer = allocator.GetReference(DocInfoAllocator::DOCID_REFER_NAME);
        INDEXLIB_TEST_TRUE(refer);
        INDEXLIB_TEST_EQUAL(sizeof(docid_t), allocator.GetDocInfoSize());
    }

    void TestCaseForDeclareTwice()
    {
        DocInfoAllocator allocator;
        Reference* refer1 = allocator.DeclareReference("int32", ft_int32);
        INDEXLIB_TEST_TRUE(refer1);

        Reference* refer2 = allocator.DeclareReference("int32", ft_int32);
        INDEXLIB_TEST_TRUE(refer2);

        INDEXLIB_TEST_EQUAL(refer1, refer2);

        Reference* refer3 = allocator.GetReference("int32");
        INDEXLIB_TEST_EQUAL(refer3, refer2);
    }

    void TestCaseForDocInfoSize()
    {
        DocInfoAllocator allocator;
        uint32_t docInfoSize = sizeof(docid_t);
        INDEXLIB_TEST_EQUAL(docInfoSize, allocator.GetDocInfoSize());

        allocator.DeclareReference("int8", ft_int8);
        docInfoSize += sizeof(int8_t);
        INDEXLIB_TEST_EQUAL(docInfoSize, allocator.GetDocInfoSize());

        allocator.DeclareReference("int16", ft_int16);
        docInfoSize += sizeof(int16_t);
        INDEXLIB_TEST_EQUAL(docInfoSize, allocator.GetDocInfoSize());

        allocator.DeclareReference("int32", ft_int32);
        docInfoSize += sizeof(int32_t);
        INDEXLIB_TEST_EQUAL(docInfoSize, allocator.GetDocInfoSize());

        allocator.DeclareReference("int64", ft_int64);
        docInfoSize += sizeof(int64_t);
        INDEXLIB_TEST_EQUAL(docInfoSize, allocator.GetDocInfoSize());

        allocator.DeclareReference("float", ft_float);
        docInfoSize += sizeof(float);
        INDEXLIB_TEST_EQUAL(docInfoSize, allocator.GetDocInfoSize());

        allocator.DeclareReference("double", ft_double);
        docInfoSize += sizeof(double);
        INDEXLIB_TEST_EQUAL(docInfoSize, allocator.GetDocInfoSize());
    }
    
    void TestCaseForAllocateDocInfo()
    {
        DocInfoAllocator allocator;
        allocator.DeclareReference("float", ft_float);
        allocator.DeclareReference("int8", ft_int8);
        
        DocInfo* docInfo = allocator.Allocate();
        INDEXLIB_TEST_TRUE(docInfo);

        allocator.Deallocate(docInfo);
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, DocInfoAllocatorTest);

INDEXLIB_UNIT_TEST_CASE(DocInfoAllocatorTest, TestCaseForAllocateDocIdReference);
INDEXLIB_UNIT_TEST_CASE(DocInfoAllocatorTest, TestCaseForDeclareTwice);
INDEXLIB_UNIT_TEST_CASE(DocInfoAllocatorTest, TestCaseForDocInfoSize);
INDEXLIB_UNIT_TEST_CASE(DocInfoAllocatorTest, TestCaseForAllocateDocInfo);

IE_NAMESPACE_END(index);

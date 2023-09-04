#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"

#include "autil/mem_pool/Pool.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DocInfoAllocatorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    size_t DeclareAllTypes(const std::shared_ptr<DocInfoAllocator>& allocator, bool supportNull) const;
    void InnerTest(bool supportNull) const;
};

size_t DocInfoAllocatorTest::DeclareAllTypes(const std::shared_ptr<DocInfoAllocator>& allocator, bool supportNull) const
{
    size_t refSize = sizeof(docid_t); // doc id
    [[maybe_unused]] auto refer = allocator->DeclareReference(/*fieldName*/ "ft_int8", ft_int8, supportNull);
    refSize += sizeof(int8_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_uint8", ft_uint8, supportNull);
    refSize += sizeof(uint8_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_int16", ft_int16, supportNull);
    refSize += sizeof(int16_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_uint16", ft_uint16, supportNull);
    refSize += sizeof(uint16_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_int32", ft_int32, supportNull);
    refSize += sizeof(int32_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_uint32", ft_uint32, supportNull);
    refSize += sizeof(uint32_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_int64", ft_int64, supportNull);
    refSize += sizeof(int64_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_uint64", ft_uint64, supportNull);
    refSize += sizeof(uint64_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_float", ft_float, supportNull);
    refSize += sizeof(float);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_double", ft_double, supportNull);
    refSize += sizeof(double);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_fp8", ft_fp8, supportNull);
    refSize += sizeof(int8_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_fp16", ft_fp16, supportNull);
    refSize += sizeof(int16_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_date", ft_date, supportNull);
    refSize += sizeof(uint32_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_time", ft_time, supportNull);
    refSize += sizeof(uint32_t);
    refer = allocator->DeclareReference(/*fieldName*/ "ft_timestamp", ft_timestamp, supportNull);
    refSize += sizeof(uint64_t);

    if (supportNull) {
        refSize += 15 * sizeof(bool);
    }
    return refSize;
}

void DocInfoAllocatorTest::InnerTest(bool supportNull) const
{
    auto allocator = std::make_shared<DocInfoAllocator>();
    auto refSize = DeclareAllTypes(allocator, supportNull);
    [[maybe_unused]] auto docInfo = allocator->Allocate();
    ASSERT_EQ(refSize, allocator->GetDocInfoSize());
    std::vector<std::string> fieldNames = {"ft_int8",   "ft_uint8", "ft_int16",  "ft_uint16", "ft_int32",
                                           "ft_uint32", "ft_int64", "ft_uint64", "ft_float",  "ft_double",
                                           "ft_fp8",    "ft_fp16",  "ft_date",   "ft_time",   "ft_timestamp"};

    for (const auto& fieldName : fieldNames) {
        auto refer = allocator->GetReference(fieldName);
        ASSERT_TRUE(refer != nullptr);
    }

    auto notExistRefer = allocator->GetReference("Not_exist");
    ASSERT_FALSE(notExistRefer != nullptr);
}
TEST_F(DocInfoAllocatorTest, TestGetDocInfoSize)
{
    auto allocator = std::make_shared<DocInfoAllocator>();
    ASSERT_EQ(sizeof(docid_t), allocator->GetDocInfoSize());
    auto docInfo = allocator->Allocate();
    ASSERT_EQ(8, allocator->GetDocInfoSize());
    allocator->Deallocate(docInfo);
}

TEST_F(DocInfoAllocatorTest, TestDeclareTwice)
{
    auto allocator = std::make_shared<DocInfoAllocator>();
    auto refer1 = allocator->DeclareReference("int32", ft_int32, /*isNull*/ false);
    ASSERT_TRUE(refer1 != nullptr);
    auto refer2 = allocator->DeclareReference("int32", ft_int32, /*isNull*/ false);
    ASSERT_TRUE(refer2 != nullptr);

    ASSERT_EQ(refer1, refer2);

    auto refer3 = allocator->GetReference("int32");
    ASSERT_EQ(refer2, refer3);
}

TEST_F(DocInfoAllocatorTest, TestNotSupportNull) { InnerTest(/*supprtNull*/ false); }

TEST_F(DocInfoAllocatorTest, TestSupportNull) { InnerTest(/*supprtNull*/ true); }

} // namespace indexlib::index

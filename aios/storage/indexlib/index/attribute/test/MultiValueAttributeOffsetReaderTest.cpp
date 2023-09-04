#include "indexlib/index/attribute/MultiValueAttributeOffsetReader.h"

#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressWriter.h"
#include "unittest/unittest.h"

using namespace indexlib::file_system;

namespace indexlibv2::index {
class MultiValueAttributeOffsetReaderTest : public TESTBASE
{
public:
    MultiValueAttributeOffsetReaderTest() = default;
    ~MultiValueAttributeOffsetReaderTest() = default;

    void setUp() override
    {
        auto fs = AttributeTestUtil::CreateFileSystem(GET_TEMP_DATA_PATH());
        _rootDir = indexlib::file_system::Directory::Get(fs);
    }
    void tearDown() override { AttributeTestUtil::ResetDir(GET_TEMP_DATA_PATH()); }

private:
    template <typename T>
    std::pair<std::unique_ptr<uint8_t[]>, size_t> PrepareOffsetBuffer(const std::string& offsetsStr,
                                                                      const std::string& delimiter, bool compress)
    {
        std::vector<T> offsets;
        std::unique_ptr<uint8_t[]> buffer = nullptr;
        size_t bufferLen = 0;
        autil::StringUtil::fromString<T>(offsetsStr, offsets, delimiter);
        if (compress) {
            indexlib::index::EquivalentCompressWriter<T> writer;
            writer.Init(64);
            for (size_t i = 0; i < offsets.size(); i++) {
                writer.CompressData(&offsets[i], 1);
            }

            bufferLen = writer.GetCompressLength() + sizeof(uint32_t);
            buffer.reset(new uint8_t[bufferLen]);
            writer.DumpBuffer(buffer.get(), bufferLen);

            uint32_t* magic = (uint32_t*)(buffer.get() + bufferLen - 4);
            if (sizeof(T) == sizeof(uint32_t)) {
                *magic = UINT32_OFFSET_TAIL_MAGIC;
            } else {
                assert(sizeof(T) == sizeof(uint64_t));
                *magic = UINT64_OFFSET_TAIL_MAGIC;
            }
        } else {
            bufferLen = offsets.size() * sizeof(T);
            buffer.reset(new uint8_t[bufferLen]);

            auto baseAddr = buffer.get();
            for (size_t i = 0; i < offsets.size(); i++) {
                *(T*)baseAddr = offsets[i];
                baseAddr += sizeof(T);
            }
        }

        return {std::move(buffer), bufferLen};
    }

    template <typename T>
    std::pair<std::shared_ptr<Directory>, std::shared_ptr<AttributeConfig>>
    PrepareFieldData(const std::vector<T>& offsetVec, bool updatable, bool compress, uint64_t u32OffsetThreshold)
    {
        const std::string delimiter(",");
        const std::string offsetStr = autil::StringUtil::toString(offsetVec.begin(), offsetVec.end(), delimiter);
        auto [buffer, bufferLen] = PrepareOffsetBuffer<T>(offsetStr, delimiter, compress);

        auto fieldDir = _rootDir->MakeDirectory("attribute")->MakeDirectory("ut_field_name");
        fieldDir->Store(ATTRIBUTE_OFFSET_FILE_NAME, std::string((const char*)buffer.get(), bufferLen));

        auto attrConfig = CreateAttributeConfig<T>(updatable, u32OffsetThreshold, compress);
        assert(attrConfig);
        return {fieldDir, attrConfig};
    }

    template <typename T>
    void InnerSimpleTest(const std::vector<T>& offsetVec, bool disableUpdate, bool updatable, bool compress)
    {
        const size_t offsetCnt = offsetVec.size() - 1;
        auto [fieldDir, attrConfig] = PrepareFieldData<T>(offsetVec, updatable, compress, /*u32OffsetThreshold*/ 0);
        MultiValueAttributeOffsetReader offsetReader(/*attributeMetrics*/ nullptr);
        auto status = offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), offsetCnt, disableUpdate);

        if (sizeof(T) == 8) {
            ASSERT_TRUE(!offsetReader.IsU32Offset());
        } else {
            ASSERT_TRUE(offsetReader.IsU32Offset());
        }

        for (size_t i = 0; i < offsetVec.size(); i++) {
            auto [status1, curOffset] = offsetReader.GetOffset((docid_t)i);
            ASSERT_TRUE(status1.IsOK());
            ASSERT_EQ(offsetVec[i], curOffset);
        }

        ASSERT_EQ(offsetCnt, offsetReader.GetDocCount());
        ASSERT_TRUE(offsetReader.GetFileReader() != nullptr);
    }

    void InnerErrorTest(bool compress)
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        const size_t offsetCnt = offsetVec.size() - 1;
        auto [fieldDir, attrConfig] =
            PrepareFieldData<uint64_t>(offsetVec, /*updatable*/ false, compress, /*u32OffsetThreshold*/ 0);
        MultiValueAttributeOffsetReader offsetReader(/*attributeMetrics*/ nullptr);
        auto status = offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), offsetCnt, /*disableUpdate*/ false);
        ASSERT_TRUE(!offsetReader.IsU32Offset());

        for (size_t i = 0; i < offsetVec.size(); i++) {
            auto [status1, curOffset] = offsetReader.GetOffset((docid_t)i);
            ASSERT_TRUE(status1.IsOK());
            ASSERT_EQ(offsetVec[i], curOffset);
        }

        ASSERT_EQ(offsetCnt, offsetReader.GetDocCount());
        ASSERT_TRUE(offsetReader.GetFileReader() != nullptr);
    }

    void InnerGetOffsetBatch(const std::vector<uint64_t>& offsetVec, const std::vector<docid_t>& docIds,
                             const std::vector<uint64_t>& expectedOffsets, bool compress)
    {
        auto [fieldDir, attrConfig] =
            PrepareFieldData<uint64_t>(offsetVec, /*updtable*/ false, compress, /*u32OffsetThreshold*/ 0);
        auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
        ASSERT_TRUE(attrDir != nullptr);

        const size_t offsetCnt = offsetVec.size() - 1;

        // data not exist in attrDir
        MultiValueAttributeOffsetReader offsetReader(/*attributeMetrics*/ nullptr);
        ASSERT_TRUE(
            offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), offsetCnt, /*disabelUpdate*/ false).IsOK());
        ASSERT_TRUE(!offsetReader.IsU32Offset());

        autil::mem_pool::Pool tmpPool;
        auto sessionReader = offsetReader.CreateSessionReader(&tmpPool);
        std::vector<uint64_t> offsets;
        indexlib::file_system::ReadOption readOption;
        auto task = sessionReader.BatchGetOffset(docIds, readOption, &offsets);
        auto results = syncAwait(task);
        ASSERT_EQ(docIds.size(), results.size());
        ASSERT_EQ(docIds.size(), offsets.size());
        ASSERT_EQ(expectedOffsets.size(), offsets.size());

        for (size_t i = 0; i < expectedOffsets.size(); i++) {
            ASSERT_EQ(expectedOffsets[i], offsets[i]);
        }
    }

    void InnerGetOffset(bool compress)
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        auto [fieldDir, attrConfig] =
            PrepareFieldData<uint64_t>(offsetVec, /*updtable*/ false, compress, /*u32OffsetThreshold*/ 0);
        auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
        ASSERT_TRUE(attrDir != nullptr);

        const size_t offsetCnt = offsetVec.size() - 1;

        // data not exist in attrDir
        MultiValueAttributeOffsetReader offsetReader(/*attributeMetrics*/ nullptr);
        ASSERT_TRUE(
            offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), offsetCnt, /*disabelUpdate*/ false).IsOK());
        ASSERT_TRUE(!offsetReader.IsU32Offset());

        for (size_t i = 0; i < offsetVec.size(); i++) {
            auto [status, curOffset] = offsetReader.GetOffset((docid_t)i);
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(offsetVec[i], curOffset);
        }
    }

    template <typename T>
    std::shared_ptr<AttributeConfig> CreateAttributeConfig(bool updatable, uint64_t u32OffsetThreshold, bool compress)
    {
        auto compressType = compress ? std::make_optional<std::string>("equal") : std::nullopt;
        return AttributeTestUtil::CreateAttrConfig(TypeInfo<T>::GetFieldType(), /*isMultiValue*/ true, "ut_field",
                                                   /*fieldId*/ 0, compressType, /*supportNull*/ false,
                                                   /*updatable*/ updatable, u32OffsetThreshold);
    }

private:
    std::shared_ptr<Directory> _rootDir;
};

TEST_F(MultiValueAttributeOffsetReaderTest, TestSimpleProcessU64NotCompress)
{
    // compressedType.HasEquivalentCompress() && !attrConfig->IsMultiValue() &&
    // attrConfig->GetFieldType() != ft_string;
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ true, /*updatable*/ false, /*compress*/ false);
    }
    tearDown();
    setUp();
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ true, /*updatable*/ true, /*compress*/ false);
    }

    tearDown();
    setUp();
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ false, /*updatable*/ false, /*compress*/ false);
    }

    tearDown();
    setUp();
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ false, /*updatable*/ true, /*compress*/ false);
    }
}

TEST_F(MultiValueAttributeOffsetReaderTest, TestSimpleProcessU64Compress)
{
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ true, /*updatable*/ false, /*compress*/ true);
    }
    tearDown();
    setUp();
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ true, /*updatable*/ true, /*compress*/ true);
    }

    tearDown();
    setUp();
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ false, /*updatable*/ false, /*compress*/ true);
    }

    tearDown();
    setUp();
    {
        std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ false, /*updatable*/ true, /*compress*/ true);
    }
}

TEST_F(MultiValueAttributeOffsetReaderTest, TestSimpleProcessU32NotCompress)
{
    {
        std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ true, /*updatale*/ false, /*compress*/ false);
    }
    tearDown();
    setUp();
    {
        std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ true, /*updatale*/ true, /*compress*/ false);
    }
    tearDown();
    setUp();
    {
        std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ false, /*updatale*/ false, /*compress*/ false);
    }
    tearDown();
    setUp();
    {
        std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ false, /*updatale*/ true, /*compress*/ false);
    }
}

TEST_F(MultiValueAttributeOffsetReaderTest, TestSimpleProcessU32Compress)
{
    {
        std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ true, /*updatale*/ false, /*compress*/ true);
    }
    tearDown();
    setUp();
    {
        std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ true, /*updatale*/ true, /*compress*/ true);
    }
    tearDown();
    setUp();
    {
        std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ false, /*updatale*/ false, /*compress*/ true);
    }
    // if disableUpdate = false and updatable = true, the type should't u32
    // tearDown();
    // setUp();
    // {
    //     std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    //     InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ false, /*updatale*/ true, /*compress*/ true);
    // }
}

TEST_F(MultiValueAttributeOffsetReaderTest, TestInitErrorNotCompress) { InnerErrorTest(/*compress*/ false); }

TEST_F(MultiValueAttributeOffsetReaderTest, TestInitErrorCompress) { InnerErrorTest(/*compress*/ true); }

TEST_F(MultiValueAttributeOffsetReaderTest, TestGetOffsetNotCompress) { InnerGetOffset(/*compress*/ false); }

TEST_F(MultiValueAttributeOffsetReaderTest, TestGetOffsetCompress) { InnerGetOffset(/*compress*/ true); }

TEST_F(MultiValueAttributeOffsetReaderTest, TestGetOffsetBatchNotCompress)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    std::vector<docid_t> docIds = {0, 1, 2, 3, 4, 5, 6};
    InnerGetOffsetBatch(offsetVec, docIds, offsetVec, /*compress*/ false);
}
TEST_F(MultiValueAttributeOffsetReaderTest, TestGetOffsetBatchCompress)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    std::vector<docid_t> docIds = {3, 4, 5};
    std::vector<uint64_t> expectedOffsets = {7, 9, 10};
    InnerGetOffsetBatch(offsetVec, docIds, expectedOffsets, /*compress*/ true);
}

} // namespace indexlibv2::index

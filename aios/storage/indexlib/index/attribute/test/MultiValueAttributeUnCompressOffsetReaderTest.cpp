#include "indexlib/index/attribute/MultiValueAttributeUnCompressOffsetReader.h"

#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "unittest/unittest.h"

using namespace indexlib::file_system;

namespace indexlibv2::index {
class MultiValueAttributeUnCompressOffsetReaderTest : public TESTBASE
{
public:
    MultiValueAttributeUnCompressOffsetReaderTest() = default;
    ~MultiValueAttributeUnCompressOffsetReaderTest() = default;

    void setUp() override
    {
        auto fs = AttributeTestUtil::CreateFileSystem(GET_TEMP_DATA_PATH());
        _rootDir = indexlib::file_system::Directory::Get(fs);
    }
    void tearDown() override { AttributeTestUtil::ResetDir(GET_TEMP_DATA_PATH()); }

private:
    template <typename T>
    std::pair<std::unique_ptr<uint8_t[]>, size_t> PrepareUnCompressedOffsetBuffer(const std::string& offsetsStr,
                                                                                  const std::string& delimiter)
    {
        std::vector<T> offsets;
        autil::StringUtil::fromString<T>(offsetsStr, offsets, delimiter);
        size_t bufferLen = offsets.size() * sizeof(T);
        std::unique_ptr<uint8_t[]> buffer(new uint8_t[bufferLen]);
        auto baseAddr = buffer.get();
        for (size_t i = 0; i < offsets.size(); i++) {
            *(T*)baseAddr = offsets[i];
            baseAddr += sizeof(T);
        }

        return {std::move(buffer), bufferLen};
    }

    template <typename T>
    std::pair<std::shared_ptr<Directory>, std::shared_ptr<AttributeConfig>>
    PrepareFieldData(const std::vector<T>& offsetVec, bool updatable, uint64_t u32OffsetThreshold)
    {
        const std::string delimiter(",");
        const std::string offsetStr = autil::StringUtil::toString(offsetVec.begin(), offsetVec.end(), delimiter);
        auto [buffer, bufferLen] = PrepareUnCompressedOffsetBuffer<T>(offsetStr, delimiter);

        auto fieldDir = _rootDir->MakeDirectory("attribute")->MakeDirectory("ut_field_name");
        fieldDir->Store(ATTRIBUTE_OFFSET_FILE_NAME, std::string((const char*)buffer.get(), bufferLen));

        auto attrConfig = CreateAttributeConfig<T>(updatable, u32OffsetThreshold);
        assert(attrConfig);
        return {fieldDir, attrConfig};
    }

    template <typename T>
    void InnerSimpleTest(const std::vector<T>& offsetVec, bool disableUpdate, bool updatable)
    {
        const size_t offsetCnt = offsetVec.size() - 1;
        auto [fieldDir, attrConfig] = PrepareFieldData<T>(offsetVec, updatable, /*u32OffsetThreshold*/ 0);
        MultiValueAttributeUnCompressOffsetReader offsetReader;
        auto status = offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), disableUpdate, offsetCnt);

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

    template <typename T>
    void InnerGetOffsetBatch(const std::vector<T>& offsetVec, const std::vector<docid_t>& docIds,
                             const std::vector<T>& expectedOffsets)
    {
        auto [fieldDir, attrConfig] = PrepareFieldData<T>(offsetVec, /*updtable*/ false, /*u32OffsetThreshold*/ 0);
        auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
        ASSERT_TRUE(attrDir != nullptr);

        const size_t offsetCnt = offsetVec.size() - 1;

        // data not exist in attrDir
        MultiValueAttributeUnCompressOffsetReader offsetReader;
        ASSERT_TRUE(
            offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetCnt).IsOK());

        if (sizeof(T) == sizeof(uint32_t)) {
            ASSERT_TRUE(offsetReader.IsU32Offset());
        } else {
            ASSERT_TRUE(!offsetReader.IsU32Offset());
        }

        autil::mem_pool::Pool tmpPool;
        auto sessionReader = offsetReader.CreateSessionReader(&tmpPool);
        std::vector<uint64_t> offsets;
        indexlib::file_system::ReadOption readOption;
        auto task = sessionReader.GetOffset(docIds, readOption, &offsets);
        auto results = syncAwait(task);
        ASSERT_EQ(docIds.size(), results.size());
        ASSERT_EQ(docIds.size(), offsets.size());
        ASSERT_EQ(expectedOffsets.size(), offsets.size());

        for (size_t i = 0; i < expectedOffsets.size(); i++) {
            ASSERT_EQ(expectedOffsets[i], offsets[i]);
        }
    }

    template <typename T>
    void InnerGetOffset()
    {
        std::vector<T> offsetVec = {0, 3, 5, 7, 9, 10, 12};
        auto [fieldDir, attrConfig] = PrepareFieldData<T>(offsetVec, /*updtable*/ false, /*u32OffsetThreshold*/ 0);
        auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
        ASSERT_TRUE(attrDir != nullptr);

        const size_t offsetCnt = offsetVec.size() - 1;

        // data not exist in attrDir
        MultiValueAttributeUnCompressOffsetReader offsetReader;
        ASSERT_TRUE(
            offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetCnt).IsOK());

        if (sizeof(T) == sizeof(uint32_t)) {
            ASSERT_TRUE(offsetReader.IsU32Offset());
        } else {
            ASSERT_TRUE(!offsetReader.IsU32Offset());
        }

        for (size_t i = 0; i < offsetVec.size(); i++) {
            auto [status, curOffset] = offsetReader.GetOffset((docid_t)i);
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(offsetVec[i], curOffset);
        }
    }

    template <typename T>
    std::shared_ptr<AttributeConfig> CreateAttributeConfig(bool updatable)
    {
        uint64_t u32OffsetThreshold = 0;
        return CreateAttributeConfig<T>(updatable, u32OffsetThreshold);
    }
    template <typename T>
    std::shared_ptr<AttributeConfig> CreateAttributeConfig(bool updatable, uint64_t u32OffsetThreshold)
    {
        return AttributeTestUtil::CreateAttrConfig(
            TypeInfo<T>::GetFieldType(), /*isMultiValue*/ true, "ut_field", /*fieldId*/ 0,
            /*CompressType*/ "equal", /*supportNull*/ false, /*updatable*/ updatable, u32OffsetThreshold);
    }

private:
    std::shared_ptr<Directory> _rootDir;
};

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestSimpleProcessU64)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ true, /*updatable*/ false);
}

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestSimpleProcessU32)
{
    std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ true, /*updatale*/ false);
}

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestSimpleProcessU64Updatable)
{
    // if disableUpdate = false and updatable = true, the type should't u32
    tearDown();
    setUp();
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ false, /*updatable*/ true);
}

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestError)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    auto [fieldDir, attrConfig] = PrepareFieldData<uint64_t>(offsetVec, /*updtable*/ false, /*u32OffsetThreshold*/ 0);
    auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
    ASSERT_TRUE(attrDir != nullptr);

    const size_t offsetCnt = offsetVec.size() - 1;
    {
        auto attrConfig = CreateAttributeConfig<uint64_t>(/*updatable*/ false);
        ASSERT_TRUE(attrConfig);
        // data not exist in attrDir
        MultiValueAttributeUnCompressOffsetReader offsetReader;
        ASSERT_FALSE(offsetReader.Init(attrConfig, attrDir->GetIDirectory(), /*disabelUpdate*/ true, offsetCnt).IsOK());
    }
    {
        auto attrConfig = CreateAttributeConfig<uint64_t>(/*updatable*/ false);
        ASSERT_TRUE(attrConfig);
        // mismatch offset cnt
        MultiValueAttributeUnCompressOffsetReader offsetReader;
        ASSERT_TRUE(offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ true, offsetVec.size())
                        .IsCorruption());
    }
    {
        auto attrConfig = CreateAttributeConfig<uint32_t>(/*updatable*/ true);
        ASSERT_TRUE(attrConfig);
        // mismatch offset cnt
        MultiValueAttributeUnCompressOffsetReader offsetReader;
        ASSERT_TRUE(offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetVec.size())
                        .IsCorruption());
    }
}

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestSetOffsetU64)
{
    uint64_t u32OffsetThreshold = 0;
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    auto [fieldDir, attrConfig] = PrepareFieldData<uint64_t>(offsetVec, /*updtable*/ true, u32OffsetThreshold);
    auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
    ASSERT_TRUE(attrDir != nullptr);

    const size_t offsetCnt = offsetVec.size() - 1;

    // data not exist in attrDir
    MultiValueAttributeUnCompressOffsetReader offsetReader;
    ASSERT_TRUE(offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetCnt).IsOK());

    ASSERT_TRUE(!offsetReader.IsU32Offset());

    for (size_t i = 0; i < offsetVec.size(); i++) {
        offsetVec[i] = offsetVec[i] + 1000;
        ASSERT_TRUE(offsetReader.SetOffset(i, offsetVec[i]));
        auto [status, curOffset] = offsetReader.GetOffset((docid_t)i);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(offsetVec[i], curOffset);
    }
}

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestSetOffsetExpand)
{
    uint64_t u32OffsetThreshold = 2000;
    std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    auto [fieldDir, attrConfig] = PrepareFieldData<uint32_t>(offsetVec, /*updtable*/ true, u32OffsetThreshold);
    auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
    ASSERT_TRUE(attrDir != nullptr);

    const size_t offsetCnt = offsetVec.size() - 1;

    // data not exist in attrDir
    MultiValueAttributeUnCompressOffsetReader offsetReader;
    ASSERT_TRUE(offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetCnt).IsOK());

    ASSERT_TRUE(offsetReader.IsU32Offset());

    for (size_t i = 0; i < offsetVec.size(); i++) {
        offsetVec[i] = offsetVec[i] + 1000;
        ASSERT_TRUE(offsetReader.SetOffset(i, offsetVec[i]));
        auto [status, curOffset] = offsetReader.GetOffset((docid_t)i);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(offsetVec[i], curOffset);
    }

    // offset > 2000 will trigger expand
    for (size_t i = 0; i < offsetVec.size(); i++) {
        offsetVec[i] = offsetVec[i] + 2000;
        ASSERT_TRUE(offsetReader.SetOffset(i, offsetVec[i]));
        auto [status, curOffset] = offsetReader.GetOffset((docid_t)i);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(offsetVec[i], curOffset);
    }
    ASSERT_TRUE(!offsetReader.IsU32Offset());
}

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestGetOffsetU32) { InnerGetOffset<uint32_t>(); }

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestGetOffsetU64) { InnerGetOffset<uint64_t>(); }

TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestGetOffsetBatchU32)
{
    std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    std::vector<docid_t> docIds = {0, 1, 2, 3, 4, 5, 6};
    InnerGetOffsetBatch<uint32_t>(offsetVec, docIds, offsetVec);
}
TEST_F(MultiValueAttributeUnCompressOffsetReaderTest, TestGetOffsetBatchU64)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    std::vector<docid_t> docIds = {3, 4, 5};
    std::vector<uint64_t> expectedOffsets = {7, 9, 10};
    InnerGetOffsetBatch<uint64_t>(offsetVec, docIds, expectedOffsets);
}
} // namespace indexlibv2::index

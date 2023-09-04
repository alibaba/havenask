#include "indexlib/index/attribute/MultiValueAttributeCompressOffsetReader.h"

#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressWriter.h"
#include "unittest/unittest.h"

using namespace indexlib::file_system;

namespace indexlibv2::index {
class MultiValueAttributeCompressOffsetReaderTest : public TESTBASE
{
public:
    MultiValueAttributeCompressOffsetReaderTest() = default;
    ~MultiValueAttributeCompressOffsetReaderTest() = default;

    void setUp() override
    {
        auto fs = AttributeTestUtil::CreateFileSystem(GET_TEMP_DATA_PATH());
        _rootDir = indexlib::file_system::Directory::Get(fs);
    }
    void tearDown() override { AttributeTestUtil::ResetDir(GET_TEMP_DATA_PATH()); }

private:
    template <typename T>
    std::pair<std::unique_ptr<uint8_t[]>, size_t> PrepareCompressedOffsetBuffer(const std::string& offsetsStr,
                                                                                const std::string& delimiter)
    {
        std::vector<T> offsets;
        autil::StringUtil::fromString<T>(offsetsStr, offsets, delimiter);

        indexlib::index::EquivalentCompressWriter<T> writer;
        writer.Init(64);
        for (size_t i = 0; i < offsets.size(); i++) {
            writer.CompressData(&offsets[i], 1);
        }

        size_t bufferLen = writer.GetCompressLength() + sizeof(uint32_t);

        std::unique_ptr<uint8_t[]> buffer(new uint8_t[bufferLen]);
        writer.DumpBuffer(buffer.get(), bufferLen);

        uint32_t* magic = (uint32_t*)(buffer.get() + bufferLen - 4);
        if (sizeof(T) == sizeof(uint32_t)) {
            *magic = UINT32_OFFSET_TAIL_MAGIC;
        } else {
            assert(sizeof(T) == sizeof(uint64_t));
            *magic = UINT64_OFFSET_TAIL_MAGIC;
        }
        return {std::move(buffer), bufferLen};
    }

    template <typename T>
    std::pair<std::shared_ptr<Directory>, std::shared_ptr<AttributeConfig>>
    PrepareFieldData(const std::vector<T>& offsetVec, bool updatable)
    {
        const std::string delimiter(",");
        const std::string offsetStr = autil::StringUtil::toString(offsetVec.begin(), offsetVec.end(), delimiter);
        auto [buffer, bufferLen] = PrepareCompressedOffsetBuffer<T>(offsetStr, delimiter);

        auto fieldDir = _rootDir->MakeDirectory("attribute")->MakeDirectory("ut_field_name");
        fieldDir->Store(ATTRIBUTE_OFFSET_FILE_NAME, std::string((const char*)buffer.get(), bufferLen));

        auto attrConfig = CreateAttributeConfig<T>(updatable);
        assert(attrConfig);
        return {fieldDir, attrConfig};
    }

    template <typename T>
    void InnerSimpleTest(const std::vector<T>& offsetVec, bool disableUpdate, bool updatable)
    {
        const size_t offsetCnt = offsetVec.size() - 1;
        auto [fieldDir, attrConfig] = PrepareFieldData<T>(offsetVec, updatable);
        MultiValueAttributeCompressOffsetReader offsetReader;
        auto status = offsetReader.Init(attrConfig, fieldDir->GetIDirectory(), disableUpdate, offsetCnt,
                                        /*null metrics*/ nullptr);

        if (sizeof(T) == 8) {
            ASSERT_TRUE(!offsetReader.IsU32Offset());
        } else {
            ASSERT_TRUE(offsetReader.IsU32Offset());
        }

        for (size_t i = 0; i < offsetVec.size(); i++) {
            auto [st, actualVal] = offsetReader.GetOffset((docid_t)i);
            ASSERT_TRUE(st.IsOK());
            ASSERT_EQ(offsetVec[i], actualVal);
        }
        size_t sliceLen = 0;
        if (!disableUpdate && updatable) {
            ASSERT_TRUE(offsetReader.GetSliceFileLen(sliceLen));
        } else {
            ASSERT_FALSE(offsetReader.GetSliceFileLen(sliceLen));
        }

        ASSERT_EQ(0, sliceLen);
        ASSERT_EQ(offsetCnt, offsetReader.GetDocCount());
        ASSERT_TRUE(offsetReader.GetFileReader() != nullptr);
    }

    template <typename T>
    void InnerGetOffsetBatch(const std::vector<T>& offsetVec, const std::vector<docid_t>& docIds,
                             const std::vector<T>& expectedOffsets)
    {
        auto [fieldDir, attrConfig] = PrepareFieldData<T>(offsetVec, /*updtable*/ false);
        auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
        ASSERT_TRUE(attrDir != nullptr);

        const size_t offsetCnt = offsetVec.size() - 1;

        // data not exist in attrDir
        MultiValueAttributeCompressOffsetReader offsetReader;
        ASSERT_TRUE(offsetReader
                        .Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetCnt,
                              /*null metrics*/ nullptr)
                        .IsOK());

        size_t sliceLen = 0;
        if (sizeof(T) == sizeof(uint32_t)) {
            ASSERT_TRUE(offsetReader.IsU32Offset());
            ASSERT_FALSE(offsetReader.GetSliceFileLen(sliceLen));
        } else {
            ASSERT_TRUE(!offsetReader.IsU32Offset());
            ASSERT_FALSE(offsetReader.GetSliceFileLen(sliceLen));
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
        auto [fieldDir, attrConfig] = PrepareFieldData<T>(offsetVec, /*updtable*/ false);
        auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
        ASSERT_TRUE(attrDir != nullptr);

        const size_t offsetCnt = offsetVec.size() - 1;

        // data not exist in attrDir
        MultiValueAttributeCompressOffsetReader offsetReader;
        ASSERT_TRUE(offsetReader
                        .Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetCnt,
                              /*null metrics*/ nullptr)
                        .IsOK());

        size_t sliceLen = 0;
        if (sizeof(T) == sizeof(uint32_t)) {
            ASSERT_TRUE(offsetReader.IsU32Offset());
            ASSERT_FALSE(offsetReader.GetSliceFileLen(sliceLen));
        } else {
            ASSERT_TRUE(!offsetReader.IsU32Offset());
            ASSERT_FALSE(offsetReader.GetSliceFileLen(sliceLen));
        }

        for (size_t i = 0; i < offsetVec.size(); i++) {
            auto [st, actualVal] = offsetReader.GetOffset((docid_t)i);
            ASSERT_TRUE(st.IsOK());
            ASSERT_EQ(offsetVec[i], actualVal);
        }
    }

    template <typename T>
    std::shared_ptr<AttributeConfig> CreateAttributeConfig(bool updatable)
    {
        return AttributeTestUtil::CreateAttrConfig(
            TypeInfo<T>::GetFieldType(), /*isMultiValue*/ true, "ut_field", /*fieldId*/ 0,
            /*CompressType*/ "equal", /*supportNull*/ false, /*updatable*/ updatable, /*u32OffsetThreshold*/ 0);
    }

private:
    std::shared_ptr<Directory> _rootDir;
};

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestSimpleProcessU64)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ true, /*updatable*/ false);
}

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestSimpleProcessU32)
{
    std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    InnerSimpleTest<uint32_t>(offsetVec, /*disabelUpdate*/ true, /*updatale*/ false);
}

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestSimpleProcessU64Updatable)
{
    // if disableUpdate = false and updatable = true, the type should't u32
    tearDown();
    setUp();
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    InnerSimpleTest<uint64_t>(offsetVec, /*disabelUpdate*/ false, /*updatable*/ true);
}

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestError)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    auto [fieldDir, attrConfig] = PrepareFieldData<uint64_t>(offsetVec, /*updtable*/ false);
    auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
    ASSERT_TRUE(attrDir != nullptr);

    const size_t offsetCnt = offsetVec.size() - 1;
    {
        auto attrConfig = CreateAttributeConfig<uint64_t>(/*updatable*/ false);
        ASSERT_TRUE(attrConfig);
        // data not exist in attrDir
        MultiValueAttributeCompressOffsetReader offsetReader;
        ASSERT_FALSE(offsetReader
                         .Init(attrConfig, attrDir->GetIDirectory(), /*disabelUpdate*/ true, offsetCnt,
                               /*null metrics*/ nullptr)
                         .IsOK());
    }
    {
        auto attrConfig = CreateAttributeConfig<uint64_t>(/*updatable*/ false);
        ASSERT_TRUE(attrConfig);
        // mismatch offset cnt
        MultiValueAttributeCompressOffsetReader offsetReader;
        ASSERT_TRUE(offsetReader
                        .Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ true, offsetVec.size(),
                              /*null metrics*/ nullptr)
                        .IsCorruption());
    }
    {
        auto attrConfig = CreateAttributeConfig<uint32_t>(/*updatable*/ true);
        ASSERT_TRUE(attrConfig);
        // mismatch offset cnt
        MultiValueAttributeCompressOffsetReader offsetReader;
        ASSERT_TRUE(offsetReader
                        .Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetVec.size(),
                              /*null metrics*/ nullptr)
                        .IsCorruption());
    }
}

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestSetOffset)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    auto [fieldDir, attrConfig] = PrepareFieldData<uint64_t>(offsetVec, /*updtable*/ true);
    auto attrDir = _rootDir->GetDirectory("attribute", /*throwExceptionIfNotExist*/ false);
    ASSERT_TRUE(attrDir != nullptr);

    const size_t offsetCnt = offsetVec.size() - 1;

    // data not exist in attrDir
    MultiValueAttributeCompressOffsetReader offsetReader;
    ASSERT_TRUE(offsetReader
                    .Init(attrConfig, fieldDir->GetIDirectory(), /*disabelUpdate*/ false, offsetCnt,
                          /*null metrics*/ nullptr)
                    .IsOK());

    size_t sliceLen = 0;
    ASSERT_TRUE(!offsetReader.IsU32Offset());
    ASSERT_TRUE(offsetReader.GetSliceFileLen(sliceLen));

    for (size_t i = 0; i < offsetVec.size(); i++) {
        offsetVec[i] = offsetVec[i] + 1000;
        ASSERT_TRUE(offsetReader.SetOffset(i, offsetVec[i]));
        auto [st, actualVal] = offsetReader.GetOffset((docid_t)i);
        ASSERT_TRUE(st.IsOK());
        ASSERT_EQ(offsetVec[i], actualVal);
    }
}

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestGetOffsetU32) { InnerGetOffset<uint32_t>(); }

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestGetOffsetU64) { InnerGetOffset<uint64_t>(); }

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestGetOffsetBatchU32)
{
    std::vector<uint32_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    std::vector<docid_t> docIds = {0, 1, 2, 3, 4, 5, 6};
    InnerGetOffsetBatch<uint32_t>(offsetVec, docIds, offsetVec);
}

TEST_F(MultiValueAttributeCompressOffsetReaderTest, TestGetOffsetBatchU64)
{
    std::vector<uint64_t> offsetVec = {0, 3, 5, 7, 9, 10, 12};
    std::vector<docid_t> docIds = {3, 4, 5};
    std::vector<uint64_t> expectedOffsets = {7, 9, 10};
    InnerGetOffsetBatch<uint64_t>(offsetVec, docIds, expectedOffsets);
}

} // namespace indexlibv2::index

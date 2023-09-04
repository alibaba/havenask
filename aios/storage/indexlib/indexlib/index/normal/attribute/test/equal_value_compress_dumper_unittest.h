#ifndef __INDEXLIB_EQUALVALUECOMPRESSDUMPERTEST_H
#define __INDEXLIB_EQUALVALUECOMPRESSDUMPERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/equal_value_compress_dumper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/SimplePool.h"

using namespace indexlib::file_system;

namespace indexlib { namespace index {

class EqualValueCompressDumperTest : public INDEXLIB_TESTBASE
{
public:
    EqualValueCompressDumperTest();
    ~EqualValueCompressDumperTest();

public:
    void TestDumpMagicTail();
    void TestDumpFile();

private:
    template <typename T>
    void InnerTestDumpFile(uint32_t count, bool needMagicTail)
    {
        tearDown();
        setUp();

        std::vector<T> data;
        MakeData<T>(1234, data);

        FileWriterPtr file = GET_PARTITION_DIRECTORY()->CreateFileWriter("dumpFile_test");
        std::string filePath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), file->GetLogicalPath());
        EqualValueCompressDumper<T> dumper(needMagicTail, &mPool);
        dumper.CompressData(data);
        dumper.Dump(file);
        ASSERT_EQ(FSEC_OK, file->Close());

        std::string content;
        file_system::FslibWrapper::AtomicLoadE(filePath, content);
        index::EquivalentCompressReader<T> reader((uint8_t*)content.data());
        size_t expectedFileLen = 0;
        EqualValueCompressAdvisor<T>::GetOptSlotItemCount(reader, expectedFileLen);
        if (needMagicTail) {
            expectedFileLen += sizeof(uint32_t); // magicTail
        }

        size_t fileLen = file_system::FslibWrapper::GetFileLength(filePath).GetOrThrow();
        file_system::FslibWrapper::DeleteFileE(filePath, DeleteOption::NoFence(false));
        ASSERT_EQ(expectedFileLen, fileLen);

        ASSERT_EQ(data.size(), (size_t)reader.Size());
        for (size_t i = 0; i < data.size(); i++) {
            ASSERT_EQ(data[i], reader[i].second);
        }
    }

private:
    template <typename T>
    util::ByteAlignedSliceArrayPtr MakeByteSliceArray(const std::vector<T>& valueVec)
    {
        util::ByteAlignedSliceArrayPtr byteSliceArray(new util::ByteAlignedSliceArray(valueVec.size()));

        for (size_t i = 0; i < valueVec.size(); ++i) {
            byteSliceArray->SetTypedValue<T>(i * sizeof(T), valueVec[i]);
        }
        return byteSliceArray;
    }

    template <typename T>
    void MakeData(uint32_t count, std::vector<T>& valueArray)
    {
        for (uint32_t i = 0; i < count; ++i) {
            T value = (T)rand();
            valueArray.push_back(value);
        }
    }

private:
    std::string mRootDir;
    util::SimplePool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(EqualValueCompressDumperTest, TestDumpMagicTail);
INDEXLIB_UNIT_TEST_CASE(EqualValueCompressDumperTest, TestDumpFile);
}} // namespace indexlib::index

#endif //__INDEXLIB_EQUALVALUECOMPRESSDUMPERTEST_H

#include "indexlib/index/normal/attribute/test/attribute_writer_unittest.h"
#include "indexlib/util/slice_array/byte_aligned_slice_array.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using testing::Return;
using testing::_;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, AttributeWriterTest);

AttributeWriterTest::AttributeWriterTest()
{
}

AttributeWriterTest::~AttributeWriterTest()
{
}

void AttributeWriterTest::SetUp()
{
    mRootDir = FileSystemWrapper::JoinPath(
            TEST_DATA_PATH, "gtest_for_single_value_attribute_writer");
    if (FileSystemWrapper::IsExist(mRootDir))
    {
        FileSystemWrapper::DeleteDir(mRootDir);
    }
    FileSystemWrapper::MkDir(mRootDir);

}

void AttributeWriterTest::TearDown()
{
    if (FileSystemWrapper::IsExist(mRootDir))
    {
        FileSystemWrapper::DeleteDir(mRootDir);
    }
}

IE_NAMESPACE_END(index);


#include "indexlib/index/normal/primarykey/test/sorted_primary_key_index_merger_typed_unittest.h"
#include "indexlib/file_system/buffered_file_reader.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);

void SortedPrimaryKeyIndexMergerTypedTest::TestCaseForSortedMerge()
{
    TestCaseForMergeUInt64();
    std::string fileName = mRootDir + "segment_3_level_0/" + INDEX_DIR_NAME
                           + "/pk/" + PRIMARY_KEY_DATA_FILE_NAME;
    BufferedFileReaderPtr fileReader(new BufferedFileReader());
    fileReader->Open(fileName);
    size_t fileLength = fileReader->GetLength();
    PKPair<uint64_t> pkData[fileLength / sizeof(PKPair<uint64_t>)];
    fileReader->Read((void*)pkData, fileLength);
    for (size_t i = 1; i < fileLength / sizeof(PKPair<uint64_t>); ++i)
    {
        INDEXLIB_TEST_TRUE(pkData[i].key > pkData[i - 1].key);
    }
}

IE_NAMESPACE_END(index);

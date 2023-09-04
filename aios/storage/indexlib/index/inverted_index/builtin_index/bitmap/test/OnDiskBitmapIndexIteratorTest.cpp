#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"

#include "autil/HashAlgorithm.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/test/BitmapPostingMaker.h"
#include "unittest/unittest.h"
namespace indexlib::index {

#define INDEX_NAME_TAG "bitmap"

class OnDiskBitmapIndexIteratorTest : public TESTBASE
{
public:
    typedef BitmapPostingMaker::Key2DocIdListMap Key2DocIdListMap;

    OnDiskBitmapIndexIteratorTest() {}

    void setUp() override
    {
        _testDir = GET_TEMP_DATA_PATH();
        file_system::FileSystemOptions options;
        options.enableAsyncFlush = false;
        options.needFlush = true;
        options.useCache = true;
        options.useRootLink = false;
        options.isOffline = false;
        _fileSystem = file_system::FileSystemCreator::Create("ut", _testDir, options).GetOrThrow();
        _directory = file_system::Directory::Get(_fileSystem);
        if (_fileSystem) {
            EXPECT_EQ(file_system::FSEC_OK,
                      _fileSystem->MountDir(_testDir, "", "", file_system::FSMT_READ_WRITE, true));
        }
    }

    void TestCaseForOneDoc()
    {
        Key2DocIdListMap answer;
        segmentid_t segId = 0;
        uint32_t docNum = 1;
        docid_t baseDocId = 0;

        BitmapPostingMaker maker(_directory, INDEX_NAME_TAG, /*enableNullTerm=*/false);
        maker.MakeOneSegmentData(segId, docNum, baseDocId, &answer);

        CheckData(answer);
    }

    void TestCaseForMultiDoc()
    {
        Key2DocIdListMap answer;
        segmentid_t segId = 0;
        uint32_t docNum = 100;
        docid_t baseDocId = 0;
        BitmapPostingMaker maker(_directory, INDEX_NAME_TAG, /*enableNullTerm=*/false);
        maker.MakeOneSegmentData(segId, docNum, baseDocId, &answer);

        CheckData(answer);
    }

private:
    void CheckData(const Key2DocIdListMap& answer)
    {
        std::stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << 0 << "_level_0/" << INDEX_DIR_NAME << "/" << INDEX_NAME_TAG;

        file_system::DirectoryPtr indexDirectory = _directory->GetDirectory(ss.str(), true);

        OnDiskBitmapIndexIterator indexIt(indexDirectory);
        indexIt.Init();

        Key2DocIdListMap::const_iterator it;

        index::DictKeyInfo key;
        std::set<dictkey_t> keySetInIndex;
        while (indexIt.HasNext()) {
            ASSERT_TRUE(indexIt.Next(key) != NULL);
            ASSERT_TRUE(!key.IsNull());
            keySetInIndex.insert(key.GetKey());
        }

        ASSERT_EQ(answer.size(), keySetInIndex.size());
        for (it = answer.begin(); it != answer.end(); ++it) {
            dictkey_t hashKey = autil::HashAlgorithm::hashString64(it->first.c_str());
            std::set<dictkey_t>::const_iterator keySetIt = keySetInIndex.find(hashKey);
            ASSERT_TRUE(keySetIt != keySetInIndex.end());
        }
    }

    std::string _testDir;
    file_system::IFileSystemPtr _fileSystem;
    file_system::DirectoryPtr _directory;
};

TEST_F(OnDiskBitmapIndexIteratorTest, TestCaseForOneDoc) { TestCaseForOneDoc(); }
TEST_F(OnDiskBitmapIndexIteratorTest, TestCaseForMultiDoc) { TestCaseForMultiDoc(); }
} // namespace indexlib::index

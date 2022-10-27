#include <autil/HashAlgorithm.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/on_disk_bitmap_index_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_maker.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_BEGIN(index);

#define INDEX_NAME_TAG "bitmap"

class OnDiskBitmapIndexIteratorTest : public INDEXLIB_TESTBASE
{
public:
    typedef BitmapPostingMaker::Key2DocIdListMap Key2DocIdListMap;

    OnDiskBitmapIndexIteratorTest()
    {
    }

    DECLARE_CLASS_NAME(OnDiskBitmapIndexIteratorTest);
    void CaseSetUp() override
    {
        mTestDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForOneDoc()
    {
        Key2DocIdListMap answer;
        segmentid_t segId = 0;
        uint32_t docNum = 1;
        docid_t baseDocId = 0;
        
        BitmapPostingMaker maker(mTestDir, INDEX_NAME_TAG);
        maker.MakeOneSegmentData(segId, docNum, baseDocId, answer);

        CheckData(answer);
    }

    void TestCaseForMultiDoc()
    {
        Key2DocIdListMap answer;
        segmentid_t segId = 0;
        uint32_t docNum = 100;
        docid_t baseDocId = 0;
        BitmapPostingMaker maker(mTestDir, INDEX_NAME_TAG);
        maker.MakeOneSegmentData(segId, docNum, baseDocId, answer);

        CheckData(answer);
    }


private:
    void CheckData(const Key2DocIdListMap& answer)
    {
        stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << 0 << "_level_0/"
           << INDEX_DIR_NAME << "/" << INDEX_NAME_TAG;

        file_system::DirectoryPtr indexDirectory = 
            GET_PARTITION_DIRECTORY()->GetDirectory(ss.str(), true);

        OnDiskBitmapIndexIterator indexIt(indexDirectory);
        indexIt.Init();

        Key2DocIdListMap::const_iterator it;

        dictkey_t key;
        set<dictkey_t> keySetInIndex;
        while (indexIt.HasNext())
        {
            INDEXLIB_TEST_TRUE(indexIt.Next(key) != NULL);  
            keySetInIndex.insert(key);
        }

        INDEXLIB_TEST_EQUAL(answer.size(), keySetInIndex.size());
        for (it = answer.begin(); it != answer.end(); ++it)
        {
            key = HashAlgorithm::hashString64(it->first.c_str());
            set<dictkey_t>::const_iterator keySetIt = keySetInIndex.find(key);
            INDEXLIB_TEST_TRUE(keySetIt != keySetInIndex.end());
        }
    }

    string mTestDir;
};

INDEXLIB_UNIT_TEST_CASE(OnDiskBitmapIndexIteratorTest, TestCaseForOneDoc);
INDEXLIB_UNIT_TEST_CASE(OnDiskBitmapIndexIteratorTest, TestCaseForMultiDoc);

IE_NAMESPACE_END(index);

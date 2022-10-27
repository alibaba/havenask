#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/single_bitmap_posting_iterator.h"
#include "indexlib/file_system/in_mem_file_node.h"

IE_NAMESPACE_BEGIN(index);

class SingleBitmapPostingIteratorTest : public INDEXLIB_TESTBASE
{
public:
    SingleBitmapPostingIteratorTest();
    ~SingleBitmapPostingIteratorTest() {}
    
    DECLARE_CLASS_NAME(SingleBitmapPostingIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForSeekDocWithOnlyOneDoc();
    void TestCaseForSeekDoc();
    void TestCaseForUnpack();
    void TestCaseForUnpackWithManyDoc();
    void TestCaseForRealTimeInit();

private:
    void MakePostingData(SingleBitmapPostingIterator& postIt,
                         const std::string& docIdStr,
                         std::vector<docid_t>& answer);

    void CheckPostingData(SingleBitmapPostingIterator& it,
                          const std::vector<docid_t>& answer);

private:
    static const termpayload_t TERM_PAYLOAD_DEF = 9;
    static const uint32_t POOL_SIZE = 1000;
    static const uint32_t MAX_DOC_COUNT = 10000;

    util::ObjectPool<SingleBitmapPostingIterator::InDocPositionStateType> mStatePool;
    std::string mTestDir;
    file_system::InMemFileNodePtr mFileReader;
};

INDEXLIB_UNIT_TEST_CASE(SingleBitmapPostingIteratorTest, TestCaseForSeekDocWithOnlyOneDoc);
INDEXLIB_UNIT_TEST_CASE(SingleBitmapPostingIteratorTest, TestCaseForSeekDoc);
INDEXLIB_UNIT_TEST_CASE(SingleBitmapPostingIteratorTest, TestCaseForUnpack);
INDEXLIB_UNIT_TEST_CASE(SingleBitmapPostingIteratorTest, TestCaseForUnpackWithManyDoc);
INDEXLIB_UNIT_TEST_CASE(SingleBitmapPostingIteratorTest, TestCaseForRealTimeInit);

IE_NAMESPACE_END(index);

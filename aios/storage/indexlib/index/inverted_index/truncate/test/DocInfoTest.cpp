#include "indexlib/index/inverted_index/truncate/DocInfo.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class DocInfoTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(DocInfoTest, TestSimple)
{
    auto buf = std::make_unique<char[]>(100);
    auto docInfo = (DocInfo*)buf.get();

    docInfo->SetDocId(1);
    ASSERT_EQ(1, docInfo->GetDocId());
    docInfo->SetDocId(100);
    ASSERT_EQ(100, docInfo->GetDocId());

    for (auto i = 0; i < 100; ++i) {
        docInfo->IncDocId();
    }
    ASSERT_EQ(200, docInfo->GetDocId());

    size_t offset = 0;
    auto addr = docInfo->Get(offset);
    *(size_t*)addr = 100;

    addr = docInfo->Get(offset + sizeof(size_t));
    *(size_t*)addr = 200;

    ASSERT_EQ(100, *(size_t*)docInfo->Get(offset));
    ASSERT_EQ(200, *(size_t*)docInfo->Get(offset + sizeof(size_t)));
}

} // namespace indexlib::index

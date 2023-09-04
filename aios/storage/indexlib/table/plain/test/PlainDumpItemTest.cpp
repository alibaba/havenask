#include "indexlib/table/plain/PlainDumpItem.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/mock/MockMemIndexer.h"
#include "indexlib/util/SimplePool.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace plain {

class PlainDumpItemTest : public TESTBASE
{
public:
    PlainDumpItemTest() = default;
    ~PlainDumpItemTest() = default;

    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PlainDumpItemTest, TestDump)
{
    std::shared_ptr<autil::mem_pool::PoolBase> dumpPool = std::make_shared<indexlib::util::SimplePool>();
    auto indexer = std::make_shared<index::MockMemIndexer>();
    EXPECT_CALL(*indexer, UpdateMemUse).WillRepeatedly(Return());
    EXPECT_CALL(*indexer, Dump).Times(2).WillOnce(Return(Status::InternalError())).WillOnce(Return(Status::OK()));
    PlainDumpItem item(dumpPool, indexer, nullptr, nullptr);
    Status status = item.Dump();
    ASSERT_FALSE(status.IsOK());
    ASSERT_FALSE(item.IsDumped());
    // retry
    status = item.Dump();
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(item.IsDumped());
}

}} // namespace indexlibv2::plain

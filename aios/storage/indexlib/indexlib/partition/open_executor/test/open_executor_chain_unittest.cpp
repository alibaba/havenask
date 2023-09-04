#include "indexlib/partition/open_executor/test/open_executor_chain_unittest.h"

#include "autil/Lock.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/online_partition_writer.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OpenExecutorChainTest);

class MockOpenExecutor : public OpenExecutor
{
public:
    MOCK_METHOD(bool, Execute, (ExecutorResource&), (override));
    MOCK_METHOD(void, Drop, (ExecutorResource&), (override));
};

DEFINE_SHARED_PTR(MockOpenExecutor);

OpenExecutorChainTest::OpenExecutorChainTest() {}

OpenExecutorChainTest::~OpenExecutorChainTest() {}

void OpenExecutorChainTest::CaseSetUp() {}

void OpenExecutorChainTest::CaseTearDown() {}

void OpenExecutorChainTest::TestSimpleProcess()
{
    index_base::Version version;
    config::IndexPartitionOptions options;
    atomic<bool> needReload;
    needReload.store(false);
    partition::OnlinePartitionReaderPtr reader;
    OnlinePartitionWriterPtr writer;
    autil::ThreadMutex lock;
    PartitionDataHolder partDataHolder;
    OnlinePartitionMetrics metrics;
    ExecutorResource resource(version, reader, lock, writer, partDataHolder, metrics, needReload, options);
    {
        OpenExecutorChain chain;
        MockOpenExecutorPtr executor(new MockOpenExecutor());

        EXPECT_CALL(*executor, Execute(_)).Times(3).WillRepeatedly(Return(true));

        for (size_t i = 0; i < 3; ++i) {
            chain.PushBack(executor);
        }
        ASSERT_TRUE(chain.Execute(resource));
    }
    {
        OpenExecutorChain chain;
        MockOpenExecutorPtr executor(new MockOpenExecutor());
        EXPECT_CALL(*executor, Execute(_)).WillOnce(Return(true)).WillOnce(Return(false));
        EXPECT_CALL(*executor, Drop(_)).Times(2);
        for (size_t i = 0; i < 3; ++i) {
            chain.PushBack(executor);
        }
        ASSERT_FALSE(chain.Execute(resource));
    }
}
}} // namespace indexlib::partition

#include "indexlib/partition/open_executor/test/open_executor_chain_unittest.h"
#include <autil/Lock.h>
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OpenExecutorChainTest);

class MockOpenExecutor: public OpenExecutor
{
public:
    MOCK_METHOD1(Execute, bool(ExecutorResource&));
    MOCK_METHOD1(Drop, void(ExecutorResource&));
};

DEFINE_SHARED_PTR(MockOpenExecutor);

OpenExecutorChainTest::OpenExecutorChainTest()
{
}

OpenExecutorChainTest::~OpenExecutorChainTest()
{
}

void OpenExecutorChainTest::CaseSetUp()
{
}

void OpenExecutorChainTest::CaseTearDown()
{
}

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
    ExecutorResource resource(version, reader, lock, writer,
                              partDataHolder, metrics, needReload, options);
    {
        OpenExecutorChain chain;
        MockOpenExecutorPtr executor(new MockOpenExecutor());

        EXPECT_CALL(*executor, Execute(_))
            .Times(3)
            .WillRepeatedly(Return(true));

        for (size_t i = 0; i < 3; ++i)
        {
            chain.PushBack(executor);
        }
        ASSERT_TRUE(chain.Execute(resource));
    }
    {
        OpenExecutorChain chain;
        MockOpenExecutorPtr executor(new MockOpenExecutor());
        EXPECT_CALL(*executor, Execute(_))
            .WillOnce(Return(true))
            .WillOnce(Return(false));
        EXPECT_CALL(*executor, Drop(_)).Times(2);
        for (size_t i = 0; i < 3; ++i)
        {
            chain.PushBack(executor);
        }
        ASSERT_FALSE(chain.Execute(resource));
    }
}

IE_NAMESPACE_END(partition);


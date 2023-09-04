#include "indexlib/base/MemoryQuotaController.h"

#include "autil/Thread.h"
#include "unittest/unittest.h"

namespace indexlibv2 {

class MemoryQuotaControllerTest : public TESTBASE
{
};

TEST_F(MemoryQuotaControllerTest, testRootController)
{
    MemoryQuotaController controller("root", 100);
    EXPECT_EQ(0, controller.GetAllocatedQuota());
    EXPECT_EQ(100, controller.GetFreeQuota());

    controller.Allocate(10);
    EXPECT_EQ(10, controller.GetAllocatedQuota());
    EXPECT_EQ(90, controller.GetFreeQuota());

    EXPECT_FALSE(controller.TryAllocate(95).IsOK());
    EXPECT_EQ(10, controller.GetAllocatedQuota());
    EXPECT_EQ(90, controller.GetFreeQuota());

    controller.Allocate(30);
    EXPECT_EQ(40, controller.GetAllocatedQuota());
    EXPECT_EQ(60, controller.GetFreeQuota());

    controller.Allocate(70);
    EXPECT_EQ(110, controller.GetAllocatedQuota());
    EXPECT_EQ(-10, controller.GetFreeQuota());
}

TEST_F(MemoryQuotaControllerTest, testLeaf)
{
    auto root = std::make_shared<MemoryQuotaController>("root", 10 * 1024 * 1024);
    auto leaf1 = std::make_shared<MemoryQuotaController>("leaf1", root);
    auto leaf2 = std::make_shared<MemoryQuotaController>("leaf2", root);

    EXPECT_EQ(0, leaf1->GetAllocatedQuota());
    EXPECT_EQ(10 * 1024 * 1024, leaf1->GetFreeQuota());

    leaf1->Allocate(10);
    EXPECT_EQ(10, leaf1->GetAllocatedQuota());
    EXPECT_EQ(6 * 1024 * 1024, leaf1->GetFreeQuota());
    EXPECT_EQ(6 * 1024 * 1024, leaf2->GetFreeQuota());

    EXPECT_FALSE(leaf1->TryAllocate(100 * 1024 * 1024).IsOK());
    EXPECT_EQ(10, leaf1->GetAllocatedQuota());
    EXPECT_EQ(6 * 1024 * 1024, leaf1->GetFreeQuota());

    EXPECT_TRUE(leaf1->TryAllocate(4 * 1024 * 1024).IsOK());
    EXPECT_EQ(10 + 4 * 1024 * 1024, leaf1->GetAllocatedQuota());
    EXPECT_EQ(2 * 1024 * 1024, leaf1->GetFreeQuota());
}

TEST_F(MemoryQuotaControllerTest, testShrinkToFit)
{
    int64_t totalQuota = 10 * 1024 * 1024;
    int64_t blockSize = 4 * 1024 * 1024;
    auto root = std::make_shared<MemoryQuotaController>("root", totalQuota);
    auto leaf1 = std::make_shared<MemoryQuotaController>("leaf1", root);
    auto leaf2 = std::make_shared<MemoryQuotaController>("leaf2", root);

    ASSERT_EQ(root->GetAllocatedQuota(), 0);
    ASSERT_EQ(leaf1->GetAllocatedQuota(), 0);
    ASSERT_EQ(leaf2->GetAllocatedQuota(), 0);

    ASSERT_EQ(root->GetTotalQuota(), totalQuota);
    ASSERT_EQ(leaf1->GetTotalQuota(), totalQuota);
    ASSERT_EQ(leaf2->GetTotalQuota(), totalQuota);

    ASSERT_EQ(root->GetFreeQuota(), totalQuota);
    ASSERT_EQ(leaf1->GetFreeQuota(), totalQuota);
    ASSERT_EQ(leaf2->GetFreeQuota(), totalQuota);

    leaf1->Allocate(100);

    ASSERT_EQ(root->GetAllocatedQuota(), blockSize);
    ASSERT_EQ(leaf1->GetAllocatedQuota(), 100);
    ASSERT_EQ(leaf2->GetAllocatedQuota(), 0);

    ASSERT_EQ(root->GetTotalQuota(), totalQuota);
    ASSERT_EQ(leaf1->GetTotalQuota(), totalQuota);
    ASSERT_EQ(leaf2->GetTotalQuota(), totalQuota);

    ASSERT_EQ(root->GetFreeQuota(), totalQuota - blockSize);
    ASSERT_EQ(leaf1->GetFreeQuota(), totalQuota - blockSize);
    ASSERT_EQ(leaf2->GetFreeQuota(), totalQuota - blockSize);

    auto status = leaf2->Reserve(100);

    ASSERT_EQ(root->GetAllocatedQuota(), 2 * blockSize);
    ASSERT_EQ(leaf1->GetAllocatedQuota(), 100);
    ASSERT_EQ(leaf2->GetAllocatedQuota(), 0);

    ASSERT_EQ(root->GetTotalQuota(), totalQuota);
    ASSERT_EQ(leaf1->GetTotalQuota(), totalQuota);
    ASSERT_EQ(leaf2->GetTotalQuota(), totalQuota);

    ASSERT_EQ(root->GetFreeQuota(), totalQuota - 2 * blockSize);
    ASSERT_EQ(leaf1->GetFreeQuota(), totalQuota - 2 * blockSize);
    ASSERT_EQ(leaf2->GetFreeQuota(), totalQuota - 2 * blockSize);

    leaf2->ShrinkToFit();

    ASSERT_EQ(root->GetAllocatedQuota(), blockSize);
    ASSERT_EQ(leaf1->GetAllocatedQuota(), 100);
    ASSERT_EQ(leaf2->GetAllocatedQuota(), 0);

    ASSERT_EQ(root->GetTotalQuota(), totalQuota);
    ASSERT_EQ(leaf1->GetTotalQuota(), totalQuota);
    ASSERT_EQ(leaf2->GetTotalQuota(), totalQuota);

    ASSERT_EQ(root->GetFreeQuota(), totalQuota - blockSize);
    ASSERT_EQ(leaf1->GetFreeQuota(), totalQuota - blockSize);
    ASSERT_EQ(leaf2->GetFreeQuota(), totalQuota - blockSize);
}

namespace {
void work(const std::shared_ptr<MemoryQuotaController>& memoryController, const bool& stop, int64_t& allocateMemorySize)
{
    while (stop) {
        if (rand() % 100 < 50) {
            int64_t size = rand() % 1024;
            memoryController->Allocate(size);
            allocateMemorySize += size;
        } else if (rand() % 100 < 70) {
            int64_t size = rand() % 1024;
            auto st = memoryController->TryAllocate(size);
            if (st.IsOK()) {
                allocateMemorySize += size;
            }
        } else if (rand() % 100 < 95) {
            int size = rand() % 1024;
            auto st = memoryController->Reserve(size);
            if (!st.IsOK()) {
                memoryController->ShrinkToFit();
            }
        } else {
            int64_t size = rand() % (allocateMemorySize + 1);
            memoryController->Free(size);
            allocateMemorySize -= size;
        }
    }
}

} // namespace

TEST_F(MemoryQuotaControllerTest, testMultiThread)
{
    int64_t totalQuota = 20 * 1024 * 1024;
    bool stop = false;
    auto root = std::make_shared<MemoryQuotaController>("root", totalQuota);
    auto leaf1 = std::make_shared<MemoryQuotaController>("leaf1", root);
    auto leaf2 = std::make_shared<MemoryQuotaController>("leaf2", root);
    auto leaf3 = std::make_shared<MemoryQuotaController>("leaf2", leaf1);
    int64_t rootSize = 0;
    int64_t leaf1Size = 0;
    int64_t leaf2Size = 0;
    int64_t leaf3Size = 0;
    autil::ThreadPtr rootThread = autil::Thread::createThread(bind(&work, root, stop, rootSize));
    autil::ThreadPtr leaf1Thread = autil::Thread::createThread(bind(&work, leaf1, stop, leaf1Size));
    autil::ThreadPtr leaf2Thread = autil::Thread::createThread(bind(&work, leaf2, stop, leaf2Size));
    autil::ThreadPtr leaf3Thread = autil::Thread::createThread(bind(&work, leaf3, stop, leaf3Size));
    sleep(5);
    stop = true;
    rootThread->join();
    leaf1Thread->join();
    leaf2Thread->join();
    leaf3Thread->join();

    int64_t rtTotalQuota = 0;
    rtTotalQuota += rootSize + root->GetLocalFreeQuota();
    rtTotalQuota += leaf1Size + leaf1->GetLocalFreeQuota();
    ASSERT_EQ(leaf1Size + leaf1->GetLocalFreeQuota(), leaf1->TEST_GetReservedParentQuota());
    rtTotalQuota += leaf2Size + leaf2->GetLocalFreeQuota();
    ASSERT_EQ(leaf2Size + leaf2->GetLocalFreeQuota(), leaf2->TEST_GetReservedParentQuota());
    rtTotalQuota += leaf3Size + leaf3->GetLocalFreeQuota();
    ASSERT_EQ(leaf3Size + leaf3->GetLocalFreeQuota(), leaf3->TEST_GetReservedParentQuota());
    ASSERT_EQ(rtTotalQuota, totalQuota);
}

} // namespace indexlibv2

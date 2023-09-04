#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTreeNode.h"

#include <functional>

#include "unittest/unittest.h"

namespace indexlib::index {
using LeafNodePtr = std::unique_ptr<LeafNode, std::function<void(LeafNode*)>>;
using InternalNodePtr = std::unique_ptr<InternalNode, std::function<void(InternalNode*)>>;

class DynamicSearchTreeNodeTest : public TESTBASE
{
public:
    DynamicSearchTreeNodeTest() = default;
    ~DynamicSearchTreeNodeTest() = default;
    void setUp() override { _dummyInternalNode = CreateInternalNode(/*maxSlotShift=*/4); }
    void tearDown() override {}

private:
    InternalNodePtr CreateInternalNode(slotid_t maxSlotShift);
    InternalNodePtr CreateInternalNodeWithoutExtraChild(slotid_t maxSlotShift, size_t numKey);
    InternalNodePtr CreateInternalNode(slotid_t maxSlotShift, size_t numKey);
    LeafNodePtr CreateLeafNode(slotid_t maxSlotShift);
    LeafNodePtr CreateLeafNode(slotid_t maxSlotShift, size_t numKey);

    void CheckLeafNode(LeafNode* node, slotid_t expectedKeySlotUse);
    void CheckInternalNode(InternalNode* node, slotid_t expectedKeySlotUse);

    void DeleteInternalNode(InternalNode* node, slotid_t maxSlotShift);
    void DeleteLeafNode(LeafNode* node, slotid_t maxSlotShift);

private:
    InternalNodePtr _dummyInternalNode;
};

InternalNodePtr DynamicSearchTreeNodeTest::CreateInternalNode(slotid_t maxSlotShift)
{
    auto mem = InternalNode::Memory(maxSlotShift);
    auto addr = operator new(mem);
    ::memset(addr, 0, mem);
    assert(addr);
    return InternalNodePtr(::new (addr) InternalNode(maxSlotShift), [this, maxSlotShift](InternalNode* node) mutable {
        DeleteInternalNode(node, maxSlotShift);
    });
}

LeafNodePtr DynamicSearchTreeNodeTest::CreateLeafNode(slotid_t maxSlotShift)
{
    auto mem = LeafNode::Memory(maxSlotShift);
    auto addr = operator new(mem);
    ::memset(addr, 0, mem);
    assert(addr);
    return LeafNodePtr(::new (addr) LeafNode(maxSlotShift),
                       [this, maxSlotShift](LeafNode* node) mutable { DeleteLeafNode(node, maxSlotShift); });
}

void DynamicSearchTreeNodeTest::DeleteInternalNode(InternalNode* node, slotid_t maxSlotShift) { operator delete(node); }

void DynamicSearchTreeNodeTest::DeleteLeafNode(LeafNode* node, slotid_t maxSlotShift) { operator delete(node); }

// Create node whose keys are 1, 3, 5, 7, 9...
InternalNodePtr DynamicSearchTreeNodeTest::CreateInternalNodeWithoutExtraChild(slotid_t maxSlotShift, size_t numKey)
{
    auto node = CreateInternalNode(maxSlotShift);
    for (int i = 0; i < numKey; ++i) {
        node->SetKey(/*idx=*/i, KeyType(2 * i + 1, false));
        node->SetChild(/*idx=*/i, _dummyInternalNode.get());
    }
    node->SetKeySlotUse(numKey);
    return node;
}

// Create node whose keys are 1, 3, 5, 7, 9...
InternalNodePtr DynamicSearchTreeNodeTest::CreateInternalNode(slotid_t maxSlotShift, size_t numKey)
{
    auto node = CreateInternalNode(maxSlotShift);
    for (int i = 0; i < numKey; ++i) {
        node->SetKey(/*idx=*/i, KeyType(2 * i + 1, false));
        node->SetChild(/*idx=*/i, _dummyInternalNode.get());
    }
    node->SetKeySlotUse(numKey);
    node->SetChild(/*idx=*/numKey, _dummyInternalNode.get());
    return node;
}

// Create node whose keys are 1, 3, 5, 7, 9...
LeafNodePtr DynamicSearchTreeNodeTest::CreateLeafNode(slotid_t maxSlotShift, size_t numKey)
{
    auto node = CreateLeafNode(maxSlotShift);
    for (int i = 0; i < numKey; ++i) {
        node->SetKey(/*idx=*/i, KeyType(2 * i + 1, false));
    }
    node->SetKeySlotUse(numKey);
    return node;
}

TEST_F(DynamicSearchTreeNodeTest, TestCopyAndInsertIntoEmptyNode)
{
    auto node = CreateInternalNodeWithoutExtraChild(/*maxSlotShift=*/4, /*numKey=*/0);
    auto newNode = CreateInternalNode(/*maxSlotShift=*/4);
    node->CopyAndInsert(newNode.get(), KeyType(123, false), /*child1=*/_dummyInternalNode.get(),
                        /*child2=*/_dummyInternalNode.get());

    EXPECT_EQ(1, newNode->KeySlotUse());
    EXPECT_EQ(newNode->Key(0), KeyType(123, false));
    EXPECT_NE(newNode->Child(0), nullptr);
}

void DynamicSearchTreeNodeTest::CheckInternalNode(InternalNode* node, slotid_t expectedKeySlotUse)
{
    EXPECT_EQ(expectedKeySlotUse, node->KeySlotUse());
    for (slotid_t k = 0; k < node->KeySlotUse() - 1; ++k) {
        EXPECT_TRUE(node->Key(k) < node->Key(k + 1));
        EXPECT_NE(node->Child(k), nullptr);
    }
    EXPECT_NE(node->Child(node->KeySlotUse() - 1), nullptr);
    EXPECT_NE(node->Child(node->KeySlotUse()), nullptr);
    if (node->KeySlotUse() + 1 <= node->MaxSlot()) {
        EXPECT_EQ(node->Child(node->KeySlotUse() + 1), nullptr);
    }
}

TEST_F(DynamicSearchTreeNodeTest, TestCopyAndInsert)
{
    for (size_t i = 1; i < 16; ++i) {
        auto node = CreateInternalNode(/*maxSlotShift=*/4, /*numKey=*/i);
        for (size_t j = 0; j < 2 * i + 1; j += 2) {
            auto newNode = CreateInternalNode(/*maxSlotShift=*/4);
            node->CopyAndInsert(newNode.get(), KeyType(j, false), /*child1=*/_dummyInternalNode.get(),
                                /*child2=*/_dummyInternalNode.get());
            CheckInternalNode(newNode.get(), /*expectedKeySlotUse=*/i + 1);
        }
    }
}

void DynamicSearchTreeNodeTest::CheckLeafNode(LeafNode* node, slotid_t expectedKeySlotUse)
{
    EXPECT_EQ(expectedKeySlotUse, node->KeySlotUse());
    for (slotid_t k = 0; k < node->KeySlotUse() - 1; ++k) {
        EXPECT_TRUE(node->Key(k) < node->Key(k + 1));
    }
}

TEST_F(DynamicSearchTreeNodeTest, TestInsertKey)
{
    for (size_t i = 1; i < 16; ++i) {
        for (size_t j = 0; j < 2 * i + 1; j += 2) {
            auto node = CreateLeafNode(/*maxSlotShift=*/4, /*numKey=*/i);
            node->InsertKey(KeyType(j, false));
            CheckLeafNode(node.get(), /*expectedKeySlotUse=*/i + 1);
        }
    }
}

TEST_F(DynamicSearchTreeNodeTest, TestLeafSplit)
{
    for (size_t j = 0; j < 33; j += 2) {
        auto node = CreateLeafNode(/*maxSlotShift=*/4, /*numKey=*/16);
        auto newNode1 = CreateLeafNode(/*maxSlotShift=*/4);
        auto newNode2 = CreateLeafNode(/*maxSlotShift=*/4);
        node->Split(newNode1.get(), newNode2.get(), KeyType(j, false));
        EXPECT_EQ(8, newNode1->KeySlotUse());
        EXPECT_EQ(9, newNode2->KeySlotUse());
        CheckLeafNode(newNode1.get(), /*expectedKeySlotUse=*/8);
        CheckLeafNode(newNode2.get(), /*expectedKeySlotUse=*/9);
    }
}

TEST_F(DynamicSearchTreeNodeTest, TestInternalSplit)
{
    for (size_t j = 0; j < 33; j += 2) {
        auto node = CreateInternalNode(/*maxSlotShift=*/4, /*numKey=*/16);
        auto newNode1 = CreateInternalNode(/*maxSlotShift=*/4);
        auto newNode2 = CreateInternalNode(/*maxSlotShift=*/4);
        KeyType splitKey;
        node->Split(KeyType(j, false), newNode1.get(), newNode2.get(), _dummyInternalNode.get(),
                    _dummyInternalNode.get(), &splitKey);
        CheckInternalNode(newNode1.get(), /*expectedKeySlotUse=*/8);
        CheckInternalNode(newNode2.get(), /*expectedKeySlotUse=*/8);
    }
}

} // namespace indexlib::index

#include "gtest/gtest.h"
#include <algorithm>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/FieldGroup.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"
#include "matchdoc/VectorStorage.h"

using namespace std;
using namespace autil;
using namespace matchdoc;

class MatchDocAllocatorCompactTest : public testing::Test {
public:
    MatchDocAllocatorCompactTest() : pool(std::make_shared<autil::mem_pool::Pool>()), allocator(pool) {}

public:
    struct RangeDef {
    public:
        RangeDef(uint32_t begin_, uint32_t end_, const std::string &op_) : begin(begin_), end(end_), op(op_) {}
        uint32_t begin;
        uint32_t end;
        string op;
    };
    struct CompactAllocatorInfo {
        MatchDocAllocatorPtr allocator;
        Reference<int> *field1;
        Reference<string> *field2;
    };
    CompactAllocatorInfo createCompactAllocator(autil::mem_pool::Pool *pool) {
        CompactAllocatorInfo info;
        auto ret = new MatchDocAllocator(pool);
        auto ref1 = ret->declare<int>("field1");
        auto ref2 = ret->declare<string>("field2");
        info.allocator.reset(ret);
        info.field1 = ref1;
        info.field2 = ref2;
        return info;
    }
    void checkSize(int32_t expect, int32_t actual) {
        if (expect != -1) {
            EXPECT_EQ(expect, actual);
        }
    }
    void testChunk(std::vector<RangeDef> defs,
                   uint32_t totalRange,
                   int32_t size = -1,
                   int32_t capacity = -1,
                   int32_t deletedCount = -1) {
        auto info = createCompactAllocator(pool.get());
        auto allocator = info.allocator;

        std::vector<MatchDoc> docs;
        docs.reserve(totalRange);
        for (size_t i = 0; i < totalRange; i++) {
            docs.push_back(allocator->allocate(i));
        }
        std::vector<MatchDoc> newDocs;
        for (const auto &def : defs) {
            for (size_t i = def.begin; i < def.end; i++) {
                if (def.op == "delete") {
                    allocator->deallocate(docs[i]);
                } else if (def.op == "keep") {
                    newDocs.push_back(docs[i]);
                }
            }
        }
        random_shuffle(newDocs.begin(), newDocs.end());
        std::vector<uint32_t> field1Values;
        std::vector<std::string> field2Values;
        for (auto doc : newDocs) {
            field1Values.push_back(info.field1->get(doc));
            field2Values.push_back(info.field2->get(doc));
        }
        auto before = newDocs;
        allocator->compact(newDocs);
        ASSERT_EQ(before.size(), newDocs.size());
        if (totalRange <= VectorStorage::CHUNKSIZE) {
            EXPECT_EQ(before, newDocs);
        }
        for (size_t i = 0; i < newDocs.size(); i++) {
            auto doc = newDocs[i];
            EXPECT_EQ(before[i].getDocId(), doc.getDocId());
            EXPECT_EQ(field1Values[i], info.field1->get(doc));
            EXPECT_EQ(field2Values[i], info.field2->get(doc));
        }
        checkSize(size, (int32_t)allocator->_size);
        checkSize(capacity, (int32_t)allocator->_capacity);
        checkSize(deletedCount, (int32_t)allocator->_deletedIds.size());

        auto old = newDocs;
        for (size_t i = 0; i < 10; i++) {
            allocator->compact(newDocs);
            EXPECT_EQ(old, newDocs);
            for (size_t i = 0; i < newDocs.size(); i++) {
                auto doc = newDocs[i];
                EXPECT_EQ(field1Values[i], info.field1->get(doc));
                EXPECT_EQ(field2Values[i], info.field2->get(doc));
            }
            checkSize(size, (int32_t)allocator->_size);
            checkSize(VectorStorage::getAlignSize(newDocs.size()), (int32_t)allocator->_capacity);
            checkSize(deletedCount, (int32_t)allocator->_deletedIds.size());
        }
    }

protected:
    std::shared_ptr<autil::mem_pool::Pool> pool;
    MatchDocAllocator allocator;
};

TEST_F(MatchDocAllocatorCompactTest, testCompactEmpty) {
    auto info = createCompactAllocator(pool.get());
    auto myAllocator = info.allocator;
    std::vector<MatchDoc> docs;
    myAllocator->compact(docs);
    EXPECT_EQ(0, myAllocator->_size);
    EXPECT_EQ(0, myAllocator->_capacity);
    EXPECT_EQ(0, myAllocator->_deletedIds.size());

    myAllocator->extend();
    Reference<string> *field3 = myAllocator->declare<string>("field3");
    ASSERT_TRUE(field3);

    myAllocator->compact(docs);
    EXPECT_EQ(0, myAllocator->_size);
    EXPECT_EQ(0, myAllocator->_capacity);
    EXPECT_EQ(0u, myAllocator->_deletedIds.size());
}

TEST_F(MatchDocAllocatorCompactTest, testCompactSingleChunk) {
    // x: to delete, 1: to keep, 0: deleted
    // enum series:
    //   x11
    //   1111
    //   111100
    //   111100xxx
    //   1111xxxxx
    //   1111xxxxx00
    //   xxxx
    //   xxxx1111
    //   xxxx11110000
    //   xxxx0000
    //   xxxx00001111
    //   0000
    //   00001111
    //   00001111xxxx
    //   0000xxxx
    //   0000xxxx1111
    {
        // x11
        auto info = createCompactAllocator(pool.get());
        auto myAllocator = info.allocator;
        MatchDoc doc1 = myAllocator->allocate(1);
        info.field1->set(doc1, 1);
        info.field2->set(doc1, "1");
        MatchDoc doc2 = myAllocator->allocate(2);
        info.field1->set(doc2, 2);
        info.field2->set(doc2, "2");
        MatchDoc doc3 = myAllocator->allocate(3);
        info.field1->set(doc3, 3);
        info.field2->set(doc3, "3");

        std::vector<MatchDoc> docs = {doc2, doc3};
        auto before = docs;
        myAllocator->compact(docs);
        EXPECT_EQ(before, docs);
        EXPECT_EQ("2", info.field2->get(docs[0]));
        EXPECT_EQ("3", info.field2->get(docs[1]));
        EXPECT_EQ(1u, myAllocator->_deletedIds.size());
        EXPECT_EQ(0u, myAllocator->_deletedIds[0]);
    }
    // 1111
    testChunk({RangeDef(0, 1024, "keep")}, 1024, 1024, 1024, 0);
    // 111100
    testChunk({RangeDef(0, 100, "keep"), RangeDef(100, 1024, "delete")}, 1024, 100, 1024, 0);
    // 111100xxx
    testChunk({RangeDef(0, 100, "keep"), RangeDef(100, 300, "delete")}, 1024, 100, 1024, 0);
    // 1111xxxxx
    testChunk({RangeDef(0, 100, "keep")}, 1024, 100, 1024, 0);
    // 1111xxxxx00
    testChunk({RangeDef(0, 100, "keep"), RangeDef(300, 1024, "delete")}, 1024, 100, 1024, 0);
    // xxxx
    testChunk({}, 1024, 0, 0, 0);
    // xxxx1111
    testChunk({RangeDef(100, 1024, "keep")}, 1024, 1024, 1024, 100);
    // xxxx11110000
    testChunk({RangeDef(100, 300, "keep"), RangeDef(300, 1024, "delete")}, 1024, 300, 1024, 100);
    // xxxx0000
    testChunk({RangeDef(300, 1024, "delete")}, 1024, 0, 0, 0);
    // xxxx00001111
    testChunk({RangeDef(100, 300, "delete"), RangeDef(300, 1024, "keep")}, 1024, 1024, 1024, 300);
    // 0000
    testChunk({RangeDef(0, 1024, "delete")}, 1024, 0, 0, 0);
    // 00001111
    testChunk({RangeDef(0, 300, "delete"), RangeDef(300, 1024, "keep")}, 1024, 1024, 1024, 300);
    // 00001111xxxx
    testChunk({RangeDef(0, 300, "delete"), RangeDef(300, 600, "keep")}, 1024, 600, 1024, 300);
    // 0000xxxx
    testChunk({RangeDef(0, 300, "delete")}, 1024, 0, 0, 0);
    // 0000xxxx1111
    testChunk({RangeDef(0, 300, "delete"), RangeDef(600, 1024, "keep")}, 1024, 1024, 1024, 600);
}

TEST_F(MatchDocAllocatorCompactTest, testCompact2ChunkBruteForce) {
    std::vector<std::vector<RangeDef>> defs = {{RangeDef(0, 1024, "keep")},
                                               {RangeDef(0, 100, "keep"), RangeDef(100, 1024, "delete")},
                                               {RangeDef(0, 100, "keep"), RangeDef(100, 300, "delete")},
                                               {RangeDef(0, 100, "keep")},
                                               {RangeDef(0, 100, "keep"), RangeDef(300, 1024, "delete")},
                                               {},
                                               {RangeDef(100, 1024, "keep")},
                                               {RangeDef(100, 300, "keep"), RangeDef(300, 1024, "delete")},
                                               {RangeDef(300, 1024, "delete")},
                                               {RangeDef(100, 300, "delete"), RangeDef(300, 1024, "keep")},
                                               {RangeDef(0, 1024, "delete")},
                                               {RangeDef(0, 300, "delete"), RangeDef(300, 1024, "keep")},
                                               {RangeDef(0, 300, "delete"), RangeDef(300, 600, "keep")},
                                               {RangeDef(0, 300, "delete")},
                                               {RangeDef(0, 300, "delete"), RangeDef(600, 1024, "keep")}};
    for (size_t i = 0; i < defs.size(); i++) {
        for (size_t j = 0; j < defs.size(); j++) {
            auto chunk1 = defs[i];
            auto chunk2 = defs[j];
            for (auto &def : chunk2) {
                def.begin += 1024;
                def.end += 1024;
            }
            chunk1.insert(chunk1.end(), chunk2.begin(), chunk2.end());
            testChunk(chunk1, 2048);
        }
    }
}

TEST_F(MatchDocAllocatorCompactTest, testCompactRemoveChunk) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    allocator.extend();
    std::vector<MatchDoc> docs;
    size_t redundantCount = 100;
    for (size_t i = 0; i < VectorStorage::CHUNKSIZE + redundantCount; i++) {
        auto doc = allocator.allocate();
        field1->set(doc, i);
        if (i >= redundantCount) {
            docs.push_back(doc);
        }
    }

    allocator.compact(docs);

    for (const auto &pair : allocator._fieldGroups) {
        EXPECT_EQ(allocator._capacity, VectorStorage::CHUNKSIZE * pair.second->_storage->_chunks.size());
    }
    for (size_t i = redundantCount; i < VectorStorage::CHUNKSIZE; i++) {
        EXPECT_EQ(i, docs[i - redundantCount].getIndex());
        EXPECT_EQ(i, field1->get(docs[i - redundantCount]));
    }
    for (size_t i = 0; i < redundantCount; i++) {
        size_t index = VectorStorage::CHUNKSIZE - redundantCount + i;
        EXPECT_EQ(i, docs[index].getIndex());
        EXPECT_EQ(index + redundantCount, field1->get(docs[index]));
    }
    for (const auto &pair : allocator._fieldGroups) {
        EXPECT_EQ(allocator._capacity, VectorStorage::CHUNKSIZE * pair.second->_storage->_chunks.size());
    }
    std::vector<MatchDoc> emptyDocs;
    EXPECT_EQ(0, allocator._deletedIds.size());
    allocator.compact(emptyDocs);
    EXPECT_EQ(0, allocator._size);
    EXPECT_EQ(0, allocator._deletedIds.size());
    EXPECT_EQ(0, allocator._capacity);
    for (const auto &pair : allocator._fieldGroups) {
        EXPECT_EQ(allocator._capacity, VectorStorage::CHUNKSIZE * pair.second->_storage->_chunks.size());
    }
}

TEST_F(MatchDocAllocatorCompactTest, testCompactRandomHalf) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    allocator.extend();
    std::vector<MatchDoc> docs;
    size_t docCount = 10000;
    size_t remainCount = 5000;
    for (size_t i = 0; i < docCount; i++) {
        auto doc = allocator.allocate();
        doc.setDocId(i);
        field1->set(doc, i);
        docs.push_back(doc);
    }
    random_shuffle(docs.begin(), docs.end());
    docs.resize(remainCount);
    allocator.compact(docs);
    for (auto doc : docs) {
        EXPECT_EQ(doc.getDocId(), field1->get(doc));
    }
    EXPECT_EQ(VectorStorage::getAlignSize(remainCount), allocator._capacity);
    for (const auto &pair : allocator._fieldGroups) {
        EXPECT_EQ(allocator._capacity, VectorStorage::CHUNKSIZE * pair.second->_storage->_chunks.size());
    }
}

TEST_F(MatchDocAllocatorCompactTest, testCompactRandomSparse) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    allocator.extend();
    std::vector<MatchDoc> docs;
    size_t docCount = 100000;
    size_t remainCount = 50;
    for (size_t i = 0; i < docCount; i++) {
        auto doc = allocator.allocate();
        doc.setDocId(i);
        field1->set(doc, i);
        docs.push_back(doc);
    }
    random_shuffle(docs.begin(), docs.end());
    docs.resize(remainCount);
    allocator.compact(docs);
    for (auto doc : docs) {
        EXPECT_EQ(doc.getDocId(), field1->get(doc));
    }
    EXPECT_EQ(VectorStorage::getAlignSize(remainCount), allocator._capacity);
    for (const auto &pair : allocator._fieldGroups) {
        EXPECT_EQ(allocator._capacity, VectorStorage::CHUNKSIZE * pair.second->_storage->_chunks.size());
    }
}

TEST_F(MatchDocAllocatorCompactTest, testCompactRandomDense) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    allocator.extend();
    std::vector<MatchDoc> docs;
    size_t docCount = 10000;
    size_t remainCount = 9990;
    for (size_t i = 0; i < docCount; i++) {
        auto doc = allocator.allocate();
        doc.setDocId(i);
        field1->set(doc, i);
        docs.push_back(doc);
    }
    random_shuffle(docs.begin(), docs.end());
    docs.resize(remainCount);
    allocator.compact(docs);
    for (auto doc : docs) {
        EXPECT_EQ(doc.getDocId(), field1->get(doc));
    }
    EXPECT_EQ(VectorStorage::getAlignSize(remainCount), allocator._capacity);
    for (const auto &pair : allocator._fieldGroups) {
        EXPECT_EQ(allocator._capacity, VectorStorage::CHUNKSIZE * pair.second->_storage->_chunks.size());
    }
}

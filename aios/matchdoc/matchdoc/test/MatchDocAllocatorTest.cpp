#include "matchdoc/MatchDocAllocator.h"

#include "gtest/gtest.h"
#include <cmath>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <stdio.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "FiveBytesString.h"
#include "autil/DataBuffer.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/FieldGroup.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorStorage.h"
#include "matchdoc/toolkit/MatchDocTestHelper.h"

using namespace std;
using namespace autil;
using namespace matchdoc;

class UserDefType1 {
public:
    void serialize(DataBuffer &dataBuffer) const {
        dataBuffer.write(id);
        dataBuffer.write(str);
    }
    void deserialize(DataBuffer &dataBuffer) {
        dataBuffer.read(id);
        dataBuffer.read(str);
    }
    int id;
    std::string str;
};

class MatchDocAllocatorTest : public testing::Test {
public:
    MatchDocAllocatorTest() : pool(std::make_shared<autil::mem_pool::Pool>()), allocator(pool) {}

    void SetUp() override {
        ReferenceTypesWrapper::getInstance()->registerType<FiveBytesString>();
        ReferenceTypesWrapper::getInstance()->registerType<UserDefType1>();
        ReferenceTypesWrapper::getInstance()->registerType<std::vector<std::vector<int64_t>>>();
    }

    void TearDown() override {
        ReferenceTypesWrapper::getInstance()->unregisterType<FiveBytesString>();
        ReferenceTypesWrapper::getInstance()->unregisterType<UserDefType1>();
        ReferenceTypesWrapper::getInstance()->unregisterType<std::vector<std::vector<int64_t>>>();
    }

    class SubCollector {
    public:
        SubCollector(const matchdoc::Reference<matchdoc::MatchDoc> *current,
                     std::map<int32_t, std::vector<int32_t>> &docs)
            : _current(current), _docs(docs) {}

    public:
        void operator()(matchdoc::MatchDoc doc) {
            matchdoc::MatchDoc subDoc = _current->get(doc);
            std::vector<int32_t> &subdocs = _docs[doc.getDocId()];
            subdocs.push_back(subDoc.getDocId());
        }

    private:
        const matchdoc::Reference<matchdoc::MatchDoc> *_current;
        std::map<int32_t, std::vector<int32_t>> &_docs;
    };

protected:
    FieldGroup *findGroup(MatchDocAllocator &allocator, const std::string &name) const {
        auto it = allocator._fieldGroups.find(name);
        return it == allocator._fieldGroups.end() ? nullptr : it->second;
    }

protected:
    std::shared_ptr<autil::mem_pool::Pool> pool;
    MatchDocAllocator allocator;
};

class FakeDocIdIterator {
public:
    FakeDocIdIterator(int32_t docid, std::vector<int32_t> &subdocIds)
        : _docid(docid), _subdocIds(subdocIds), _it(_subdocIds.begin()) {}

public:
    int32_t getCurDocId() { return _docid; }
    bool beginSubDoc() { return true; }
    int32_t nextSubDocId() {
        if (_it != _subdocIds.end()) {
            return *_it++;
        }
        return -1;
    }

private:
    int32_t _docid;
    std::vector<int32_t> _subdocIds;
    std::vector<int32_t>::iterator _it;
};

TEST_F(MatchDocAllocatorTest, testInt64Convert) {
    auto doc = allocator.allocate(0);
    int64_t i = doc.toInt64();
    MatchDoc doc1;
    doc1.fromInt64(i);
    EXPECT_TRUE(doc1 == doc);
    EXPECT_TRUE(doc1.getDocId() == doc.getDocId());
}

TEST_F(MatchDocAllocatorTest, testMatchDocEqual) {
    MatchDoc doc1;
    EXPECT_EQ(INVALID_MATCHDOC, doc1);
    doc1.setDeleted();
    EXPECT_EQ(INVALID_MATCHDOC, doc1);

    MatchDoc doc2(30);
    EXPECT_TRUE(doc1 != doc2);
    MatchDoc doc3(30);
    EXPECT_TRUE(doc3 == doc2);
    doc3.setDeleted();
    EXPECT_TRUE(doc3 == doc2);
    doc2.setDeleted();
    EXPECT_TRUE(doc3 == doc2);

    MatchDoc doc4(40);
    doc4.setDeleted();
    EXPECT_FALSE(doc3 == doc4);
}

TEST_F(MatchDocAllocatorTest, testClean) {
    Reference<int> *field1 = allocator.declare<int>("field1", "group1");
    Reference<int64_t> *field2 = allocator.declare<int64_t>("field2", "group2");
    allocator.extend();

    Reference<int64_t> *toExtendRef = allocator.declare<int64_t>("field3", "group3");
    EXPECT_EQ(12u, allocator.getDocSize());
    MatchDoc doc1 = allocator.allocate(0);
    ASSERT_EQ(0u, doc1.index);

    // clean allocator
    unordered_set<string> keepFieldGroups;
    keepFieldGroups.insert("group2");
    allocator.clean(keepFieldGroups);
    EXPECT_EQ(8u, allocator.getDocSize());

    // group1 group3 cleaned, group2 keep
    field1 = allocator.findReference<int>("field1");
    ASSERT_FALSE(field1);
    toExtendRef = allocator.findReference<int64_t>("field3");
    ASSERT_FALSE(toExtendRef);
    field2 = allocator.findReference<int64_t>("field2");
    ASSERT_TRUE(field2);

    // group2 not extend group
    Reference<int64_t> *field3 = allocator.declare<int64_t>("field3", "group2");
    ASSERT_FALSE(field3);

    // redecare group1
    field1 = allocator.declare<int>("field1", "group1");
    ASSERT_TRUE(field1);
    allocator.extend();

    EXPECT_EQ(12u, allocator.getDocSize());
    doc1 = allocator.allocate(0);
    ASSERT_EQ(0u, doc1.index);

    // default group name
    Reference<int> *field4 = allocator.declare<int>("field4");
    ASSERT_TRUE(field4);
    allocator.extend();

    Reference<int> *field5 = allocator.declare<int>("field5");
    ASSERT_TRUE(field5);
    allocator.extend();

    EXPECT_EQ(1u, allocator._defaultGroupNameCounter);
    allocator.clean(keepFieldGroups);
    EXPECT_EQ(0u, allocator._defaultGroupNameCounter);
}

TEST_F(MatchDocAllocatorTest, testSimpleProcess) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);

    EXPECT_EQ(0u, allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());

    allocator.extend();

    EXPECT_EQ(sizeof(string) + sizeof(int), allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());

    MatchDoc doc1 = allocator.allocate();
    field1->set(doc1, 1);
    field2->set(doc1, "1");
    MatchDoc doc2 = allocator.allocate();
    field1->set(doc2, 2);
    field2->set(doc2, "2");
    MatchDoc doc3 = allocator.allocate();
    field1->set(doc3, 3);
    field2->set(doc3, "3");

    EXPECT_EQ(1, field1->get(doc1));
    EXPECT_EQ(2, field1->get(doc2));
    EXPECT_EQ(3, field1->get(doc3));
    EXPECT_EQ(string("1"), field2->get(doc1));
    EXPECT_EQ(string("2"), field2->get(doc2));
    EXPECT_EQ(string("3"), field2->get(doc3));

    Reference<int> *field3 = allocator.declare<int>("field3", "default2");
    ASSERT_TRUE(field3);
    Reference<string> *field4 = allocator.declare<string>("field4", "default3");
    ASSERT_TRUE(field4);
    allocator.extend();

    field3->set(doc3, 33);
    field4->set(doc3, "34");
    EXPECT_EQ(1, field1->get(doc1));
    EXPECT_EQ(2, field1->get(doc2));
    EXPECT_EQ(3, field1->get(doc3));
    EXPECT_EQ(string("1"), field2->get(doc1));
    EXPECT_EQ(string("2"), field2->get(doc2));
    EXPECT_EQ(string("3"), field2->get(doc3));
    EXPECT_EQ(33, field3->get(doc3));
    EXPECT_EQ(string("34"), field4->get(doc3));

    allocator.allocate();
    MatchDoc doc5 = allocator.allocate();

    field1->set(doc5, 5);
    field2->set(doc5, "5");
    field3->set(doc5, 5);
    field4->set(doc5, "5");
    EXPECT_EQ(5, field1->get(doc5));
    EXPECT_EQ(string("5"), field2->get(doc5));
    EXPECT_EQ(5, field3->get(doc5));
    EXPECT_EQ(string("5"), field4->get(doc5));
}

TEST_F(MatchDocAllocatorTest, testBatchAllocate) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);

    EXPECT_EQ(0u, allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());
    int32_t docCount = 100000;
    vector<int32_t> docIds;
    for (int32_t i = 0; i < docCount; i++) {
        docIds.push_back(i);
    }
    vector<MatchDoc> matchDocs = allocator.batchAllocate(docIds);

    EXPECT_EQ(sizeof(string) + sizeof(int), allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());
    EXPECT_EQ(docCount, matchDocs.size());
    for (size_t i = 0; i < docIds.size(); i++) {
        ASSERT_EQ(docIds[i], matchDocs[i].getDocId());
        EXPECT_EQ(string(""), field2->get(matchDocs[i]));
    }
    ASSERT_EQ(0, allocator._deletedIds.size());
    allocator.deallocate(matchDocs.data(), matchDocs.size());
    ASSERT_EQ(matchDocs.size(), allocator._deletedIds.size());

    docCount = 90000;
    docIds.clear();
    for (int32_t i = 0; i < docCount; i++) {
        docIds.push_back(i);
    }
    matchDocs = allocator.batchAllocate(docIds);
    EXPECT_EQ(docCount, matchDocs.size());
    for (size_t i = 0; i < docIds.size(); i++) {
        ASSERT_EQ(docIds[i], matchDocs[i].getDocId());
        EXPECT_EQ(string(""), field2->get(matchDocs[i]));
    }
    ASSERT_EQ(10000, allocator._deletedIds.size());

    docCount = 20000;
    docIds.clear();
    for (int32_t i = 0; i < docCount; i++) {
        docIds.push_back(i);
    }
    matchDocs = allocator.batchAllocate(docIds);
    EXPECT_EQ(docCount, matchDocs.size());
    for (size_t i = 0; i < docIds.size(); i++) {
        ASSERT_EQ(docIds[i], matchDocs[i].getDocId());
        EXPECT_EQ(string(""), field2->get(matchDocs[i]));
    }
    ASSERT_EQ(0, allocator._deletedIds.size());
}

TEST_F(MatchDocAllocatorTest, testBatchAllocateWillNotConstruct) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);

    EXPECT_EQ(0u, allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());
    int32_t docCount = 100;
    vector<int32_t> docIds;
    for (int32_t i = 0; i < docCount; i++) {
        docIds.push_back(i);
    }
    vector<MatchDoc> matchDocs = allocator.batchAllocate(docIds, true);

    EXPECT_EQ(sizeof(string) + sizeof(int), allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());
    EXPECT_EQ(docCount, matchDocs.size());
    for (size_t i = 0; i < docIds.size(); i++) {
        ASSERT_EQ(docIds[i], matchDocs[i].getDocId());
        EXPECT_EQ(0, field1->get(matchDocs[i]));
        EXPECT_EQ(string(""), field2->get(matchDocs[i]));
    }

    ASSERT_EQ(0, allocator._deletedIds.size());
    allocator.deallocate(matchDocs.data(), matchDocs.size());
    ASSERT_EQ(matchDocs.size(), allocator._deletedIds.size());
}

TEST_F(MatchDocAllocatorTest, testPoolPtrConstructor) {
    {
        std::shared_ptr<autil::mem_pool::Pool> poolPtr(new autil::mem_pool::Pool());
        EXPECT_EQ(1, poolPtr.use_count());
        MatchDocAllocator allocator(poolPtr);
        EXPECT_EQ(poolPtr, allocator._poolPtr);
        EXPECT_EQ(poolPtr.get(), allocator.getSessionPool());
        EXPECT_FALSE(allocator._subAllocator);
        EXPECT_FALSE(allocator._mountInfo);
        EXPECT_EQ(2, poolPtr.use_count());

        Reference<int> *field1 = allocator.declare<int>("field1");
        allocator.extend();
        EXPECT_EQ(3, poolPtr.use_count());
        EXPECT_EQ(poolPtr.get(), field1->_docStorage->_pool.get());

        allocator.dropField("field1");
        EXPECT_EQ(2, poolPtr.use_count());
    }
}

TEST_F(MatchDocAllocatorTest, testMultiChunk) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);
    allocator.extend();
    ASSERT_EQ(0, field1->_docStorage->_chunks.size());

    vector<MatchDoc> matchdocs;
    for (int i = 0; i < 40960; i++) {
        matchdocs.push_back(allocator.allocate());
    }
    for (int i = 0; i < 40960; i++) {
        field1->set(matchdocs[i], i);
        field2->set(matchdocs[i], autil::StringUtil::toString(i));
    }
    for (int i = 0; i < 40960; i++) {
        EXPECT_EQ(i, field1->get(matchdocs[i]));
        EXPECT_EQ(autil::StringUtil::toString(i), field2->get(matchdocs[i]));
    }
    ASSERT_EQ(ceil(40960 / VectorStorage::CHUNKSIZE), field1->_docStorage->_chunks.size());
}

TEST_F(MatchDocAllocatorTest, testCreateSingleFieldNoDoc) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);
    allocator.extend();
    vector<MatchDoc> matchdocs;
    for (int i = 0; i < 1000; i++) {
        matchdocs.push_back(allocator.allocate());
    }
    for (int i = 0; i < 1000; i++) {
        field1->set(matchdocs[i], i);
        field2->set(matchdocs[i], autil::StringUtil::toString(i));
    }
    MatchDocAllocator allocator2(pool);
    Reference<int> *tfield1 = allocator2.declare<int>("field1");
    ASSERT_TRUE(tfield1);
    Reference<string> *tfield2 = allocator2.declare<string>("field2");
    ASSERT_TRUE(tfield2);
    allocator.extend();
    vector<MatchDoc> matchdocs2;
    bool ret = allocator2.mergeAllocator(&allocator, matchdocs, matchdocs2);
    ASSERT_TRUE(ret);
}

TEST_F(MatchDocAllocatorTest, testCreateSingleFieldCase1) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);
    allocator.extend();
    vector<MatchDoc> matchdocs;
    for (int i = 0; i < 1000; i++) {
        matchdocs.push_back(allocator.allocate());
    }
    for (int i = 0; i < 1000; i++) {
        field1->set(matchdocs[i], i);
        field2->set(matchdocs[i], autil::StringUtil::toString(i));
    }
    MatchDocAllocator allocator2(pool);
    Reference<int> *tfield1 = allocator2.declare<int>("field1");
    ASSERT_TRUE(tfield1);
    Reference<string> *tfield2 = allocator2.declare<string>("field2");
    ASSERT_TRUE(tfield2);
    allocator.extend();
    vector<MatchDoc> matchdocs2;
    bool ret = allocator2.mergeAllocator(&allocator, matchdocs, matchdocs2);
    ASSERT_TRUE(ret);
}

TEST_F(MatchDocAllocatorTest, testCreateTwoDimVector) {
    auto *field1 = allocator.declare<vector<vector<int64_t>>>("field1");
    ASSERT_TRUE(field1);
    allocator.extend();
    vector<MatchDoc> matchdocs;
    for (int i = 0; i < 1; i++) {
        matchdocs.push_back(allocator.allocate());
    }
    for (int i = 0; i < 1; i++) {
        vector<vector<int64_t>> test = {{1, 2, 3}, {4, 5, 6}};
        field1->set(matchdocs[i], test);
    }
    ASSERT_EQ(autil::StringUtil::toString(field1->get(matchdocs[0]), ";"), "1 2 3;4 5 6");
}

TEST_F(MatchDocAllocatorTest, testAddGroupFromBuffer) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);
    allocator.extend();

    vector<MatchDoc> matchdocs;
    for (int i = 0; i < 20000; i++) {
        matchdocs.push_back(allocator.allocate());
    }
    for (int i = 0; i < 20000; i++) {
        field1->set(matchdocs[i], i);
        field2->set(matchdocs[i], autil::StringUtil::toString(i));
    }
    EXPECT_EQ(field1->_docStorage, field2->_docStorage);

    EXPECT_EQ(20000 / VectorStorage::CHUNKSIZE + 1, field1->_docStorage->_chunks.size());
    vector<double> dVec;
    for (int i = 0; i < 20000; i++) {
        dVec.push_back(i * 1.0);
    }
    auto builder = std::make_unique<FieldGroupBuilder>("field3", pool);
    auto group = builder->fromBuffer(dVec.data(), dVec.size());
    ASSERT_TRUE(group);
    ASSERT_TRUE(allocator.addGroup(std::move(group), true, false));
    Reference<double> *field3 = allocator.findReference<double>("field3");
    ASSERT_TRUE(field3);

    EXPECT_EQ(20000 / VectorStorage::CHUNKSIZE + 1, field3->_docStorage->_chunks.size());

    for (int i = 0; i < 20000; i++) {
        EXPECT_EQ(i, field1->get(matchdocs[i]));
        EXPECT_EQ(autil::StringUtil::toString(i), field2->get(matchdocs[i]));
        ASSERT_EQ(i * 1.0, field3->get(matchdocs[i]));
    }
    for (int i = 20000; i < 30000; i++) {
        matchdocs.push_back(allocator.allocate());
    }
    for (int i = 20000; i < 30000; i++) {
        field1->set(matchdocs[i], i);
        field2->set(matchdocs[i], autil::StringUtil::toString(i));
        field3->set(matchdocs[i], i * 1.0);
    }
    for (int i = 0; i < 30000; i++) {
        EXPECT_EQ(i, field1->get(matchdocs[i]));
        EXPECT_EQ(autil::StringUtil::toString(i), field2->get(matchdocs[i]));
        EXPECT_EQ(i * 1.0, field3->get(matchdocs[i]));
    }

    MatchDocAllocator allocator2(pool);
    Reference<int> *tfield1 = allocator2.declare<int>("field1");
    ASSERT_TRUE(tfield1);
    Reference<string> *tfield2 = allocator2.declare<string>("field2");
    ASSERT_TRUE(tfield2);
    Reference<double> *tfield3 = allocator2.declare<double>("field3");
    ASSERT_TRUE(tfield3);
    allocator.extend();

    vector<MatchDoc> matchdocs2;
    for (int i = 0; i < 10000; i++) {
        matchdocs2.push_back(allocator2.allocate());
    }
    for (int i = 0; i < 10000; i++) {
        tfield1->set(matchdocs2[i], i);
        tfield2->set(matchdocs2[i], autil::StringUtil::toString(i));
        tfield3->set(matchdocs2[i], i * 1.0);
    }
    for (int i = 0; i < 10000; i++) {
        EXPECT_EQ(i, tfield1->get(matchdocs2[i]));
        EXPECT_EQ(autil::StringUtil::toString(i), tfield2->get(matchdocs2[i]));
        EXPECT_EQ(i * 1.0, tfield3->get(matchdocs2[i]));
    }
    bool ret = allocator2.mergeAllocator(&allocator, matchdocs, matchdocs2);
    ASSERT_TRUE(ret);
    EXPECT_EQ(matchdocs2.size(), 40000);
    EXPECT_EQ(allocator2.getAllocatedCount(), 40000);
    for (int i = 0; i < 10000; i++) {
        EXPECT_EQ(i, tfield1->get(matchdocs2[i]));
        EXPECT_EQ(autil::StringUtil::toString(i), tfield2->get(matchdocs2[i]));
        EXPECT_EQ(i * 1.0, tfield3->get(matchdocs2[i]));
    }
    for (int j = 10000; j < 40000; j++) {
        int i = j - 10000;
        EXPECT_EQ(i, tfield1->get(matchdocs2[j]));
        EXPECT_EQ(autil::StringUtil::toString(i), tfield2->get(matchdocs2[j]));
        EXPECT_EQ(i * 1.0, tfield3->get(matchdocs2[j]));
    }
}

TEST_F(MatchDocAllocatorTest, testFindReference) {
    ASSERT_EQ(NULL, allocator.findReference<int>("field1"));
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    // find success
    ASSERT_EQ(field1, allocator.findReference<int>("field1"));
    // find wrong type
    ASSERT_FALSE(allocator.findReference<string>("field1"));
}

TEST_F(MatchDocAllocatorTest, testDropFieldDeclare) {
    allocator.dropField("field1");
    allocator.declare<std::string>("field1");
    ASSERT_TRUE(allocator._fieldGroups.empty());
    ASSERT_EQ(1u, allocator._fieldGroupBuilders.size());
    ASSERT_EQ(1u, allocator._referenceMap.size());
    ASSERT_TRUE(allocator.findReferenceWithoutType("field1"));

    allocator.dropField("field1");
    allocator.dropField("not_exist");

    ASSERT_FALSE(allocator.findReferenceWithoutType("field1"));
    ASSERT_TRUE(allocator._fieldGroups.empty());
    ASSERT_TRUE(allocator._fieldGroupBuilders.empty());
    ASSERT_TRUE(allocator._referenceMap.empty());
}

TEST_F(MatchDocAllocatorTest, testDropFieldDeclareSub) {
    allocator.declare<std::string>("mainField");
    allocator.declareSub<std::string>("subField");
    allocator.extend();
    allocator.extendSub();
    allocator.declare<std::string>("mainField1");
    const auto &subAllocator = allocator._subAllocator;
    ASSERT_TRUE(subAllocator);
    ASSERT_TRUE(allocator.findReferenceWithoutType("mainField"));
    ASSERT_TRUE(allocator.findReferenceWithoutType("subField"));

    ASSERT_EQ(2u, allocator._fieldGroups.size());
    ASSERT_EQ(1u, allocator._fieldGroupBuilders.size());
    ASSERT_EQ(4u, allocator._referenceMap.size());

    ASSERT_EQ(1u, subAllocator->_fieldGroups.size());
    ASSERT_EQ(0u, subAllocator->_fieldGroupBuilders.size());
    ASSERT_EQ(2u, subAllocator->_referenceMap.size());

    allocator.dropField("mainField");
    allocator.dropField("not_exist");
    ASSERT_FALSE(allocator.findReferenceWithoutType("mainField"));
    ASSERT_TRUE(allocator.findReferenceWithoutType("subField"));

    ASSERT_EQ(1u, allocator._fieldGroups.size());
    ASSERT_EQ(1u, allocator._fieldGroupBuilders.size());
    ASSERT_EQ(3u, allocator._referenceMap.size());

    ASSERT_EQ(1u, subAllocator->_fieldGroups.size());
    ASSERT_EQ(0u, subAllocator->_fieldGroupBuilders.size());
    ASSERT_EQ(2u, subAllocator->_referenceMap.size());

    allocator.dropField("subField");

    ASSERT_EQ(1u, allocator._fieldGroups.size());
    ASSERT_EQ(1u, allocator._fieldGroupBuilders.size());
    ASSERT_EQ(3u, allocator._referenceMap.size());

    ASSERT_EQ(1u, subAllocator->_fieldGroups.size());
    ASSERT_EQ(0u, subAllocator->_fieldGroupBuilders.size());
    ASSERT_EQ(1u, subAllocator->_referenceMap.size());
}

TEST_F(MatchDocAllocatorTest, testDropFieldAllocate) {
    DataBuffer dataBuffer;
    size_t count = 0;
    {
        allocator.declare<int32_t>("int1");
        allocator.declare<std::string>("field1");
        auto ref2 = allocator.declare<std::string>("field2");
        allocator.declareSub<std::string>("sub_field1");
        auto subRef2 = allocator.declareSub<std::string>("sub_field2");
        vector<MatchDoc> docs;
        for (size_t i = 0; i < 1000; i++) {
            auto doc = allocator.allocate(i);
            ref2->set(doc, "field2_" + StringUtil::toString(i));
            for (size_t j = 0; j * j < i; j++) {
                allocator.allocateSub(doc, j);
                subRef2->set(doc, "sub_field2_" + StringUtil::toString(j));
            }
            docs.push_back(doc);
        }
        vector<MatchDoc> newDocs;
        size_t deleteCount = 0;
        for (size_t i = 0; i < docs.size(); i++) {
            if (i % 7 == 0) {
                allocator.deallocate(docs[i]);
                deleteCount++;
            } else {
                newDocs.push_back(docs[i]);
            }
        }
        const auto &subAllocator = allocator._subAllocator;
        ASSERT_EQ(2u, allocator._fieldGroups.size());
        ASSERT_EQ(0u, allocator._fieldGroupBuilders.size());
        ASSERT_EQ(5u, allocator._referenceMap.size());
        ASSERT_EQ(deleteCount, allocator._deletedIds.size());
        ASSERT_EQ(1u, subAllocator->_fieldGroups.size());
        ASSERT_EQ(0u, subAllocator->_fieldGroupBuilders.size());
        ASSERT_EQ(3u, subAllocator->_referenceMap.size());

        ASSERT_TRUE(allocator.findReferenceWithoutType("field1"));
        ASSERT_TRUE(allocator.findReferenceWithoutType("field2"));
        ASSERT_TRUE(allocator.findReferenceWithoutType("sub_field1"));
        ASSERT_TRUE(allocator.findReferenceWithoutType("sub_field2"));
        allocator.dropField("int1");
        allocator.dropField("field1");
        allocator.dropField("sub_field1");
        allocator.dropField("not_exist");
        ASSERT_FALSE(allocator.findReferenceWithoutType("field1"));
        ASSERT_TRUE(allocator.findReferenceWithoutType("field2"));
        ASSERT_FALSE(allocator.findReferenceWithoutType("sub_field1"));
        ASSERT_TRUE(allocator.findReferenceWithoutType("sub_field2"));

        ASSERT_EQ(2u, allocator._fieldGroups.size());
        ASSERT_EQ(0u, allocator._fieldGroupBuilders.size());
        ASSERT_EQ(3u, allocator._referenceMap.size());
        ASSERT_EQ(deleteCount, allocator._deletedIds.size());
        ASSERT_EQ(1u, subAllocator->_fieldGroups.size());
        ASSERT_EQ(0u, subAllocator->_fieldGroupBuilders.size());
        ASSERT_EQ(2u, subAllocator->_referenceMap.size());

        size_t begin = docs.size();
        for (size_t i = begin; i < begin + 1000; i++) {
            auto doc = allocator.allocate(i);
            ref2->set(doc, "field2_" + StringUtil::toString(i));
            for (size_t j = 0; j * j < i; j++) {
                allocator.allocateSub(doc, j * j);
                subRef2->set(doc, "sub_field2_" + StringUtil::toString(j * j));
            }
            newDocs.push_back(doc);
        }
        count = newDocs.size();
        allocator.serialize(dataBuffer, newDocs, 0);
    }
    MatchDocAllocator otherAllocator(pool);
    vector<MatchDoc> otherDocs;
    otherAllocator.deserialize(dataBuffer, otherDocs);
    ASSERT_EQ(count, otherDocs.size());
    ASSERT_FALSE(otherAllocator.findReferenceWithoutType("field1"));
    ASSERT_TRUE(otherAllocator.findReferenceWithoutType("field2"));
    ASSERT_FALSE(otherAllocator.findReferenceWithoutType("sub_field1"));
    ASSERT_TRUE(otherAllocator.findReferenceWithoutType("sub_field2"));

    const auto &otherSubAllocator = otherAllocator._subAllocator;
    ASSERT_EQ(2u, otherAllocator._fieldGroups.size());
    ASSERT_EQ(0u, otherAllocator._fieldGroupBuilders.size());
    ASSERT_EQ(3u, otherAllocator._referenceMap.size());
    ASSERT_EQ(1u, otherSubAllocator->_fieldGroups.size());
    ASSERT_EQ(0u, otherSubAllocator->_fieldGroupBuilders.size());
    ASSERT_EQ(2u, otherSubAllocator->_referenceMap.size());

    auto newRef2 = otherAllocator.findReference<std::string>("field2");
    auto newSubRef2 = otherAllocator.findReference<std::string>("sub_field2");
    auto accessor = otherAllocator.getSubDocAccessor();
    ASSERT_TRUE(newRef2);
    for (size_t i = 0; i < otherDocs.size(); i++) {
        ASSERT_EQ("field2_" + StringUtil::toString(otherDocs[i].getDocId()), newRef2->get(otherDocs[i]));
        std::vector<int32_t> subDocids;
        vector<std::string> values;
        accessor->getSubDocIds(otherDocs[i], subDocids);
        accessor->getSubDocValues(otherDocs[i], newSubRef2, values);
        for (size_t j = 0; j < values.size(); j++) {
            ASSERT_EQ("sub_field2_" + StringUtil::toString(subDocids[j]), values[j]);
        }
    }
}

TEST_F(MatchDocAllocatorTest, testRenameFieldDeclare) {
    ASSERT_FALSE(allocator.renameField("field1", "field2"));
    allocator.declare<std::string>("field1");
    ASSERT_TRUE(allocator._fieldGroups.empty());
    ASSERT_EQ(1u, allocator._fieldGroupBuilders.size());
    ASSERT_EQ(1u, allocator._referenceMap.size());
    ASSERT_TRUE(allocator.findReferenceWithoutType("field1"));

    allocator.renameField("field1", "field2");
    allocator.renameField("not_exist", "not_exist1");

    ASSERT_FALSE(allocator.findReferenceWithoutType("field1"));
    ASSERT_TRUE(allocator._fieldGroups.empty());
    ASSERT_EQ(1u, allocator._fieldGroupBuilders.size());
    ASSERT_EQ(1u, allocator._referenceMap.size());
    ASSERT_TRUE(allocator.findReferenceWithoutType("field2"));
}

TEST_F(MatchDocAllocatorTest, testRenameFieldDeclareSub) {
    auto mainField = allocator.declare<std::string>("mainField");
    auto subField = allocator.declareSub<std::string>("subField");
    allocator.extend();
    allocator.extendSub();
    auto mainField1 = allocator.declare<std::string>("mainField1");
    const auto &subAllocator = allocator._subAllocator;
    ASSERT_TRUE(subAllocator);
    ASSERT_TRUE(allocator.findReferenceWithoutType("mainField"));
    ASSERT_TRUE(allocator.findReferenceWithoutType("subField"));

    ASSERT_EQ("mainField", mainField->getName());
    ASSERT_EQ("mainField1", mainField1->getName());
    ASSERT_EQ("subField", subField->getName());

    ASSERT_FALSE(allocator.renameField("mainField", "mainField1"));
    ASSERT_FALSE(allocator.renameField("mainField", "subField"));
    ASSERT_TRUE(allocator.renameField("mainField", "mainField"));
    ASSERT_FALSE(allocator.renameField("subField", "mainField"));
    ASSERT_FALSE(allocator.renameField("subField", "mainField1"));
    ASSERT_TRUE(allocator.renameField("subField", "subField"));

    ASSERT_TRUE(allocator.renameField("mainField", "f"));
    ASSERT_TRUE(allocator.renameField("mainField1", "f1"));
    ASSERT_TRUE(allocator.renameField("subField", "s"));

    ASSERT_EQ(mainField, allocator.findReferenceWithoutType("f"));
    ASSERT_EQ(mainField1, allocator.findReferenceWithoutType("f1"));
    ASSERT_EQ(subField, allocator.findReferenceWithoutType("s"));

    ASSERT_FALSE(allocator.findReferenceWithoutType("mainField"));
    ASSERT_FALSE(allocator.findReferenceWithoutType("mainField1"));
    ASSERT_FALSE(allocator.findReferenceWithoutType("subField"));

    ASSERT_EQ("f", mainField->getName());
    ASSERT_EQ("f1", mainField1->getName());
    ASSERT_EQ("s", subField->getName());
}

TEST_F(MatchDocAllocatorTest, testRenameWithAliasMount) {
    {
        MountInfoPtr mountInfo(new MountInfo());
        ASSERT_TRUE(mountInfo->insert("f1", 0, 0));
        ASSERT_TRUE(mountInfo->insert("f2", 0, 4));
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
        allocator->declare<int32_t>("f1", "g1");
        allocator->declare<int32_t>("f2", "g1");
        allocator->extend();
        allocator->setReferenceAliasMap(
            MatchDocAllocator::ReferenceAliasMapPtr(new MatchDocAllocator::ReferenceAliasMap({{"aa", "f1"}})));
        ASSERT_TRUE(allocator->getMountInfo() == mountInfo);
        ASSERT_TRUE(allocator->renameField("aa", "bb"));
        ASSERT_EQ(2u, allocator->getMountInfo()->getMountMap().size());
        ASSERT_TRUE(allocator->getMountInfo()->get("f1") == nullptr);
        ASSERT_TRUE(allocator->getMountInfo()->get("bb") != nullptr);
        ASSERT_TRUE(allocator->getMountInfo()->get("f2") != nullptr);
    }
}

TEST_F(MatchDocAllocatorTest, testFindReferenceWithoutType) {
    ASSERT_EQ(NULL, allocator.findReference<int>("field1"));

    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    allocator.extend();
    ASSERT_EQ(size_t(1), allocator._fieldGroups.size());
    ASSERT_EQ(size_t(0), allocator._fieldGroupBuilders.size());
    Reference<string> *field2 = allocator.declare<string>("field2", "group1");
    ASSERT_TRUE(field2);
    ASSERT_EQ(size_t(1), allocator._fieldGroups.size());
    ASSERT_EQ(size_t(1), allocator._fieldGroupBuilders.size());

    ASSERT_EQ(field1, allocator.findReference<int>("field1"));
    ASSERT_EQ(field2, allocator.findReference<string>("field2"));
}

TEST_F(MatchDocAllocatorTest, testExtendAndDeallocate) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    ASSERT_EQ(size_t(0), allocator._fieldGroups.size());
    ASSERT_EQ(size_t(1), allocator._fieldGroupBuilders.size());

    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);
    ASSERT_EQ(size_t(0), allocator._fieldGroups.size());
    ASSERT_EQ(size_t(1), allocator._fieldGroupBuilders.size());

    allocator.extend();
    ASSERT_EQ(size_t(1), allocator._fieldGroups.size());
    ASSERT_EQ(size_t(0), allocator._fieldGroupBuilders.size());

    MatchDoc doc1 = allocator.allocate();
    field1->set(doc1, 1);
    field2->set(doc1, "1");
    MatchDoc doc2 = allocator.allocate();
    field1->set(doc2, 2);
    field2->set(doc2, "2");
    MatchDoc doc3 = allocator.allocate();
    field1->set(doc3, 3);
    field2->set(doc3, "3");

    allocator.deallocate(doc2);
    ASSERT_EQ(size_t(1), allocator._deletedIds.size());
    ASSERT_EQ(size_t(doc2.index), allocator._deletedIds[0]);
    allocator.deallocate(doc1);
    allocator.deallocate(doc3);
}

TEST_F(MatchDocAllocatorTest, testAlignSize) {
    ASSERT_TRUE(VectorStorage::getAlignSize(0) == 0);
    ASSERT_TRUE(VectorStorage::getAlignSize(1) == VectorStorage::CHUNKSIZE);
    ASSERT_TRUE(VectorStorage::getAlignSize(1024) == VectorStorage::CHUNKSIZE);
    ASSERT_TRUE(VectorStorage::getAlignSize(1025) == VectorStorage::CHUNKSIZE * 2);
}

TEST_F(MatchDocAllocatorTest, testDeclare) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<int> *field2 = allocator.declare<int>("field1");
    ASSERT_TRUE(field2);

    ASSERT_EQ(field1, field2);
}

TEST_F(MatchDocAllocatorTest, testDeclareSuccess) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    ASSERT_EQ(size_t(1), allocator._fieldGroupBuilders.size());
    ASSERT_TRUE(allocator._fieldGroupBuilders.count("_default_0_") > 0);
    Reference<int> *field2 = allocator.declare<int>("field1");
    ASSERT_TRUE(field2);
    ASSERT_EQ(size_t(1), allocator._fieldGroupBuilders.size());
    ASSERT_TRUE(allocator._fieldGroupBuilders.count("_default_0_") > 0);
}

TEST_F(MatchDocAllocatorTest, testDeclareGroupAllocated) {
    Reference<int> *field1 = allocator.declare<int>("field1", "default");
    ASSERT_TRUE(field1);
    allocator.extend();
    Reference<int> *field2 = allocator.declare<int>("field2", "default");
    ASSERT_FALSE(field2);
    Reference<int> *field3 = allocator.declare<int>("field2", "default");
    ASSERT_EQ(field2, field3);
}

TEST_F(MatchDocAllocatorTest, testDeclareWithSerializeLevel) {
    Reference<int> *field1 = allocator.declare<int>("field1", "default");
    ASSERT_TRUE(field1);
    ASSERT_EQ((uint8_t)0, field1->getSerializeLevel());

    field1 = allocator.declare<int>("field1", "default", 2);
    ASSERT_TRUE(field1);
    ASSERT_EQ((uint8_t)2, field1->getSerializeLevel());
}

TEST_F(MatchDocAllocatorTest, testDeclareWithAllocateSize) {
    Reference<FiveBytesString> *ref1 = allocator.declare<FiveBytesString>("ref1", 0, 5);
    ASSERT_TRUE(ref1);
    allocator.extend();
    EXPECT_EQ((size_t)5, allocator.getDocSize());
    MatchDoc doc1 = allocator.allocate(1);
    MatchDoc doc2 = allocator.allocate(2);
    const char *value1 = "12345";
    const char *value2 = "abcde";
    ref1->set(doc1, *((FiveBytesString *)(value1)));
    ref1->set(doc2, *((FiveBytesString *)(value2)));
    EXPECT_EQ(string("12345"), ref1->getPointer(doc1)->toString());
    EXPECT_EQ(string("abcde"), ref1->getPointer(doc2)->toString());
}

TEST_F(MatchDocAllocatorTest, testDeclareSubWithAllocateSize) {
    Reference<FiveBytesString> *ref1 = allocator.declareSub<FiveBytesString>("ref1", 0, 5);
    ASSERT_TRUE(ref1);
    allocator.extend();
    allocator.extendSub();
    EXPECT_EQ((size_t)5 + sizeof(MatchDoc), allocator.getSubDocSize());
    MatchDoc doc1 = allocator.allocate(1);
    MatchDoc doc2 = allocator.allocate(2);
    allocator.allocateSub(doc1, 1);
    allocator.allocateSub(doc2, 3);
    const char *value1 = "12345";
    const char *value2 = "abcde";
    ref1->set(doc1, *((FiveBytesString *)(value1)));
    ref1->set(doc2, *((FiveBytesString *)(value2)));
    EXPECT_EQ(string("12345"), ref1->getPointer(doc1)->toString());
    EXPECT_EQ(string("abcde"), ref1->getPointer(doc2)->toString());
}

TEST_F(MatchDocAllocatorTest, testSerializeWithAllocateSize) {
    string data;
    {
        Reference<FiveBytesString> *ref1 = allocator.declare<FiveBytesString>("ref1", 0, 5);
        ASSERT_TRUE(ref1);
        EXPECT_EQ((uint32_t)5, ref1->meta.allocateSize);
        allocator.extend();
        vector<MatchDoc> docs;
        docs.push_back(allocator.allocate(1));
        docs.push_back(allocator.allocate(2));
        const char *value1 = "12345";
        const char *value2 = "abcde";
        ref1->set(docs[0], *((FiveBytesString *)(value1)));
        ref1->set(docs[1], *((FiveBytesString *)(value2)));
        DataBuffer dataBuffer;
        allocator.serialize(dataBuffer, docs, 0);
        data.assign(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    {
        DataBuffer dataBuffer((void *)data.c_str(), data.size());
        MatchDocAllocator allocator2(pool);
        vector<MatchDoc> docs;
        allocator2.deserialize(dataBuffer, docs);
        Reference<FiveBytesString> *ref1 = allocator2.findReference<FiveBytesString>("ref1");
        ASSERT_TRUE(ref1);
        EXPECT_EQ((uint32_t)5, ref1->meta.allocateSize);
        EXPECT_EQ((size_t)2, docs.size());
        EXPECT_EQ(string("12345"), ref1->getPointer(docs[0])->toString());
        EXPECT_EQ(string("abcde"), ref1->getPointer(docs[1])->toString());
    }
}

TEST_F(MatchDocAllocatorTest, testSerializeWithLevel) {
    // serialize
    MatchDocAllocatorPtr allocator =
        MatchDocTestHelper::createAllocator("group_1:int_a,group_2:int_b,group_2:int_c,group_3:int_d", pool);
    MatchDocTestHelper::setSerializeLevel(allocator, "int_a:0,int_b:10,int_c:1,int_d:3");

    vector<MatchDoc> docs;
    docs.push_back(MatchDocTestHelper::createMatchDoc(allocator, "0#int_a:1,int_b:2,int_c:3,int_d:4"));
    docs.push_back(MatchDocTestHelper::createMatchDoc(allocator, "1#int_a:5,int_b:6,int_c:7,int_d:8"));

    for (uint8_t sl = 0; sl <= 11; sl++) {
        string data;
        DataBuffer dataBuffer;
        allocator->serialize(dataBuffer, docs, sl);
        data.assign(dataBuffer.getData(), dataBuffer.getDataLen());

        // deserialize
        DataBuffer dataBuffer2((void *)data.c_str(), data.size());
        MatchDocAllocatorPtr allocator2(new MatchDocAllocator(pool));
        vector<MatchDoc> docs;
        allocator2->deserialize(dataBuffer2, docs);

        MatchDocTestHelper::checkSerializeLevel(allocator2, sl, "int_a:0,int_b:10,int_c:1,int_d:3");
        MatchDocTestHelper::checkDeserializedDocValue(allocator2, docs[0], sl, "int_a:1,int_b:2,int_c:3,int_d:4");
        ASSERT_EQ((int32_t)0, docs[0].getDocId());

        MatchDocTestHelper::checkDeserializedDocValue(allocator2, docs[1], sl, "int_a:5,int_b:6,int_c:7,int_d:8");
        ASSERT_EQ((int32_t)1, docs[1].getDocId());
    }
}

TEST_F(MatchDocAllocatorTest, testCloneRefWithAllocateSize) {
    Reference<FiveBytesString> *ref1 = allocator.declare<FiveBytesString>("ref1", 0, 5);
    ASSERT_TRUE(ref1);
    Reference<FiveBytesString> *ref2 =
        dynamic_cast<Reference<FiveBytesString> *>(allocator.cloneReference(ref1, "ref2", "new_group"));
    ASSERT_TRUE(ref2);
    allocator.extend();
    EXPECT_EQ((size_t)10, allocator.getDocSize());
    MatchDoc doc1 = allocator.allocate(1);
    MatchDoc doc2 = allocator.allocate(2);
    const char *value1 = "12345";
    const char *value2 = "abcde";
    ref2->set(doc1, *((FiveBytesString *)(value1)));
    ref2->set(doc2, *((FiveBytesString *)(value2)));
    EXPECT_EQ(string("12345"), ref2->getPointer(doc1)->toString());
    EXPECT_EQ(string("abcde"), ref2->getPointer(doc2)->toString());
}

TEST_F(MatchDocAllocatorTest, testAllocate) {
    Reference<int> *field1 = allocator.declare<int>("field1", "default");
    ASSERT_TRUE(field1);
    allocator.extend();

    MatchDoc doc1 = allocator.allocate();
    EXPECT_EQ(uint32_t(0), doc1.index);
    MatchDoc doc2 = allocator.allocate();
    EXPECT_EQ(uint32_t(1), doc2.index);

    EXPECT_EQ(uint32_t(2), allocator._size);
    EXPECT_EQ(uint32_t(1024), allocator._capacity);
    allocator.deallocate(doc1);
    EXPECT_EQ(size_t(1), allocator._deletedIds.size());
    EXPECT_EQ(doc1.index, allocator._deletedIds[0]);
}

TEST_F(MatchDocAllocatorTest, testAllocateWithConstructFlag) {
    {
        MatchDocAllocator allocator(pool);
        Reference<int> *field1 = allocator.declare<int>("field1", "default");
        Reference<string> *field2 = allocator.declare<string>("field2", "default");
        allocator.extend();
        MatchDoc doc1 = allocator.allocate();
        EXPECT_EQ(uint32_t(0), doc1.index);
        field1->set(doc1, 10);
        field2->set(doc1, "aaa");
        EXPECT_EQ(10, field1->get(doc1));
        EXPECT_EQ(string("aaa"), field2->get(doc1));

        allocator.deallocate(doc1);
        doc1 = allocator.allocate();

        EXPECT_EQ(uint32_t(0), doc1.index);
        EXPECT_EQ(0, field1->get(doc1));
        EXPECT_EQ(string(""), field2->get(doc1));
    }
    {
        MatchDocAllocator allocator(pool);
        Reference<int> *field1 = allocator.declareWithConstructFlag<int>("field1", "default", false);
        Reference<string> *field2 = allocator.declare<string>("field2", "default");
        allocator.extend();
        MatchDoc doc1 = allocator.allocate();
        EXPECT_EQ(uint32_t(0), doc1.index);
        field1->set(doc1, 10);
        field2->set(doc1, "aaa");
        EXPECT_EQ(10, field1->get(doc1));
        EXPECT_EQ(string("aaa"), field2->get(doc1));

        allocator.deallocate(doc1);
        doc1 = allocator.allocate();

        EXPECT_EQ(uint32_t(0), doc1.index);
        EXPECT_EQ(10, field1->get(doc1));
        EXPECT_EQ(string(""), field2->get(doc1));
    }
}

TEST_F(MatchDocAllocatorTest, testAllocateAndClone) {
    auto fieldString = allocator.declare<std::string>("fieldString");
    auto fieldDouble = allocator.declare<double>("fieldDouble");
    ASSERT_TRUE(fieldString);
    ASSERT_TRUE(fieldDouble);
    allocator.extend();
    auto doc1 = allocator.allocate(100);
    fieldString->set(doc1, "doc1String");
    fieldDouble->set(doc1, 15.8);

    auto value = make_unique<int>(1);
    auto builder = std::make_unique<FieldGroupBuilder>("mount", pool);
    auto group = builder->fromBuffer(value.get(), 1);
    ASSERT_TRUE(group);
    ASSERT_TRUE(allocator.addGroup(std::move(group), true, false));
    auto ref2 = allocator.findReference<int>("mount");
    ASSERT_TRUE(ref2);
    auto fieldInt = allocator.declare<int>("fieldInt");
    ASSERT_TRUE(fieldInt);
    auto doc2 = allocator.allocateAndClone(doc1);

    ASSERT_EQ(doc1.getDocId(), doc2.getDocId());
    ASSERT_EQ(fieldString->get(doc1), fieldString->get(doc2));
    ASSERT_EQ(fieldDouble->get(doc1), fieldDouble->get(doc2));
    ASSERT_EQ(fieldInt->get(doc1), fieldInt->get(doc2));
    ASSERT_EQ(ref2->get(doc1), ref2->get(doc2));
    ASSERT_NE(value.get(), ref2->getPointer(doc1));
}

TEST_F(MatchDocAllocatorTest, testDeallocate) {
    Reference<int> *field1 = allocator.declare<int>("field1", "default");
    ASSERT_TRUE(field1);
    allocator.extend();

    MatchDoc doc1 = allocator.allocate();
    EXPECT_EQ(uint32_t(0), doc1.index);
    MatchDoc doc2 = allocator.allocate();
    EXPECT_EQ(uint32_t(1), doc2.index);

    // test reclaim matchdoc
    allocator.deallocate(doc1);
    EXPECT_EQ(size_t(1), allocator._deletedIds.size());
    EXPECT_EQ(doc1.index, allocator._deletedIds[0]);
    MatchDoc doc3 = allocator.allocate();
    EXPECT_EQ(size_t(0), allocator._deletedIds.size());
    EXPECT_EQ(doc1.index, doc3.index);
}

TEST_F(MatchDocAllocatorTest, testAutoCreateGroup) {
    Reference<int> *field3 = allocator.declare<int>("field3");
    ASSERT_TRUE(field3);
    Reference<string> *field4 = allocator.declare<string>("field4");
    ASSERT_TRUE(field4);
    allocator.extend();
    MatchDoc doc1 = allocator.allocate();
    MatchDoc doc2 = allocator.allocate();
    field3->set(doc1, 1);
    field4->set(doc1, "11");
    field3->set(doc2, 2);
    field4->set(doc2, "21");

    Reference<string> *field5 = allocator.declare<string>("field5");
    allocator.extend();
    Reference<string> *field6 = allocator.declare<string>("field6");
    allocator.extend();
    field5->set(doc1, "15");
    field5->set(doc2, "25");
    field6->set(doc1, "16");
    field6->set(doc2, "26");

    EXPECT_EQ(1, field3->get(doc1));
    EXPECT_EQ(2, field3->get(doc2));
    EXPECT_EQ("11", field4->get(doc1));
    EXPECT_EQ("21", field4->get(doc2));
    EXPECT_EQ("15", field5->get(doc1));
    EXPECT_EQ("25", field5->get(doc2));
    EXPECT_EQ("16", field6->get(doc1));
    EXPECT_EQ("26", field6->get(doc2));

    EXPECT_TRUE(allocator._fieldGroups["_default_0_"]->findReferenceWithoutType("field3"));
    EXPECT_TRUE(allocator._fieldGroups["_default_0_"]->findReferenceWithoutType("field4"));
    EXPECT_TRUE(allocator._fieldGroups["_default_1_"]->findReferenceWithoutType("field5"));
    EXPECT_TRUE(allocator._fieldGroups["_default_2_"]->findReferenceWithoutType("field6"));
}

TEST_F(MatchDocAllocatorTest, testValueType) {
    ReferenceBase *ref = allocator.declare<int32_t>("f1");
    ASSERT_TRUE(ref != NULL);
    ValueType valueType = ref->getValueType();
    EXPECT_TRUE(valueType.isBuiltInType());
    EXPECT_FALSE(valueType.isMultiValue());
    EXPECT_TRUE(valueType.needConstruct());
    EXPECT_EQ(bt_int32, valueType.getBuiltinType());

    ref = allocator.declare<MultiInt8>("f2");
    ASSERT_TRUE(ref != NULL);
    valueType = ref->getValueType();
    EXPECT_TRUE(valueType.isBuiltInType());
    EXPECT_TRUE(valueType.isMultiValue());
    EXPECT_TRUE(valueType.needConstruct());
    EXPECT_EQ(bt_int8, valueType.getBuiltinType());

    ref = NULL;
    ref = allocator.declare<UserDefType1>("f3");
    ASSERT_TRUE(ref != NULL);
    valueType = ref->getValueType();
    EXPECT_TRUE(valueType.needConstruct());
    EXPECT_FALSE(valueType.isBuiltInType());
}

TEST_F(MatchDocAllocatorTest, testIsSame) {
    MatchDocAllocator allocator1(pool);
    MatchDocAllocator allocator2(pool);
    MatchDocAllocator allocator3(pool, true);
    ASSERT_TRUE(allocator1.isSame(allocator2));
    ASSERT_TRUE(allocator1.isSame(allocator1));
    ASSERT_FALSE(allocator1.isSame(allocator3));
    allocator1.declare<int32_t>("ref1");
    allocator2.declare<int32_t>("ref1");
    ASSERT_TRUE(allocator1.isSame(allocator2));
    allocator1.extend();
    ASSERT_FALSE(allocator1.isSame(allocator2));
    allocator2.extend();
    ASSERT_TRUE(allocator1.isSame(allocator2));
    allocator1.declare<int32_t>("ref2", "g1");
    allocator2.declare<int32_t>("ref2", "g1");
    ASSERT_TRUE(allocator1.isSame(allocator2));
    allocator1.declare<int32_t>("ref3", "g1");
    allocator2.declare<int32_t>("ref3", "g2");
    ASSERT_FALSE(allocator1.isSame(allocator2));
}

TEST_F(MatchDocAllocatorTest, testSerialize) {
    string data;
    {
        autil::DataBuffer dataBuffer;
        MatchDocAllocator allocator1(pool);

        Reference<int> *ref1 = allocator1.declare<int>("field1");
        Reference<string> *ref2 = allocator1.declare<string>("field2");
        Reference<UserDefType1> *ref3 = allocator1.declare<UserDefType1>("field3");
        Reference<MultiInt8> *ref4 = allocator1.declare<MultiInt8>("field4");
        allocator1.extend();

        vector<MatchDoc> docs;
        docs.push_back(allocator1.allocate(1));
        docs.push_back(allocator1.allocate(2));
        docs.push_back(allocator1.allocate(3));
        for (size_t i = 0; i < docs.size(); i++) {
            ref1->set(docs[i], i);
            ref2->set(docs[i], string("string") + StringUtil::toString(i));
            UserDefType1 x;
            x.id = i;
            x.str = StringUtil::toString(i);
            ref3->set(docs[i], x);

            int8_t dataArray[] = {(int8_t)i, 3};
            char *buf = MultiValueCreator::createMultiValueBuffer(dataArray, 2, pool.get());
            MultiValueType<int8_t> multiValue(buf);
            ref4->set(docs[i], multiValue);
        }
        allocator1.serialize(dataBuffer, docs, 0);
        data.assign(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    {
        // pool for MultiInt8
        DataBuffer dataBuffer((void *)data.c_str(), data.size(), pool.get());
        MatchDocAllocator allocator2(pool);

        vector<MatchDoc> docs;
        allocator2.deserialize(dataBuffer, docs);
        Reference<int> *ref1 = allocator2.findReference<int>("field1");
        ASSERT_TRUE(ref1);
        Reference<string> *ref2 = allocator2.findReference<string>("field2");
        ASSERT_TRUE(ref2);
        Reference<UserDefType1> *ref3 = allocator2.findReference<UserDefType1>("field3");
        ASSERT_TRUE(ref3);
        Reference<MultiInt8> *ref4 = allocator2.findReference<MultiInt8>("field4");
        ASSERT_TRUE(ref4);

        ASSERT_EQ(size_t(3), docs.size());
        for (size_t i = 0; i < docs.size(); i++) {
            EXPECT_EQ(int32_t(i + 1), docs[i].getDocId());
            EXPECT_EQ(int(i), ref1->get(docs[i]));
            EXPECT_EQ(string("string") + StringUtil::toString(i), ref2->get(docs[i]));
            const UserDefType1 &x = ref3->get(docs[i]);
            EXPECT_EQ(int(i), x.id);
            EXPECT_EQ(StringUtil::toString(i), x.str);

            int8_t dataArray[] = {(int8_t)i, 3};
            char *buf = MultiValueCreator::createMultiValueBuffer(dataArray, 2, pool.get());
            MultiValueType<int8_t> multiValue(buf);
            EXPECT_EQ(multiValue, ref4->get(docs[i]));
            std::cout << "---doc: " << ref4->toString(docs[i]) << std::endl;
        }
    }
    {
        std::shared_ptr<autil::mem_pool::Pool> poolPtr(new autil::mem_pool::Pool());
        DataBuffer dataBuffer((void *)data.c_str(), data.size(), poolPtr.get());
        MatchDocAllocator allocator2(poolPtr);

        vector<MatchDoc> docs;
        allocator2.deserialize(dataBuffer, docs);
        Reference<int> *ref1 = allocator2.findReference<int>("field1");
        ASSERT_TRUE(ref1);
        EXPECT_EQ(poolPtr, ref1->_docStorage->_pool);
    }
}

TEST_F(MatchDocAllocatorTest, testDeclareAndFindWithAlias) {
    MatchDocAllocator::ReferenceAliasMapPtr aliasMap =
        MatchDocTestHelper::createReferenceAliasMap("field1_alias:field1");
    allocator.setReferenceAliasMap(aliasMap);

    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<int> *field2 = allocator.declare<int>("field1_alias");
    ASSERT_TRUE(field2);
    ASSERT_EQ(field1, field2);

    Reference<int> *field3 = allocator.findReference<int>("field1_alias");
    ASSERT_EQ(field1, field3);
}

TEST_F(MatchDocAllocatorTest, testDeleteMatchDoc) {
    Reference<int> *f1 = allocator.declare<int>("f1");
    ASSERT_TRUE(f1);
    allocator.extend();

    vector<MatchDoc> docs;
    for (size_t i = 0; i < 10; i++) {
        docs.push_back(allocator.allocate(i));
    }
    for (size_t i = 0; i < 10; i++) {
        ASSERT_EQ(i, docs[i].getIndex());
        ASSERT_FALSE(docs[i].isDeleted());
    }

    docs[4].setDeleted();
    docs[5].setDeleted();
    ASSERT_EQ(4u, docs[4].getIndex());
    ASSERT_EQ(5u, docs[5].getIndex());
    ASSERT_TRUE(docs[4].index != docs[4].getIndex()) << "left: " << docs[4].index << ", right: " << docs[4].getIndex();
    ASSERT_TRUE(docs[5].index != docs[5].getIndex());

    allocator.deallocate(docs[4]);
    ASSERT_EQ(size_t(1), allocator._deletedIds.size());
    ASSERT_EQ(4u, allocator._deletedIds[0]);
}

TEST_F(MatchDocAllocatorTest, testDeserializeAndAppend) {
    // allocator1
    MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("int_a,int_b,int_c", pool);
    MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,int_c:2,int_b:1");
    MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:2,int_c:3,int_b:4");

    vector<MatchDoc> matchDocs1;
    matchDocs1.push_back(doc11);
    matchDocs1.push_back(doc12);

    // allocator2
    MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("int_a,int_c,int_b", pool);
    MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:3,int_c:4,int_b:1");
    MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:4,int_c:5,int_b:5");

    vector<MatchDoc> matchDocs2;
    matchDocs2.push_back(doc21);
    matchDocs2.push_back(doc22);

    // allocator3
    MatchDocAllocatorPtr allocator3 = MatchDocTestHelper::createAllocator("int_a,int_c,int_d", pool);
    MatchDoc doc31 = MatchDocTestHelper::createMatchDoc(allocator3, "int_a:3,int_c:4,int_d:6");
    MatchDoc doc32 = MatchDocTestHelper::createMatchDoc(allocator3, "int_a:4,int_c:5,int_d:7");

    vector<MatchDoc> matchDocs3;
    matchDocs3.push_back(doc31);
    matchDocs3.push_back(doc32);

    // serialize
    DataBuffer buffer1(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool.get());
    allocator1->serialize(buffer1, matchDocs1, 0);
    DataBuffer buffer2(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool.get());
    allocator2->serialize(buffer2, matchDocs2, 0);
    DataBuffer buffer3(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool.get());
    allocator3->serialize(buffer3, matchDocs3, 0);

    // deserialize
    vector<MatchDoc> matchDocs;
    DataBuffer buffer4(buffer1.getData(), buffer1.getDataLen(), pool.get());
    allocator.deserialize(buffer4, matchDocs);
    ASSERT_EQ((size_t)2, matchDocs.size());
    DataBuffer buffer5(buffer2.getData(), buffer2.getDataLen(), pool.get());
    ASSERT_TRUE(allocator.deserializeAndAppend(buffer5, matchDocs));
    ASSERT_EQ((size_t)4, matchDocs.size());
    DataBuffer buffer6(buffer3.getData(), buffer3.getDataLen(), pool.get());
    ASSERT_FALSE(allocator.deserializeAndAppend(buffer6, matchDocs));
}

TEST_F(MatchDocAllocatorTest, testDeserializeAndDelete) {
    auto dataBufferPool = new autil::mem_pool::Pool();
    DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, dataBufferPool);
    MatchDocAllocator allocator(pool);
    Reference<MultiString> *ref = allocator.declare<MultiString>("str_field");
    allocator.extend();

    vector<MatchDoc> docs;
    docs.push_back(allocator.allocate(1));
    vector<string> dataVec{"", "s2", "s3;s4;s5"};
    char *buf = MultiValueCreator::createMultiStringBuffer(dataVec, pool.get());
    MultiString multiStr;
    multiStr.init(buf);
    ref->set(docs[0], multiStr);
    allocator.serialize(dataBuffer, docs, 0);

    vector<MatchDoc> docs2;
    MatchDocAllocator allocator2(pool);
    allocator2.deserialize(dataBuffer, docs2);
    delete dataBufferPool;
    ASSERT_EQ(size_t(1), docs2.size());
    Reference<MultiString> *ref2 = allocator.declare<MultiString>("str_field");
    ASSERT_EQ(multiStr, ref2->get(docs2[0]));
}

TEST_F(MatchDocAllocatorTest, testMergeAllocator1) {
    // allocator1
    MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
    std::vector<MatchDoc> docs1;
    MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,int_c:2,int_b:3");
    docs1.push_back(doc11);
    MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:4,int_c:5,int_b:6");
    docs1.push_back(doc12);

    // allocator2
    MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
    std::vector<MatchDoc> docs2;
    MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,int_c:12,int_b:13");
    docs2.push_back(doc21);
    MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,int_c:15,int_b:16");
    docs2.push_back(doc22);

    ASSERT_TRUE(allocator1->mergeAllocator(allocator2.get(), docs2, docs1));
    ASSERT_EQ(4u, docs1.size());
    auto refVec = allocator1->getAllNeedSerializeReferences(0);
    for (size_t i = 0; i < 4; i++) {
        for (auto ref : refVec) {
            auto doc = docs1[i];
            std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex() << ", field: " << ref->getName()
                      << ", toString: " << ref->toString(doc) << std::endl;
        }
    }
}

TEST_F(MatchDocAllocatorTest, testMergeAllocator2) {
    // allocator1
    MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("g1:int_a,g1:int_b,g3:int_c", pool);
    std::vector<MatchDoc> docs1;
    MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,int_c:2,int_b:3");
    docs1.push_back(doc11);
    MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:4,int_c:5,int_b:6");
    docs1.push_back(doc12);

    // allocator2
    MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
    std::vector<MatchDoc> docs2;
    MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,int_c:12,int_b:13");
    docs2.push_back(doc21);
    MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,int_c:15,int_b:16");
    docs2.push_back(doc22);

    ASSERT_TRUE(allocator1->mergeAllocator(allocator2.get(), docs2, docs1));
    ASSERT_EQ(4u, docs1.size());
    MatchDoc doc13 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:21,int_c:22,int_b:23");
    docs1.push_back(doc13);
    auto refVec = allocator1->getAllNeedSerializeReferences(0);
    for (size_t i = 0; i < docs1.size(); i++) {
        for (auto ref : refVec) {
            auto doc = docs1[i];
            std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex() << ", field: " << ref->getName()
                      << ", toString: " << ref->toString(doc) << std::endl;
        }
    }
}

TEST_F(MatchDocAllocatorTest, testMergeAllocator3) {
    // allocator1
    MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("g1:int_a,g1:int_b,g1:int_c", pool);
    std::vector<MatchDoc> docs1;
    MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,int_c:2,int_b:3");
    docs1.push_back(doc11);
    MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:4,int_c:5,int_b:6");
    docs1.push_back(doc12);

    // allocator2
    MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
    std::vector<MatchDoc> docs2;
    MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,int_c:12,int_b:13");
    docs2.push_back(doc21);
    MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,int_c:15,int_b:16");
    docs2.push_back(doc22);

    ASSERT_TRUE(allocator1->mergeAllocator(allocator2.get(), docs2, docs1));
    ASSERT_EQ(4u, docs1.size());
    MatchDoc doc13 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:21,int_c:22,int_b:23");
    docs1.push_back(doc13);
    auto refVec = allocator1->getAllNeedSerializeReferences(0);
    for (size_t i = 0; i < docs1.size(); i++) {
        for (auto ref : refVec) {
            auto doc = docs1[i];
            std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex() << ", field: " << ref->getName()
                      << ", toString: " << ref->toString(doc) << std::endl;
        }
    }
}

TEST_F(MatchDocAllocatorTest, testMergeAllocator4) {
    // allocator1
    MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:string_c", pool);
    std::vector<MatchDoc> docs1;
    MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,string_c:abc,int_b:3");
    docs1.push_back(doc11);
    MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:4,string_c:def,int_b:6");
    docs1.push_back(doc12);

    // allocator2
    MatchDocAllocatorPtr allocator2 =
        MatchDocTestHelper::createAllocator("g2:int_a,g3:int_b,g4:string_c,g4:int_d", pool);
    std::vector<MatchDoc> docs2;
    MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,string_c:12,int_b:13,int_d:17");
    docs2.push_back(doc21);
    MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,string_c:15,int_b:16,int_d:17");
    docs2.push_back(doc22);

    ASSERT_TRUE(allocator1->mergeAllocator(allocator2.get(), docs2, docs1));
    ASSERT_EQ(4u, docs1.size());
    MatchDoc doc13 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:21,string_c:22,int_b:23");
    docs1.push_back(doc13);
    auto refVec = allocator1->getAllNeedSerializeReferences(0);
    for (size_t i = 0; i < docs1.size(); i++) {
        for (auto ref : refVec) {
            auto doc = docs1[i];
            std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex() << ", field: " << ref->getName()
                      << ", toString: " << ref->toString(doc) << std::endl;
        }
    }
}

TEST_F(MatchDocAllocatorTest, testMergeAllocator5) {
    // allocator1
    MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:string_c", pool);
    std::vector<MatchDoc> docs1;
    MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,string_c:abc,int_b:3");
    docs1.push_back(doc11);
    MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:4,string_c:def,int_b:6");
    docs1.push_back(doc12);

    // allocator2
    MatchDocAllocatorPtr allocator2 =
        MatchDocTestHelper::createAllocator("g2:int_a,g3:int_b,g4:string_c,g4:int_d,g4:string_e", pool);
    std::vector<MatchDoc> docs2;
    MatchDoc doc21 =
        MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,string_c:12,int_b:13,int_d:17,string_e:21");
    docs2.push_back(doc21);
    MatchDoc doc22 =
        MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,string_c:15,int_b:16,int_d:17,string_e:22");
    docs2.push_back(doc22);

    ASSERT_TRUE(allocator1->mergeAllocator(allocator2.get(), docs2, docs1));
    ASSERT_EQ(4u, docs1.size());
    MatchDoc doc13 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:21,string_c:22,int_b:23");
    docs1.push_back(doc13);
    auto refVec = allocator1->getAllNeedSerializeReferences(0);
    for (size_t i = 0; i < docs1.size(); i++) {
        for (auto ref : refVec) {
            auto doc = docs1[i];
            std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex() << ", field: " << ref->getName()
                      << ", toString: " << ref->toString(doc) << std::endl;
        }
    }
}

TEST_F(MatchDocAllocatorTest, testMergeAllocatorCopy_Append) {
    MatchDocAllocatorPtr allocator1(new MatchDocAllocator(pool));
    MatchDocAllocatorPtr allocator2(new MatchDocAllocator(pool));
    MatchDocAllocatorPtr allocator3(new MatchDocAllocator(pool));
    MatchDocAllocatorPtr allocator4(new MatchDocAllocator(pool));
    auto ref1 = allocator1->declare<std::string>("string", "group1");
    allocator1->declare<int>("int", "group1");
    allocator1->declare<std::string>("string2", "group4");

    auto ref2 = allocator2->declare<std::string>("string", "group1");
    auto ref21 = allocator2->declare<int>("int", "group1");
    allocator2->declare<int>("int2", "group1");
    allocator2->declare<std::string>("string2", "group4");

    auto ref3 = allocator3->declare<std::string>("string", "group1");
    allocator3->declare<int>("int", "group1");
    allocator3->declare<int>("int2", "group1");
    allocator3->declare<std::string>("string2", "group4");

    auto ref4 = allocator4->declare<std::string>("string", "group1");
    allocator4->declare<int>("int", "group1");
    allocator4->declare<std::string>("string2", "group4");

    std::vector<MatchDoc> docs1, docs2, docs3, docs4;

    for (size_t i = 0; i < 100; i++) {
        auto doc1 = allocator1->allocate(1);
        ref1->set(doc1, "value");
        docs1.push_back(doc1);
    }

    for (size_t i = 0; i < 100; i++) {
        auto doc2 = allocator2->allocate(2);
        ref2->set(doc2, "value");
        ref21->set(doc2, 33);
        docs2.push_back(doc2);
    }

    for (size_t i = 0; i < 100; i++) {
        auto doc3 = allocator3->allocate(3);
        ref3->set(doc3, "value");
        docs3.push_back(doc3);
    }

    for (size_t i = 0; i < 100; i++) {
        auto doc4 = allocator4->allocate(4);
        ref4->set(doc4, "value");
        docs4.push_back(doc4);
    }

    allocator1->mergeAllocator(allocator2.get(), docs2, docs1);
    allocator1->mergeAllocator(allocator3.get(), docs3, docs1);
    allocator1->mergeAllocator(allocator4.get(), docs4, docs1);
    ASSERT_EQ(400u, docs1.size());
    for (size_t i = 0; i < docs1.size(); i++) {
        ASSERT_EQ("value", ref1->get(docs1[i]));
    }
    for (const auto &pair : allocator1->_fieldGroups) {
        ASSERT_EQ(4, pair.second->_storage->_chunks.size());
        ASSERT_EQ(allocator1->_capacity, pair.second->_storage->_chunks.size() * VectorStorage::CHUNKSIZE);
    }
    allocator1->compact(docs1);
    ASSERT_EQ(400u, docs1.size());
    for (size_t i = 0; i < docs1.size(); i++) {
        ASSERT_EQ("value", ref1->get(docs1[i]));
    }
    for (const auto &pair : allocator1->_fieldGroups) {
        ASSERT_EQ(1, pair.second->_storage->_chunks.size());
        ASSERT_EQ(allocator1->_capacity, pair.second->_storage->_chunks.size() * VectorStorage::CHUNKSIZE);
    }
}

TEST_F(MatchDocAllocatorTest, testMergeAllocatorEmptyAllocator) {
    // allocator1
    MatchDocAllocatorPtr allocator1(new MatchDocAllocator(pool));
    std::vector<MatchDoc> docs1;
    auto doc11 = allocator1->allocate(1);
    docs1.push_back(doc11);
    auto doc12 = allocator1->allocate(2);
    docs1.push_back(doc12);

    // allocator2
    MatchDocAllocatorPtr allocator2 =
        MatchDocTestHelper::createAllocator("g2:int_a,g3:int_b,g4:string_c,g4:int_d,g4:string_e", pool);
    std::vector<MatchDoc> docs2;
    MatchDoc doc21 =
        MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,string_c:12,int_b:13,int_d:17,string_e:21");
    docs2.push_back(doc21);
    MatchDoc doc22 =
        MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,string_c:15,int_b:16,int_d:17,string_e:22");
    docs2.push_back(doc22);

    ASSERT_TRUE(allocator1->mergeAllocator(allocator2.get(), docs2, docs1));
    ASSERT_EQ(4u, docs1.size());
    MatchDoc doc13 = allocator1->allocate(13);
    docs1.push_back(doc13);
    auto refVec = allocator1->getAllNeedSerializeReferences(0);
    for (size_t i = 0; i < docs1.size(); i++) {
        for (auto ref : refVec) {
            auto doc = docs1[i];
            std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex() << ", field: " << ref->getName()
                      << ", toString: " << ref->toString(doc) << std::endl;
        }
    }
}

TEST_F(MatchDocAllocatorTest, testMergeAllocatorSub1) {
    vector<string> docStrVec;
    docStrVec.reserve(5);
    docStrVec.emplace_back("0,1^2^3#int_a:1,string_c:abc,int_b:3,sub_int_d:1^2^3,sub_string_e:Sabc1^Sdef1^Sgh1");
    docStrVec.emplace_back("1,4^5^6#int_a:10,string_c:xyz,int_b:30,sub_int_d:4^5^6,sub_string_e:Sabc2^Sdef2^Sgh2");
    docStrVec.emplace_back("2,7^8^9#int_a:11,string_c:12,int_b:13,int_d:14,sub_int_d:7^8^9,sub_string_e:Sabc3^Sdef3^"
                           "Sgh3,sub_string_f:Sf1^Sf2^Sf3");
    docStrVec.emplace_back("3,10^11^12#int_a:14,string_c:15,int_b:16,int_d:17,sub_int_d:10^11^12,sub_string_e:Sabc22^"
                           "Sdef22^Sgh22,sub_string_f:Sf22^Sf22^Sf22");
    docStrVec.emplace_back(
        "4,14^15^16#int_a:18,string_c:18,int_b:20,sub_int_d:14^15^16,sub_string_e:Sabc13^Sdef13^Sgh13");

    auto expectVec = docStrVec;
    expectVec[2] = "2,7^8^9#int_a:11,string_c:12,int_b:13,sub_int_d:7^8^9,sub_string_e:Sabc3^Sdef3^Sgh3";
    expectVec[3] = "3,10^11^12#int_a:14,string_c:15,int_b:16,sub_int_d:10^11^12,sub_string_e:Sabc22^Sdef22^Sgh22";

    // allocator1
    MatchDocAllocatorPtr allocator1 =
        MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:string_c,sub1:sub_int_d,sub2:sub_string_e", pool);
    std::vector<MatchDoc> docs1;
    MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, docStrVec[0]);
    docs1.push_back(doc11);
    MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, docStrVec[1]);
    docs1.push_back(doc12);

    // allocator2
    MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator(
        "g2:int_a,g3:int_b,g4:string_c,g4:int_d,sub1:sub_int_d,sub2:sub_string_e,sub3:sub_string_f", pool);
    std::vector<MatchDoc> docs2;
    MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, docStrVec[2]);
    docs2.push_back(doc21);
    MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, docStrVec[3]);
    docs2.push_back(doc22);

    std::map<int32_t, std::vector<int32_t>> docs;
    SubCollector subCollector1(allocator1->getSubDocRef(), docs);
    for (auto matchdoc : docs1) {
        allocator1->getSubDocAccessor()->foreach (matchdoc, subCollector1);
    }
    SubCollector subCollector2(allocator2->getSubDocRef(), docs);
    for (auto matchdoc : docs2) {
        allocator2->getSubDocAccessor()->foreach (matchdoc, subCollector2);
    }
    MatchDocAllocatorPtr allocator3(new MatchDocAllocator(pool, true));
    std::vector<MatchDoc> docs3;
    {
        DataBuffer dataBuffer;
        allocator1->serialize(dataBuffer, docs1, 255);
        allocator3->deserialize(dataBuffer, docs3);
        ASSERT_TRUE(allocator3->_subAllocator->_capacity == VectorStorage::getAlignSize(1));
    }
    MatchDocAllocatorPtr allocator4(new MatchDocAllocator(pool, true));
    std::vector<MatchDoc> docs4;
    {
        DataBuffer dataBuffer;
        allocator2->serialize(dataBuffer, docs2, 0);
        allocator4->deserialize(dataBuffer, docs4);
    }

    ASSERT_TRUE(allocator3->mergeAllocator(allocator2.get(), docs2, docs3));
    std::map<int32_t, std::vector<int32_t>> docsAfterMerge;
    SubCollector subCollector3(allocator3->getSubDocRef(), docsAfterMerge);
    for (auto matchdoc : docs3) {
        allocator3->getSubDocAccessor()->foreach (matchdoc, subCollector3);
    }
    for (auto item : docs) {
        printf("befer merge get docid: %d subdoc: %s\n", item.first, autil::StringUtil::toString(item.second).c_str());
    }
    for (auto item : docsAfterMerge) {
        printf("after merge get docid: %d subdoc: %s\n", item.first, autil::StringUtil::toString(item.second).c_str());
    }
    ASSERT_TRUE(docsAfterMerge == docs);
    ASSERT_TRUE(allocator1->mergeAllocator(allocator4.get(), docs4, docs1));
    ASSERT_EQ(4u, docs1.size());
    allocator1->compact(docs1);
    MatchDoc doc13 = MatchDocTestHelper::createMatchDoc(allocator1, docStrVec[4]);
    docs1.push_back(doc13);
    auto ref_a = allocator1->findReference<int>("int_a");
    auto ref_b = allocator1->findReference<int>("int_b");
    auto ref_c = allocator1->findReference<std::string>("string_c");
    auto ref_d = allocator1->findReference<int>("int_d");
    auto ref_sub_d = allocator1->findReference<int>("sub_int_d");
    auto ref_sub_e = allocator1->findReference<std::string>("sub_string_e");
    auto ref_sub_f = allocator1->findReference<std::string>("sub_string_f");
    ASSERT_TRUE(ref_a);
    ASSERT_TRUE(ref_b);
    ASSERT_TRUE(ref_c);
    ASSERT_FALSE(ref_d);
    ASSERT_TRUE(ref_sub_d);
    ASSERT_TRUE(ref_sub_e);
    ASSERT_FALSE(ref_sub_f);
    ostringstream result;
    auto printRef = [&](MatchDoc doc) {
        std::cout << "Main MainDocId: " << doc.getDocId() << ", index: " << doc.getIndex()
                  << ", field: " << ref_a->getName() << ", value: " << ref_a->toString(doc)
                  << ", field: " << ref_b->getName() << ", value: " << ref_b->toString(doc)
                  << ", field: " << ref_c->getName() << ", value: " << ref_c->toString(doc) << std::endl;
    };
    auto acessor = allocator1->getSubDocAccessor();
    auto current = acessor->_current;
    for (size_t i = 0; i < docs1.size(); i++) {
        auto mainDoc = docs1[i];
        printRef(mainDoc);
        string sub_doc_ids, ref_sub_d_val, ref_sub_e_val;
        auto customLambda = [&](MatchDoc doc) {
            auto subDoc = current->get(doc);
            std::cout << "MainDocId: " << doc.getDocId() << ", MainIndex: " << doc.getIndex()
                      << ", subDocId: " << subDoc.getDocId() << ", index: " << subDoc.getIndex()
                      << ", field: " << ref_sub_d->getName() << ", value: " << ref_sub_d->toString(doc)
                      << ", field: " << ref_sub_e->getName() << ", value: " << ref_sub_e->toString(doc) << std::endl;
            if (!ref_sub_d_val.empty()) {
                ref_sub_d_val += "^";
                ref_sub_e_val += "^";
                sub_doc_ids += "^";
            }
            ref_sub_d_val += ref_sub_d->toString(doc);
            ref_sub_e_val += ref_sub_e->toString(doc);
            sub_doc_ids += autil::StringUtil::toString(subDoc.getDocId());
        };
        acessor->foreach (mainDoc, customLambda);
        result << mainDoc.getDocId() << "," << sub_doc_ids << "#" << ref_a->getName() << ":" << ref_a->toString(mainDoc)
               << "," << ref_c->getName() << ":" << ref_c->toString(mainDoc) << "," << ref_b->getName() << ":"
               << ref_b->toString(mainDoc) << "," << ref_sub_d->getName() << ":" << ref_sub_d_val << ","
               << ref_sub_e->getName() << ":" << ref_sub_e_val;
        cout << "---tag: " << result.str() << endl;
        EXPECT_EQ(expectVec[i], result.str());
        result.clear();
        result.str("");
        ref_sub_d_val.clear();
        ref_sub_e_val.clear();
        sub_doc_ids.clear();
    }
}

TEST_F(MatchDocAllocatorTest, testDeclareDiffGroup) {
    Reference<int> *f1 = allocator.declare<int>("f1", "group1");
    Reference<int> *f12 = allocator.declare<int>("f1", "group2");
    EXPECT_TRUE(f1 == f12);
}

TEST_F(MatchDocAllocatorTest, testExtendGroup1) {
    Reference<int> *f1 = allocator.declare<int>("f1", "group1");
    ASSERT_TRUE(f1);
    Reference<int> *f2 = allocator.declare<int>("f2", "group2");
    ASSERT_TRUE(f2);
    allocator.extendGroup("group1");
    auto doc1 = allocator.allocate(1);
    auto doc2 = allocator.allocate(2);
    f1->set(doc1, 1);
    f1->set(doc2, 2);
    EXPECT_EQ(1, f1->get(doc1));
    EXPECT_EQ(2, f1->get(doc2));
    Reference<int> *f3 = allocator.declare<int>("f3", "group3");
    ASSERT_TRUE(f3);
    allocator.extend();
    f3->set(doc1, 1);
    f3->set(doc2, 2);
    EXPECT_EQ(1, f3->get(doc1));
    EXPECT_EQ(2, f3->get(doc2));
}

TEST_F(MatchDocAllocatorTest, testExtendGroup2) {
    MatchDocAllocatorPtr allocator1(new MatchDocAllocator(pool));
    allocator1->declare<std::string>("string", "group1");
    allocator1->declare<int>("int", "group1");
    allocator1->declare<std::string>("string2", "group2");

    for (size_t i = 0; i < VectorStorage::CHUNKSIZE * 3 - 10; i++) {
        allocator1->allocate(i);
    }

    auto group1 = findGroup(*allocator1, "group1");
    auto group2 = findGroup(*allocator1, "group2");
    ASSERT_TRUE(group1);
    ASSERT_TRUE(group2);

    ASSERT_EQ(3u, group1->_storage->_chunks.size());
    ASSERT_EQ(3u, group2->_storage->_chunks.size());

    allocator1->declare<std::string>("string3", "group3");
    allocator1->extend();

    auto group3 = findGroup(*allocator1, "group3");
    ASSERT_TRUE(group3);
    ASSERT_EQ(3u, group3->_storage->_chunks.size());

    for (size_t i = 0; i < 20; i++) {
        allocator1->allocate(i);
    }

    ASSERT_EQ(4u, group1->_storage->_chunks.size());
    ASSERT_EQ(4u, group2->_storage->_chunks.size());
    ASSERT_EQ(4u, group3->_storage->_chunks.size());
}

TEST_F(MatchDocAllocatorTest, testValueType1) {
    ASSERT_EQ(0, bt_unknown);
    ASSERT_EQ(1, bt_int8);
    ASSERT_EQ(2, bt_int16);
    ASSERT_EQ(3, bt_int32);
    ASSERT_EQ(4, bt_int64);
    ASSERT_EQ(5, bt_uint8);
    ASSERT_EQ(6, bt_uint16);
    ASSERT_EQ(7, bt_uint32);
    ASSERT_EQ(8, bt_uint64);
    ASSERT_EQ(9, bt_float);
    ASSERT_EQ(10, bt_double);
    ASSERT_EQ(11, bt_string);
    ASSERT_EQ(12, bt_bool);
    ASSERT_EQ(13, bt_cstring_deprecated);
    ASSERT_EQ(14, bt_hash_128);
    ASSERT_EQ(24, bt_max);

    ValueType value;
    value.setBuiltin();
    ASSERT_EQ(1u, value.getType());
    value.setMultiValue(true);
    ASSERT_EQ(3u, value.getType());
    value.setBuiltinType(bt_int8);
    ASSERT_EQ(7u, value.getType());
    value.setBuiltinType(bt_max);
    ASSERT_EQ(99u, value.getType());

    ValueType type;
#define TEST_TYPE(tp, tf1)                                                                                             \
    type = ValueTypeHelper<tp>::getValueType();                                                                        \
    ASSERT_TRUE(type.isBuiltInType());                                                                                 \
    ASSERT_TRUE(tf1 == type.isMultiValue());

    bool isMulti = true;
    struct MyStruct {};
    type = ValueTypeHelper<MyStruct>::getValueType();
    ASSERT_FALSE(type.isBuiltInType());
    ASSERT_FALSE(type.isMultiValue());

    TEST_TYPE(autil::MultiInt8, isMulti);
    TEST_TYPE(autil::MultiInt16, isMulti);
    TEST_TYPE(autil::MultiInt32, isMulti);
    TEST_TYPE(autil::MultiInt64, isMulti);
    TEST_TYPE(autil::MultiUInt8, isMulti);
    TEST_TYPE(autil::MultiUInt16, isMulti);
    TEST_TYPE(autil::MultiUInt32, isMulti);
    TEST_TYPE(autil::MultiUInt64, isMulti);
    TEST_TYPE(autil::MultiFloat, isMulti);
    TEST_TYPE(autil::MultiDouble, isMulti);
    TEST_TYPE(autil::MultiString, isMulti);
#undef TEST_TYPE
}

TEST_F(MatchDocAllocatorTest, testToString) {
#define TEST_SINGLE_TYPE_TO_STRING(type, v1, output_string)                                                            \
    {                                                                                                                  \
        MatchDocAllocator alloc(pool);                                                                                 \
        auto ref = alloc.declare<type>("ref", 0);                                                                      \
        ASSERT_TRUE(ref);                                                                                              \
        alloc.extend();                                                                                                \
        auto doc1 = alloc.allocate(1);                                                                                 \
        ref->set(doc1, v1);                                                                                            \
        auto s = ref->toString(doc1);                                                                                  \
        std::cout << s << endl;                                                                                        \
        EXPECT_EQ(output_string, s);                                                                                   \
    }

    TEST_SINGLE_TYPE_TO_STRING(int8_t, 1, "1");
    TEST_SINGLE_TYPE_TO_STRING(int16_t, 1, "1");
    TEST_SINGLE_TYPE_TO_STRING(int32_t, 1, "1");
    TEST_SINGLE_TYPE_TO_STRING(int64_t, 1, "1");

    TEST_SINGLE_TYPE_TO_STRING(uint8_t, 1, "1");
    TEST_SINGLE_TYPE_TO_STRING(uint16_t, 1, "1");
    TEST_SINGLE_TYPE_TO_STRING(uint32_t, 1, "1");
    TEST_SINGLE_TYPE_TO_STRING(uint64_t, 1, "1");

    TEST_SINGLE_TYPE_TO_STRING(autil::uint128_t, 1, "00000000000000000000000000000001");
    TEST_SINGLE_TYPE_TO_STRING(float, 1.2, "1.2");
    TEST_SINGLE_TYPE_TO_STRING(double, 1.2, "1.2");
    TEST_SINGLE_TYPE_TO_STRING(bool, true, "1");
    TEST_SINGLE_TYPE_TO_STRING(std::string, "abc", "abc");
#undef TEST_SINGLE_TYPE_TO_STRING

#define TEST_MULTIVALUE_TYPE_TO_STRING(singleType, v1, v2, v3, output_string)                                          \
    {                                                                                                                  \
        MatchDocAllocator allocator(pool);                                                                             \
        auto *ref = allocator.declare<autil::MultiValueType<singleType>>("ref");                                       \
        allocator.extend();                                                                                            \
        auto slab = allocator.allocate(1);                                                                             \
        ASSERT_TRUE(ref);                                                                                              \
        singleType intVec[] = {v1, v2, v3};                                                                            \
        auto mi = autil::MultiValueCreator::createMultiValueBuffer(intVec, 3, pool.get());                             \
        ref->set(slab, mi);                                                                                            \
        auto s = ref->toString(slab);                                                                                  \
        std::cout << s << endl;                                                                                        \
        EXPECT_EQ(output_string, s);                                                                                   \
    }

    TEST_MULTIVALUE_TYPE_TO_STRING(int8_t, 1, 2, -3, "12-3");
    TEST_MULTIVALUE_TYPE_TO_STRING(int16_t, 1, 2, 3, "123");
    TEST_MULTIVALUE_TYPE_TO_STRING(int32_t, 1, 2, 3, "123");
    TEST_MULTIVALUE_TYPE_TO_STRING(int64_t, 1, 2, 3, "123");

    TEST_MULTIVALUE_TYPE_TO_STRING(uint8_t, 1, 2, 3, "123");
    TEST_MULTIVALUE_TYPE_TO_STRING(uint16_t, 1, 2, 3, "123");
    TEST_MULTIVALUE_TYPE_TO_STRING(uint32_t, 1, 2, 3, "123");
    TEST_MULTIVALUE_TYPE_TO_STRING(uint64_t, 1, 2, 3, "123");

    TEST_MULTIVALUE_TYPE_TO_STRING(float, 1.2, 2.3, 3.5, "1.22.33.5");
    TEST_MULTIVALUE_TYPE_TO_STRING(double, 1.2, 2.3, 3.5, "1.22.33.5");
    TEST_MULTIVALUE_TYPE_TO_STRING(char, 'a', 'b', 'c', "abc");
    {
        // MultiString
        MatchDocAllocator allocator(pool);
        auto *ref = allocator.declare<autil::MultiValueType<autil::MultiChar>>("ref");
        allocator.extend();
        auto slab = allocator.allocate(1);
        ASSERT_TRUE(ref);
        std::vector<std::string> vec = {"abc", "def", ""};
        auto mi = autil::MultiValueCreator::createMultiStringBuffer(vec, pool.get());
        ref->set(slab, mi);
        auto s = ref->toString(slab);
        std::cout << s << endl;
        EXPECT_EQ("abcdef", s);
    }

#undef TEST_MULTIVALUE_TYPE_TO_STRING
}

TEST_F(MatchDocAllocatorTest, testGetRefBySerializeLevel) {
    MatchDocAllocator allocator(pool);
#define DECLARE_FIELD(type, name, serializeLevel) allocator.declare<type>(name, serializeLevel);

    DECLARE_FIELD(int32_t, "ref1", 1);
    DECLARE_FIELD(int32_t, "ref2", 1);
    DECLARE_FIELD(int32_t, "ref3", 2);
    DECLARE_FIELD(int32_t, "ref4", 3);
    DECLARE_FIELD(int32_t, "ref5", 3);
    DECLARE_FIELD(int32_t, "ref6", 3);
    allocator.extend();
    DECLARE_FIELD(int32_t, "ref7", 1);
    DECLARE_FIELD(int32_t, "ref8", 2);
    DECLARE_FIELD(int32_t, "ref9", 4);
#undef DECLARE_FIELD

    auto level1 = allocator.getRefBySerializeLevel(1);
    ASSERT_EQ((size_t)3, level1.size());
    ASSERT_EQ(string("ref1"), level1[0]->getName());
    ASSERT_EQ(string("ref2"), level1[1]->getName());
    ASSERT_EQ(string("ref7"), level1[2]->getName());

    auto level2 = allocator.getRefBySerializeLevel(2);
    ASSERT_EQ((size_t)2, level2.size());
    ASSERT_EQ(string("ref3"), level2[0]->getName());
    ASSERT_EQ(string("ref8"), level2[1]->getName());

    auto level3 = allocator.getRefBySerializeLevel(3);
    ASSERT_EQ((size_t)3, level3.size());
    ASSERT_EQ(string("ref4"), level3[0]->getName());
    ASSERT_EQ(string("ref5"), level3[1]->getName());
    ASSERT_EQ(string("ref6"), level3[2]->getName());

    auto level4 = allocator.getRefBySerializeLevel(4);
    ASSERT_EQ((size_t)1, level4.size());
    ASSERT_EQ(string("ref9"), level4[0]->getName());
}

TEST_F(MatchDocAllocatorTest, testCloneAllocatorWithoutData) {
    {
        MatchDocAllocator allocator(pool);
        allocator.declare<int32_t>("aa");
        allocator.extend();
        allocator.declare<int32_t>("bb");
        MatchDocAllocator *clone = allocator.cloneAllocatorWithoutData();
        MatchDocAllocatorPtr newAllocator(clone);
        ASSERT_TRUE(newAllocator != nullptr);
        ASSERT_TRUE(allocator.isSame(*newAllocator));
    }
    {
        std::shared_ptr<autil::mem_pool::Pool> poolPtr(new autil::mem_pool::Pool());
        MatchDocAllocator allocator(poolPtr, true);
        allocator.declare<int32_t>("aa");
        allocator.declareSub<int32_t>("cc");
        allocator.extend();
        allocator.declare<int32_t>("bb");
        MatchDocAllocator *clone = allocator.cloneAllocatorWithoutData();
        MatchDocAllocatorPtr newAllocator(clone);
        ASSERT_TRUE(newAllocator != nullptr);
        ASSERT_TRUE(allocator.isSame(*newAllocator));
        EXPECT_EQ(allocator._poolPtr, clone->_poolPtr);
    }
}

TEST_F(MatchDocAllocatorTest, testSwapDocStorage) {
    {
        MatchDocAllocator allocator(pool);
        allocator.declare<int32_t>("aa");
        vector<MatchDoc> matchDoc1;
        MatchDocAllocator allocator2(pool);
        allocator2.declare<int32_t>("bb");
        vector<MatchDoc> matchDoc2;
        ASSERT_FALSE(allocator2.swapDocStorage(allocator, matchDoc1, matchDoc2));
    }
    {
        MatchDocAllocator allocator(pool);
        auto ref1 = allocator.declare<int32_t>("aa");
        vector<MatchDoc> matchDoc1;
        MatchDoc doc = allocator.allocate(1);
        matchDoc1.push_back(doc);
        ref1->set(doc, 10);

        std::shared_ptr<autil::mem_pool::Pool> poolPtr(new autil::mem_pool::Pool());
        MatchDocAllocator allocator2(poolPtr);
        auto ref2 = allocator2.declare<int32_t>("aa");
        vector<MatchDoc> matchDoc2;
        ASSERT_TRUE(allocator2.swapDocStorage(allocator, matchDoc1, matchDoc2));
        ASSERT_EQ(0, matchDoc1.size());
        ASSERT_EQ(1, matchDoc2.size());
        ASSERT_EQ(10, ref2->get(matchDoc2[0]));
        ASSERT_EQ(poolPtr, allocator._poolPtr);
        ASSERT_TRUE(allocator2._poolPtr);
        ASSERT_EQ(pool, allocator2._poolPtr);
    }

    { // subdoc
        MatchDocAllocator allocator(pool, true);
        auto ref1 = allocator.declare<int32_t>("aa");
        auto subRef1 = allocator.declareSub<int32_t>("sub_aa");
        vector<MatchDoc> matchDoc1;
        MatchDoc doc = allocator.allocate(1);
        MatchDoc subDoc = allocator.allocateSub(doc, 20);
        matchDoc1.push_back(doc);
        ref1->set(doc, 10);
        subRef1->set(subDoc, 20);

        MatchDocAllocator allocator2(pool, true);
        auto ref2 = allocator2.declare<int32_t>("aa");
        auto subRef2 = allocator2.declareSub<int32_t>("sub_aa");
        allocator2.extend();
        allocator2.extendSub();
        SubDocAccessor *subDocAccessor = allocator2.getSubDocAccessor();
        vector<MatchDoc> matchDoc2;
        ASSERT_TRUE(allocator2.swapDocStorage(allocator, matchDoc1, matchDoc2));
        ASSERT_EQ(0, matchDoc1.size());
        ASSERT_EQ(1, matchDoc2.size());
        ASSERT_EQ(10, ref2->get(matchDoc2[0]));
        vector<MatchDoc> subDocs;
        ASSERT_EQ(1, subDocAccessor->getSubDocs(matchDoc2[0], subDocs));
        ASSERT_EQ(20, subRef2->get(subDocs[0]));
        ASSERT_EQ(20, subRef2->get(subDoc));
    }
}

TEST_F(MatchDocAllocatorTest, testGrow) {
    MatchDocAllocatorPtr allocator1(new MatchDocAllocator(pool));
    allocator1->declare<std::string>("string", "group1");
    allocator1->declare<int>("int", "group1");
    allocator1->declare<std::string>("string2", "group2");

    allocator1->extend();

    auto group1 = findGroup(*allocator1, "group1");
    auto group2 = findGroup(*allocator1, "group2");

    ASSERT_TRUE(group1);
    ASSERT_TRUE(group2);
    ASSERT_EQ(0u, group1->_storage->_chunks.size());
    ASSERT_EQ(0u, group2->_storage->_chunks.size());

    allocator1->grow();
    ASSERT_EQ(VectorStorage::CHUNKSIZE, allocator1->_capacity);
    ASSERT_EQ(1u, group1->_storage->_chunks.size());
    ASSERT_EQ(1u, group2->_storage->_chunks.size());

    allocator1->grow();
    ASSERT_EQ(VectorStorage::CHUNKSIZE * 2, allocator1->_capacity);
    ASSERT_EQ(2u, group1->_storage->_chunks.size());
    ASSERT_EQ(2u, group2->_storage->_chunks.size());
}

TEST_F(MatchDocAllocatorTest, testMergeMountInfo) {
    {
        MountInfoPtr mountInfo;
        ASSERT_TRUE(allocator.mergeMountInfo(mountInfo));
    }
    {
        MountInfoPtr mountInfo(new MountInfo());
        ASSERT_TRUE(mountInfo->insert("f1", 0, 0));
        ASSERT_TRUE(mountInfo->insert("f2", 0, 4));
        ASSERT_TRUE(mountInfo->insert("f3", 1, 0));
        ASSERT_TRUE(mountInfo->insert("f4", 2, 0));
        ASSERT_TRUE(allocator.mergeMountInfo(mountInfo));
        ASSERT_EQ(4u, allocator.getMountInfo()->getMountMap().size());
        allocator.declare<int32_t>("f1");
        allocator.declare<int32_t>("f2");
        allocator.declare<int32_t>("f3");
        allocator.declare<int32_t>("f4");

        MountInfoPtr otherMountInfo(new MountInfo());
        ASSERT_TRUE(otherMountInfo->insert("f1", 0, 0));
        ASSERT_TRUE(otherMountInfo->insert("f5", 0, 4));

        ASSERT_TRUE(allocator.mergeMountInfo(otherMountInfo));
        ASSERT_EQ(5u, allocator.getMountInfo()->getMountMap().size());

        MatchDocAllocator::ReferenceAliasMapPtr aliasMap(new MatchDocAllocator::ReferenceAliasMap());
        aliasMap->insert(make_pair("f6", "f1"));
        allocator.setReferenceAliasMap(aliasMap);
        MountInfoPtr otherMountInfo2(new MountInfo());
        ASSERT_TRUE(otherMountInfo2->insert("f6", 0, 0));
        ASSERT_TRUE(allocator.mergeMountInfo(otherMountInfo2));
        ASSERT_EQ(5u, allocator.getMountInfo()->getMountMap().size());

        MountInfoPtr otherMountInfo3(new MountInfo());
        ASSERT_TRUE(otherMountInfo3->insert("f5", 0, 0));
        ASSERT_FALSE(allocator.mergeMountInfo(otherMountInfo3));
    }
    {
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool));
        set<std::string> appendFields = {"f1", "f2"};
        MountInfoPtr mountInfo;
        ASSERT_TRUE(allocator->mergeMountInfo(mountInfo, appendFields));
    }
    {
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool));
        set<std::string> appendFields;
        MountInfoPtr mountInfo(new MountInfo());
        mountInfo->insert("f1", 0, 0);
        mountInfo->insert("f2", 0, 4);
        mountInfo->insert("f3", 0, 6);
        ASSERT_TRUE(allocator->mergeMountInfo(mountInfo, appendFields));
        ASSERT_EQ(3u, allocator->getMountInfo()->getMountMap().size());
    }
    {
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool));
        set<std::string> appendFields = {"f1", "f2"};
        MountInfoPtr mountInfo(new MountInfo());
        mountInfo->insert("f1", 0, 0);
        mountInfo->insert("f2", 0, 4);
        mountInfo->insert("f3", 0, 6);
        ASSERT_TRUE(allocator->mergeMountInfo(mountInfo, appendFields));
        ASSERT_EQ(2u, allocator->getMountInfo()->getMountMap().size());
    }
}

TEST_F(MatchDocAllocatorTest, testFillDocidWithSubDoc) {
    Reference<FiveBytesString> *ref1 = allocator.declareSub<FiveBytesString>("ref1", 0, 5);
    ASSERT_TRUE(ref1);
    allocator.extend();
    allocator.extendSub();
    EXPECT_EQ((size_t)5 + sizeof(MatchDoc), allocator.getSubDocSize());
    std::vector<int32_t> subdocIds1 = {1, 3, 5};
    std::vector<int32_t> subdocIds2 = {2, 4, 6};
    std::vector<matchdoc::MatchDoc> subdocVec1, subdocVec2;
    MatchDoc doc1 = allocator.batchAllocateSubdoc(2, subdocVec1, 1);
    MatchDoc doc2 = allocator.batchAllocateSubdoc(4, subdocVec2, 3);
    FakeDocIdIterator iterator1(doc1.getDocId(), subdocIds1);
    FakeDocIdIterator iterator2(doc2.getDocId(), subdocIds2);

    allocator.fillDocidWithSubDoc(&iterator1, doc1, subdocVec1);
    allocator.fillDocidWithSubDoc(&iterator2, doc2, subdocVec2);
    const char *value1 = "12345";
    const char *value2 = "abcde";
    ref1->set(doc1, *((FiveBytesString *)(value1)));
    ref1->set(doc2, *((FiveBytesString *)(value2)));
    EXPECT_EQ(string("12345"), ref1->getPointer(doc1)->toString());
    EXPECT_EQ(string("abcde"), ref1->getPointer(doc2)->toString());

    std::map<int32_t, std::vector<int32_t>> docs;
    SubCollector subCollector(allocator.getSubDocRef(), docs);
    allocator.getSubDocAccessor()->foreach (doc1, subCollector);
    allocator.getSubDocAccessor()->foreach (doc2, subCollector);

    size_t filledDocNum1 = min(subdocIds1.size(), subdocVec1.size());
    size_t filledDocNum2 = min(subdocIds2.size(), subdocVec2.size());
    subdocIds1.resize(filledDocNum1);
    subdocIds2.resize(filledDocNum2);
    EXPECT_EQ(docs[doc1.getDocId()], subdocIds1);
    EXPECT_EQ(docs[doc2.getDocId()], subdocIds2);
}

TEST_F(MatchDocAllocatorTest, testFillDocidWithoutSubDoc) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);

    EXPECT_EQ(0u, allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());

    vector<MatchDoc> matchDocs;
    MatchDoc doc = allocator.batchAllocateSubdoc(0, matchDocs, -1);
    EXPECT_EQ(sizeof(string) + sizeof(int), allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());
    EXPECT_EQ(0u, matchDocs.size());

    std::vector<int32_t> subdocIds = {2, 4, 6};
    int32_t docid = 10;
    FakeDocIdIterator iterator(docid, subdocIds);
    allocator.fillDocidWithSubDoc(&iterator, doc, matchDocs);
    EXPECT_EQ(docid, doc.getDocId());
    EXPECT_EQ(0u, matchDocs.size());
}

TEST_F(MatchDocAllocatorTest, testFillDocidWithSubDocTwice) {
    Reference<FiveBytesString> *ref1 = allocator.declareSub<FiveBytesString>("ref1", 0, 5);
    ASSERT_TRUE(ref1);
    allocator.extend();
    allocator.extendSub();
    EXPECT_EQ((size_t)5 + sizeof(MatchDoc), allocator.getSubDocSize());
    std::vector<int32_t> subdocIds1 = {1, 2, 3, 4};
    std::map<int32_t, std::vector<int32_t>> docs;
    std::vector<matchdoc::MatchDoc> subdocVec;

    MatchDoc doc1 = allocator.batchAllocateSubdoc(4, subdocVec, 1);
    FakeDocIdIterator iterator1(doc1.getDocId(), subdocIds1);
    allocator.fillDocidWithSubDoc(&iterator1, doc1, subdocVec);
    const char *value1 = "12345";
    ref1->set(doc1, *((FiveBytesString *)(value1)));
    EXPECT_EQ(string("12345"), ref1->getPointer(doc1)->toString());
    EXPECT_EQ(subdocIds1.size(), subdocVec.size());

    SubCollector subCollector(allocator.getSubDocRef(), docs);
    allocator.getSubDocAccessor()->foreach (doc1, subCollector);
    size_t filledDocNum1 = min(subdocIds1.size(), subdocVec.size());
    subdocIds1.resize(filledDocNum1);
    EXPECT_EQ(docs[doc1.getDocId()], subdocIds1);

    {
        std::vector<int32_t> subdocIds2 = {5, 6};
        MatchDoc doc2(0, 3);
        FakeDocIdIterator iterator2(doc2.getDocId(), subdocIds2);
        allocator.fillDocidWithSubDoc(&iterator2, doc2, subdocVec);
        size_t maxSize = subdocIds1.size();
        EXPECT_EQ(maxSize, subdocVec.size());

        const char *value2 = "abcde";
        ref1->set(doc2, *((FiveBytesString *)(value2)));
        EXPECT_EQ(string("abcde"), ref1->getPointer(doc2)->toString());
        allocator.getSubDocAccessor()->foreach (doc2, subCollector);
        size_t filledDocNum2 = min(subdocIds2.size(), subdocVec.size());
        subdocIds2.resize(filledDocNum2);
        EXPECT_EQ(docs[doc2.getDocId()], subdocIds2);
        for (size_t i = 0; i < filledDocNum2; ++i) {
            EXPECT_EQ(subdocIds2[i], subdocVec[i].getDocId());
        }
        for (size_t i = filledDocNum2; i < maxSize; ++i) {
            EXPECT_EQ(subdocIds1[i], subdocVec[i].getDocId());
        }
    }

    {
        std::vector<int32_t> subdocIds2 = {5, 6, 7, 8, 9};
        MatchDoc doc2(0, 4);
        FakeDocIdIterator iterator2(doc2.getDocId(), subdocIds2);
        allocator.fillDocidWithSubDoc(&iterator2, doc2, subdocVec);
        size_t maxSize = subdocIds1.size();
        EXPECT_EQ(maxSize, subdocVec.size());

        const char *value2 = "abcde";
        ref1->set(doc2, *((FiveBytesString *)(value2)));
        EXPECT_EQ(string("abcde"), ref1->getPointer(doc2)->toString());
        allocator.getSubDocAccessor()->foreach (doc2, subCollector);
        size_t filledDocNum2 = min(subdocIds2.size(), subdocVec.size());
        subdocIds2.resize(filledDocNum2);
        EXPECT_EQ(docs[doc2.getDocId()], subdocIds2);
        for (size_t i = 0; i < filledDocNum2; ++i) {
            EXPECT_EQ(subdocIds2[i], subdocVec[i].getDocId());
        }
        for (size_t i = filledDocNum2; i < maxSize; ++i) {
            EXPECT_EQ(subdocIds1[i], subdocVec[i].getDocId());
        }
    }
}

TEST_F(MatchDocAllocatorTest, testGetMountValue) {
    MountInfoPtr mountInfo(new MountInfo());
    ASSERT_TRUE(mountInfo->insert("int32", 0, 0));
    MatchDocAllocator allocator(pool, false, mountInfo);

    auto field = allocator.declare<int32_t>("int32");
    ASSERT_TRUE(field);
    ASSERT_TRUE(field->isMount());
    auto doc = allocator.allocate(-1);

    int32_t mountValue = 32;
    field->mount(doc, (char *)&mountValue);

    int32_t value0 = field->get(doc);
    ASSERT_EQ(mountValue, value0);

    int32_t value1 = field->getMountValue(doc);
    ASSERT_EQ(mountValue, value1);
}

TEST_F(MatchDocAllocatorTest, testIgnoreUselessGroupMeta) {
    Reference<int> *field1 = allocator.declare<int>("field1", DEFAULT_GROUP, 1);
    ASSERT_TRUE(field1);
    ASSERT_EQ(size_t(0), allocator._fieldGroups.size());
    ASSERT_EQ(size_t(1), allocator._fieldGroupBuilders.size());
    allocator.extend();

    Reference<string> *field2 = allocator.declare<string>("field2", "default_test");
    ASSERT_TRUE(field2);
    ASSERT_EQ(size_t(1), allocator._fieldGroups.size());
    ASSERT_EQ(size_t(1), allocator._fieldGroupBuilders.size());

    allocator.extend();
    ASSERT_EQ(size_t(2), allocator._fieldGroups.size());
    ASSERT_EQ(size_t(0), allocator._fieldGroupBuilders.size());

    MatchDoc doc1 = allocator.allocate();
    field1->set(doc1, 1);
    field2->set(doc1, "1");
    MatchDoc doc2 = allocator.allocate();
    field1->set(doc2, 2);
    field2->set(doc2, "2");

    vector<MatchDoc> docs;
    docs.push_back(doc1);
    docs.push_back(doc2);
    DataBuffer dataBuffer;
    allocator.serialize(dataBuffer, docs, matchdoc::DEFAULT_SERIALIZE_LEVEL, true);
    string data;
    data.assign(dataBuffer.getData(), dataBuffer.getDataLen());

    // deserialize
    DataBuffer dataBuffer2((void *)data.c_str(), data.size());
    MatchDocAllocatorPtr allocator2(new MatchDocAllocator(pool));
    docs.clear();
    allocator2->deserialize(dataBuffer2, docs);
    MatchDocTestHelper::checkSerializeLevel(allocator2, 0, "field1:1");
    MatchDocTestHelper::checkSerializeLevel(allocator2, 1, "field1:1");
}

TEST_F(MatchDocAllocatorTest, testDropMountMetaIfExist) {
    {
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool));
        Reference<int32_t> *f1 = allocator->declare<int32_t>("f1", "g1");
        EXPECT_TRUE(f1);
        allocator->dropField("f1", true);
        EXPECT_TRUE(allocator->findReferenceWithoutType("f1") == nullptr);
    }
    {
        MountInfoPtr mountInfo(new MountInfo());
        ASSERT_TRUE(mountInfo->insert("f1", 0, 0));
        ASSERT_TRUE(mountInfo->insert("f2", 0, 4));
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
        Reference<int32_t> *f1 = allocator->declare<int32_t>("f1", "g1");
        Reference<int32_t> *f2 = allocator->declare<int32_t>("f2", "g1");
        ASSERT_TRUE(allocator->getMountInfo() == mountInfo);
        ASSERT_TRUE(f1 && f1->isMount());
        ASSERT_TRUE(f2 && f2->isMount());
        allocator->dropField("f1", false);
        ASSERT_TRUE(allocator->getMountInfo() == mountInfo);
        f1 = allocator->declare<int32_t>("f1", "g1");
        ASSERT_TRUE(f1 && f1->isMount());
    }
    {
        MountInfoPtr mountInfo(new MountInfo());
        ASSERT_TRUE(mountInfo->insert("f1", 0, 0));
        ASSERT_TRUE(mountInfo->insert("f2", 0, 4));
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
        Reference<int32_t> *f1 = allocator->declare<int32_t>("f1", "g1");
        Reference<int32_t> *f2 = allocator->declare<int32_t>("f2", "g1");
        ASSERT_TRUE(allocator->getMountInfo() == mountInfo);
        ASSERT_TRUE(f1 && f1->isMount());
        ASSERT_TRUE(f2 && f2->isMount());
        allocator->dropField("f1", true);
        ASSERT_TRUE(allocator->getMountInfo() != mountInfo);
        ASSERT_TRUE(allocator->getMountInfo()->get("f1") == nullptr);
        f1 = allocator->declare<int32_t>("f1", "g1");
        ASSERT_TRUE(f1 && !f1->isMount());
    }
    {
        MountInfoPtr mountInfo(new MountInfo());
        ASSERT_TRUE(mountInfo->insert("f1", 0, 0));
        ASSERT_TRUE(mountInfo->insert("f2", 0, 4));
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
        ASSERT_TRUE(allocator->getMountInfo() == mountInfo);
        allocator->dropField("f1", true); // force drop mount info
        ASSERT_EQ(1u, allocator->getMountInfo()->getMountMap().size());
    }
    {
        MountInfoPtr mountInfo(new MountInfo());
        ASSERT_TRUE(mountInfo->insert("f1", 0, 0));
        ASSERT_TRUE(mountInfo->insert("f2", 0, 4));

        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
        allocator->declare<int32_t>("f1", "g1");
        allocator->declare<int32_t>("f2", "g1");
        allocator->extend();
        allocator->setReferenceAliasMap(
            MatchDocAllocator::ReferenceAliasMapPtr(new MatchDocAllocator::ReferenceAliasMap({{"aa", "f1"}})));
        ASSERT_TRUE(allocator->getMountInfo() == mountInfo);
        allocator->dropField("aa", true); // force drop mount info
        ASSERT_EQ(1u, allocator->getMountInfo()->getMountMap().size());
    }
}

TEST_F(MatchDocAllocatorTest, testMergeDocs) {
    {
        // allocator1
        MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
        std::vector<MatchDoc> docs1;
        MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,int_c:2,int_b:3");
        docs1.push_back(doc11);
        MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:4,int_c:5,int_b:6");
        docs1.push_back(doc12);

        // allocator2
        MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
        std::vector<MatchDoc> docs2;
        MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,int_c:12,int_b:13");
        docs2.push_back(doc21);
        MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,int_c:15,int_b:16");
        docs2.push_back(doc22);

        allocator1->mergeDocs(allocator2.get(), docs2, docs1);
        ASSERT_EQ(4u, docs1.size());
        auto refVec = allocator1->getAllNeedSerializeReferences(0);
        for (size_t i = 0; i < 4; i++) {
            for (auto ref : refVec) {
                auto doc = docs1[i];
                std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex()
                          << ", field: " << ref->getName() << ", toString: " << ref->toString(doc) << std::endl;
            }
        }
    }
    {
        // allocator1
        MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("", pool);
        std::vector<MatchDoc> docs1;
        MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "");
        docs1.push_back(doc11);
        MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "");
        docs1.push_back(doc12);

        // allocator2
        MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("", pool);
        std::vector<MatchDoc> docs2;
        MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "");
        docs2.push_back(doc21);
        MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "");
        docs2.push_back(doc22);

        allocator1->mergeDocs(allocator2.get(), docs2, docs1);
        ASSERT_EQ(4u, docs1.size());
        auto refVec = allocator1->getAllNeedSerializeReferences(0);
        for (size_t i = 0; i < 4; i++) {
            for (auto ref : refVec) {
                auto doc = docs1[i];
                std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex()
                          << ", field: " << ref->getName() << ", toString: " << ref->toString(doc) << std::endl;
            }
        }
    }
}

TEST_F(MatchDocAllocatorTest, testAppendDocs) {
    {
        // allocator1
        MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
        std::vector<MatchDoc> docs1;
        MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,int_c:2,int_b:3");
        docs1.push_back(doc11);
        MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:4,int_c:5,int_b:6");
        docs1.push_back(doc12);

        // allocator2
        MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
        std::vector<MatchDoc> docs2;
        MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,int_c:12,int_b:13");
        docs2.push_back(doc21);
        MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,int_c:15,int_b:16");
        docs2.push_back(doc22);

        allocator1->appendDocs(allocator2.get(), docs2, docs1);
        ASSERT_EQ(4u, docs1.size());
        auto refVec = allocator1->getAllNeedSerializeReferences(0);
        for (size_t i = 0; i < 4; i++) {
            for (auto ref : refVec) {
                auto doc = docs1[i];
                std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex()
                          << ", field: " << ref->getName() << ", toString: " << ref->toString(doc) << std::endl;
            }
        }
    }
    {
        // allocator1
        MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("", pool);
        std::vector<MatchDoc> docs1;
        MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "");
        docs1.push_back(doc11);
        MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "");
        docs1.push_back(doc12);

        // allocator2
        MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("", pool);
        std::vector<MatchDoc> docs2;
        MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "");
        docs2.push_back(doc21);
        MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "");
        docs2.push_back(doc22);

        allocator1->appendDocs(allocator2.get(), docs2, docs1);
        ASSERT_EQ(4u, docs1.size());
        auto refVec = allocator1->getAllNeedSerializeReferences(0);
        for (size_t i = 0; i < 4; i++) {
            for (auto ref : refVec) {
                auto doc = docs1[i];
                std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex()
                          << ", field: " << ref->getName() << ", toString: " << ref->toString(doc) << std::endl;
            }
        }
    }
}

TEST_F(MatchDocAllocatorTest, testMergeAllocatorOnlySame) {
    {
        // allocator1
        MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
        std::vector<MatchDoc> docs1;
        MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:1,int_c:2,int_b:3");
        docs1.push_back(doc11);
        MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "int_a:4,int_c:5,int_b:6");
        docs1.push_back(doc12);

        // allocator2
        MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("g1:int_a,g2:int_b,g3:int_c", pool);
        std::vector<MatchDoc> docs2;
        MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:11,int_c:12,int_b:13");
        docs2.push_back(doc21);
        MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "int_a:14,int_c:15,int_b:16");
        docs2.push_back(doc22);

        ASSERT_TRUE(allocator1->mergeAllocatorOnlySame(allocator2.get(), docs2, docs1));
        ASSERT_EQ(4u, docs1.size());
        auto refVec = allocator1->getAllNeedSerializeReferences(0);
        for (size_t i = 0; i < 4; i++) {
            for (auto ref : refVec) {
                auto doc = docs1[i];
                std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex()
                          << ", field: " << ref->getName() << ", toString: " << ref->toString(doc) << std::endl;
            }
        }
    }
    {
        // allocator1
        MatchDocAllocatorPtr allocator1 = MatchDocTestHelper::createAllocator("", pool);
        std::vector<MatchDoc> docs1;
        MatchDoc doc11 = MatchDocTestHelper::createMatchDoc(allocator1, "");
        docs1.push_back(doc11);
        MatchDoc doc12 = MatchDocTestHelper::createMatchDoc(allocator1, "");
        docs1.push_back(doc12);

        // allocator2
        MatchDocAllocatorPtr allocator2 = MatchDocTestHelper::createAllocator("", pool);
        std::vector<MatchDoc> docs2;
        MatchDoc doc21 = MatchDocTestHelper::createMatchDoc(allocator2, "");
        docs2.push_back(doc21);
        MatchDoc doc22 = MatchDocTestHelper::createMatchDoc(allocator2, "");
        docs2.push_back(doc22);

        ASSERT_TRUE(allocator1->mergeAllocatorOnlySame(allocator2.get(), docs2, docs1));
        ASSERT_EQ(4u, docs1.size());
        auto refVec = allocator1->getAllNeedSerializeReferences(0);
        for (size_t i = 0; i < 4; i++) {
            for (auto ref : refVec) {
                auto doc = docs1[i];
                std::cout << "docId: " << doc.getDocId() << ", index: " << doc.getIndex()
                          << ", field: " << ref->getName() << ", toString: " << ref->toString(doc) << std::endl;
            }
        }
    }
}

TEST_F(MatchDocAllocatorTest, test) {
    MatchDocAllocator allocator(pool);
    ASSERT_TRUE(allocator.declare<int>("a_1", SL_NONE));
    ASSERT_TRUE(allocator.declare<int>("a_2", SL_ATTRIBUTE));
    ASSERT_TRUE(allocator.declare<int>("b_1", SL_NONE));
    ASSERT_TRUE(allocator.declare<int>("c_1", SL_ATTRIBUTE));
    ASSERT_TRUE(allocator.declareSub<int>("d_1", SL_ATTRIBUTE));
    ASSERT_TRUE(allocator.declareSub<int>("e_1", SL_NONE));
    ASSERT_TRUE(allocator.declareSub<int>("f_1", SL_ATTRIBUTE));
    ASSERT_TRUE(allocator.declareSub<int>("f_2", SL_NONE));
    allocator.extend();
    allocator.extendSub();
    allocator.allocate();
    ASSERT_EQ(6, allocator.getReferenceCount());
    ASSERT_EQ(5, allocator.getSubReferenceCount());
    allocator.reserveFields(SL_ATTRIBUTE);
    ASSERT_EQ(4, allocator.getReferenceCount());
    ASSERT_EQ(3, allocator.getSubReferenceCount());
}

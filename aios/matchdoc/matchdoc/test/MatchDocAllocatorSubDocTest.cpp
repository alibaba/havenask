#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <functional>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"
#include "matchdoc/toolkit/MatchDocTestHelper.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace testing;

class MatchDocAllocatorSubDocTest : public testing::Test {
public:
    MatchDocAllocatorSubDocTest() : pool(std::make_shared<autil::mem_pool::Pool>()), allocator(pool) {}

protected:
    std::shared_ptr<autil::mem_pool::Pool> pool;
    MatchDocAllocator allocator;
};

TEST_F(MatchDocAllocatorSubDocTest, testClean) {
    Reference<int> *field1 = allocator.declare<int>("field1", "group1");
    Reference<int> *subfield1 = allocator.declareSub<int>("subfield1", "sub_group1");
    Reference<uint64_t> *subfield2 = allocator.declareSub<uint64_t>("subfield2", "sub_group2");
    allocator.extend();
    allocator.extendSub();

    MatchDoc doc1 = allocator.allocate();
    field1->set(doc1, 1);

    allocator.allocateSub(doc1);
    subfield1->set(doc1, 2);
    subfield2->set(doc1, 2);

    allocator.allocateSub(doc1);
    subfield1->set(doc1, 3);
    subfield2->set(doc1, 3);

    SubCounter counter;
    allocator.getSubDocAccessor()->foreach (doc1, counter);
    EXPECT_EQ(uint32_t(2), counter.get());

    unordered_set<string> keepFieldGroups;
    keepFieldGroups.insert("sub_group2");
    allocator.clean(keepFieldGroups);
    field1 = allocator.declare<int>("field1", "group1");
    subfield1 = allocator.findReference<int>("subfield1");
    ASSERT_FALSE(subfield1);
    subfield2 = allocator.findReference<uint64_t>("subfield2");
    ASSERT_TRUE(subfield2);
    allocator.extend();
    allocator.extendSub();

    doc1 = allocator.allocate();
    allocator.allocateSub(doc1);
    MatchDoc subMatchDoc = allocator._currentSub->getReference(doc1);
    ASSERT_EQ(0u, subMatchDoc.index);
    subfield2->set(doc1, 1);
    allocator.allocateSub(doc1);
    subfield2->set(doc1, 2);
    SubCounter counter1;
    allocator.getSubDocAccessor()->foreach (doc1, counter1);
    EXPECT_EQ(uint32_t(2), counter1.get());
}

TEST_F(MatchDocAllocatorSubDocTest, testSubInitPoolPtr) {
    std::shared_ptr<autil::mem_pool::Pool> poolPtr(new autil::mem_pool::Pool());
    MatchDocAllocator allocator1(poolPtr, true);
    EXPECT_TRUE(allocator1._subAllocator);
    EXPECT_EQ(poolPtr, allocator1._subAllocator->_poolPtr);
}

TEST_F(MatchDocAllocatorSubDocTest, testDeclareAndAllocateSub) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = allocator.declare<string>("field2");
    ASSERT_TRUE(field2);

    Reference<int> *subfield1 = allocator.declareSub<int>("subfield1");
    ASSERT_TRUE(subfield1);
    Reference<string> *subfield2 = allocator.declareSub<string>("subfield2");
    ASSERT_TRUE(subfield2);

    EXPECT_EQ(0u, allocator.getDocSize());
    EXPECT_EQ(0u, allocator.getSubDocSize());

    allocator.extend();
    allocator.extendSub();

    EXPECT_EQ(sizeof(string) + 2 * sizeof(MatchDoc) + sizeof(int), allocator.getDocSize());
    EXPECT_EQ(sizeof(string) + sizeof(MatchDoc) + sizeof(int), allocator.getSubDocSize());

    MatchDoc doc1 = allocator.allocate();
    field1->set(doc1, 1);
    field2->set(doc1, "1");

    MatchDoc subDoc1 = allocator.allocateSub(doc1);
    ASSERT_EQ((int32_t)-1, subDoc1.getDocId());
    subfield1->set(doc1, 2);
    subfield2->set(doc1, "2");

    MatchDoc subDoc2 = allocator.allocateSub(doc1, 10);
    ASSERT_EQ((int32_t)10, subDoc2.getDocId());
    subfield1->set(doc1, 3);
    subfield2->set(doc1, "3");

    SubCounter counter;
    allocator.getSubDocAccessor()->foreach (doc1, counter);
    EXPECT_EQ(uint32_t(2), counter.get());
}

TEST_F(MatchDocAllocatorSubDocTest, testDeclareWithNameConflict) {
    Reference<int> *field1 = allocator.declare<int>("field1");
    EXPECT_TRUE(field1);
    Reference<int> *field2 = allocator.declareSub<int>("field1");
    EXPECT_FALSE(field2);
}

TEST_F(MatchDocAllocatorSubDocTest, testFindSubReference) {
    Reference<int> *field1 = allocator.declareSub<int>("field1");
    EXPECT_TRUE(field1);
    Reference<int> *field2 = allocator.declareSub<int>("field1");
    EXPECT_EQ(field1, field2);
    Reference<int> *field3 = allocator.findReference<int>("field1");
    EXPECT_EQ(field1, field3);
}

TEST_F(MatchDocAllocatorSubDocTest, testCloneVariableSlab) {
    Reference<int32_t> *ref1 = allocator.declare<int32_t>("field1");
    Reference<string> *ref2 = allocator.declare<string>("field2");
    allocator.extend();

    MatchDoc doc1 = allocator.allocate();
    ref1->getReference(doc1) = 10;
    ref2->getReference(doc1) = "1234";
    MatchDoc doc2 = allocator.allocateAndClone(doc1);
    EXPECT_EQ(int32_t(10), ref1->get(doc2));
    EXPECT_EQ(string("1234"), ref2->get(doc2));
}

class SubCollector {
public:
    SubCollector(Reference<int> *subfield1Ref_, Reference<string> *subfield2Ref_, Reference<MatchDoc> *currentSubRef_)
        : subfield1Ref(subfield1Ref_), subfield2Ref(subfield2Ref_), currentSubRef(currentSubRef_) {}
    void operator()(MatchDoc doc) {
        subfield1.push_back(subfield1Ref->get(doc));
        subfield2.push_back(subfield2Ref->get(doc));
        docIds.push_back(currentSubRef->get(doc).getDocId());
    }
    Reference<int> *subfield1Ref;
    Reference<string> *subfield2Ref;
    Reference<MatchDoc> *currentSubRef;

    vector<int> subfield1;
    vector<string> subfield2;
    vector<int32_t> docIds;
};

class SubDocIdChecker {
public:
    // expectValueStr : 1,2,3
    SubDocIdChecker(Reference<MatchDoc> *currentRef, const std::string &expectIdStr) : _curRef(currentRef) {
        StringUtil::fromString(expectIdStr, _expectIds, ",");
    }

public:
    void operator()(MatchDoc doc) { _ids.push_back(_curRef->get(doc).getDocId()); }

    void check() { EXPECT_EQ(_expectIds, _ids); }

private:
    Reference<MatchDoc> *_curRef;
    vector<int32_t> _expectIds;
    vector<int32_t> _ids;
};

TEST_F(MatchDocAllocatorSubDocTest, testSerializeWithLevel) {
    // serialize
    MatchDocAllocatorPtr allocator =
        MatchDocTestHelper::createAllocator("group_1:int_a,group_2:int_b,group_2:int_c,group_3:int_d,"
                                            "sub_grp1:sub_int_a,sub_grp2:sub_int_b,sub_grp3:sub_int_c",
                                            pool);

    MatchDocTestHelper::setSerializeLevel(allocator,
                                          "int_a:0,int_b:10,int_c:1,int_d:3,"
                                          "sub_int_a:0,sub_int_b:10,sub_int_c:3");

    vector<MatchDoc> docs;
    docs.push_back(MatchDocTestHelper::createMatchDoc(allocator,
                                                      "0,1^3^5#int_a:1,int_b:2,int_c:3,int_d:4,"
                                                      "sub_int_a:11^12^13,sub_int_b:21^22^23,sub_int_c:31^32^33"));
    docs.push_back(MatchDocTestHelper::createMatchDoc(allocator,
                                                      "2,7^9#int_a:5,int_b:6,int_c:7,int_d:8,"
                                                      "sub_int_a:51^52,sub_int_b:61^62,sub_int_c:71^72"));

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

        MatchDocTestHelper::checkSerializeLevel(allocator2,
                                                sl,
                                                "int_a:0,int_b:10,int_c:1,int_d:3,"
                                                "sub_int_a:0,sub_int_b:10,sub_int_c:3");
        MatchDocTestHelper::checkDeserializedDocValue(allocator2,
                                                      docs[0],
                                                      sl,
                                                      "int_a:1,int_b:2,int_c:3,int_d:4,"
                                                      "sub_int_a:11^12^13,sub_int_b:21^22^23,sub_int_c:31^32^33");
        MatchDocTestHelper::checkDeserializedDocValue(allocator2,
                                                      docs[1],
                                                      sl,
                                                      "int_a:5,int_b:6,int_c:7,int_d:8,"
                                                      "sub_int_a:51^52,sub_int_b:61^62,sub_int_c:71^72");

        Reference<MatchDoc> *curSubRef = allocator2->findReference<MatchDoc>(CURRENT_SUB_REF);
        SubDocAccessor *subAccessor = allocator2->getSubDocAccessor();
        {
            ASSERT_EQ((int32_t)0, docs[0].getDocId());
            SubDocIdChecker checker(curSubRef, "1,3,5");
            subAccessor->foreach (docs[0], checker);
            checker.check();
        }
        {
            ASSERT_EQ((int32_t)2, docs[1].getDocId());
            SubDocIdChecker checker(curSubRef, "7,9");
            subAccessor->foreach (docs[1], checker);
            checker.check();
        }
    }
}

TEST_F(MatchDocAllocatorSubDocTest, testSerialize) {
    string data;
    {
        MatchDocAllocator allocator1(pool, true);
        Reference<int> *field1 = allocator1.declare<int>("field1");
        ASSERT_TRUE(field1);
        Reference<string> *field2 = allocator1.declare<string>("field2");
        ASSERT_TRUE(field2);
        Reference<int> *subfield1 = allocator1.declareSub<int>("subfield1");
        ASSERT_TRUE(subfield1);
        Reference<string> *subfield2 = allocator1.declareSub<string>("subfield2");
        ASSERT_TRUE(subfield2);
        allocator1.extend();
        allocator1.extendSub();

        vector<MatchDoc> docs;
        int maincount = 3;
        int subcount = 4;
        for (int i = 0; i < maincount; i++) {
            MatchDoc doc1 = allocator1.allocate(i);
            field1->set(doc1, i);
            field2->set(doc1, StringUtil::toString(i));
            docs.push_back(doc1);
            for (int j = 0; j < subcount; j++) {
                allocator1.allocateSub(doc1);
                subfield1->set(doc1, j);
                subfield2->set(doc1, StringUtil::toString(j));
            }
        }
        DataBuffer dataBuffer;
        allocator1.serialize(dataBuffer, docs, 0);
        data.assign(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    {
        DataBuffer dataBuffer((void *)data.c_str(), data.size());
        MatchDocAllocator allocator2(pool, true);
        vector<MatchDoc> docs;
        allocator2.deserialize(dataBuffer, docs);
        Reference<int> *field1 = allocator2.findReference<int>("field1");
        ASSERT_TRUE(field1);
        Reference<string> *field2 = allocator2.findReference<string>("field2");
        ASSERT_TRUE(field2);
        Reference<int> *subfield1 = allocator2.findReference<int>("subfield1");
        ASSERT_TRUE(subfield1);
        Reference<string> *subfield2 = allocator2.findReference<string>("subfield2");
        ASSERT_TRUE(subfield2);
        Reference<MatchDoc> *currentSubRef = allocator2.getCurrentSubDocRef();

        ASSERT_EQ(size_t(3), docs.size());
        for (int i = 0; i < int(docs.size()); i++) {
            EXPECT_EQ((int32_t)i, docs[i].getDocId());
            EXPECT_EQ(i, field1->get(docs[i]));
            EXPECT_EQ(StringUtil::toString(i), field2->get(docs[i]));
            SubCollector collector(subfield1, subfield2, currentSubRef);
            allocator2.getSubDocAccessor()->foreach (docs[i], collector);
            EXPECT_THAT(collector.subfield1, UnorderedElementsAre(0, 1, 2, 3));
            EXPECT_THAT(collector.subfield2, UnorderedElementsAre("0", "1", "2", "3"));
        }
    }
    {
        std::shared_ptr<autil::mem_pool::Pool> poolPtr(new autil::mem_pool::Pool());
        DataBuffer dataBuffer((void *)data.c_str(), data.size());
        MatchDocAllocator allocator2(poolPtr, true);
        vector<MatchDoc> docs;
        allocator2.deserialize(dataBuffer, docs);
        Reference<int> *subfield = allocator2.findReference<int>("subfield1");
        ASSERT_TRUE(subfield);
        EXPECT_EQ(poolPtr, allocator2._subAllocator->_poolPtr);
        EXPECT_EQ(poolPtr.get(), allocator2._subAllocator->getSessionPool());
        EXPECT_EQ(poolPtr, subfield->_docStorage->_pool);
    }
}

TEST_F(MatchDocAllocatorSubDocTest, testAllocateAndCloneWithSub) {
    Reference<int> *field1 = allocator.declare<int>("field1", "group1");
    Reference<int> *subfield1 = allocator.declareSub<int>("subfield1", "sub_group1");
    Reference<string> *subfield2 = allocator.declareSub<string>("subfield2", "sub_group2");
    allocator.extend();
    allocator.extendSub();

    MatchDoc doc1 = allocator.allocate();
    field1->set(doc1, 1);

    allocator.allocateSub(doc1, 0);
    subfield1->set(doc1, 2);
    subfield2->set(doc1, "20");

    allocator.allocateSub(doc1, 1);
    subfield1->set(doc1, 3);
    subfield2->set(doc1, "30");

    allocator.allocateSub(doc1, 2);
    subfield1->set(doc1, 4);
    subfield2->set(doc1, "40");

    MatchDoc doc2 = allocator.allocateAndCloneWithSub(doc1);
    EXPECT_EQ(1, field1->get(doc2));

    auto *currentRef = allocator.getCurrentSubDocRef();
    auto *subDocAccesor = allocator.getSubDocAccessor();

    vector<int> subField1Values;
    vector<string> subField2Values;
    vector<MatchDoc> subDocs;
    auto tp = [&subDocs, currentRef, &subField1Values, &subField2Values, subfield1, subfield2](MatchDoc doc) {
        subDocs.push_back(currentRef->get(doc));
        subField1Values.push_back(subfield1->get(doc));
        subField2Values.push_back(subfield2->get(doc));
    };

    subField1Values.clear();
    subField2Values.clear();
    subDocs.clear();
    subDocAccesor->foreach (doc1, tp);
    EXPECT_EQ(3, subDocs.size());
    EXPECT_EQ(3, subField1Values.size());
    EXPECT_EQ(3, subField2Values.size());
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(i, subDocs[i].getDocId());
        EXPECT_EQ(i, subDocs[i].getIndex());
        EXPECT_EQ(i + 2, subField1Values[i]);
        EXPECT_EQ(autil::StringUtil::toString((i + 2) * 10), subField2Values[i]);
    }

    subField1Values.clear();
    subField2Values.clear();
    subDocs.clear();
    subDocAccesor->foreach (doc2, tp);
    EXPECT_EQ(3, subDocs.size());
    EXPECT_EQ(3, subField1Values.size());
    EXPECT_EQ(3, subField2Values.size());
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(i, subDocs[i].getDocId());
        EXPECT_EQ(i + 3, subDocs[i].getIndex());
        EXPECT_EQ(i + 2, subField1Values[i]);
        EXPECT_EQ(autil::StringUtil::toString((i + 2) * 10), subField2Values[i]);
    }
}

TEST_F(MatchDocAllocatorSubDocTest, testAllocateAndCloneWithSubButNoSub) {
    Reference<int> *field1 = allocator.declare<int>("field1", "group1");
    allocator.extend();

    MatchDoc doc1 = allocator.allocate();
    field1->set(doc1, 1);

    MatchDoc doc2 = allocator.allocateAndCloneWithSub(doc1);
    EXPECT_EQ(1, field1->get(doc2));

    EXPECT_EQ(nullptr, allocator.getFirstSubDocRef());
    EXPECT_EQ(nullptr, allocator.getCurrentSubDocRef());
}

TEST_F(MatchDocAllocatorSubDocTest, testAllocateAndClone) {
    Reference<int> *field1 = allocator.declare<int>("field1", "group1");
    Reference<int> *subfield1 = allocator.declareSub<int>("subfield1", "sub_group1");
    Reference<string> *subfield2 = allocator.declareSub<string>("subfield2", "sub_group2");
    allocator.extend();
    allocator.extendSub();

    MatchDoc doc1 = allocator.allocate();
    field1->set(doc1, 1);

    allocator.allocateSub(doc1, 0);
    subfield1->set(doc1, 2);
    subfield2->set(doc1, "20");

    allocator.allocateSub(doc1, 1);
    subfield1->set(doc1, 3);
    subfield2->set(doc1, "30");

    allocator.allocateSub(doc1, 2);
    subfield1->set(doc1, 4);
    subfield2->set(doc1, "40");

    MatchDoc doc2 = allocator.allocateAndCloneWithSub(doc1);
    EXPECT_EQ(field1->get(doc1), field1->get(doc2));

    allocator.deallocate(doc1);
    allocator.deallocate(doc2);
}

TEST_F(MatchDocAllocatorSubDocTest, testNextSubRefCorrect) {
    allocator.declareSub<int>("sub");
    allocator.extend();
    allocator.extendSub();

    MatchDoc mainDoc = allocator.allocate();
    MatchDoc subDoc1 = allocator.allocateSub(mainDoc);
    MatchDoc subDoc2 = allocator.allocateSub(mainDoc);
    MatchDoc subDoc3 = allocator.allocateSub(mainDoc);

    Reference<MatchDoc> *nextRef = allocator.findReference<MatchDoc>(NEXT_SUB_REF);
    ASSERT_TRUE(nextRef != nullptr);
    Reference<MatchDoc> *firstRef = allocator.getFirstSubDocRef();
    MatchDoc curDoc = firstRef->get(mainDoc);
    ASSERT_EQ(subDoc1, curDoc);
    curDoc = nextRef->get(curDoc);
    ASSERT_EQ(subDoc2, curDoc);
    curDoc = nextRef->get(curDoc);
    ASSERT_EQ(subDoc3, curDoc);
}

TEST_F(MatchDocAllocatorSubDocTest, testCloneRefWithSub) {
    Reference<int32_t> *ref1 = allocator.declareSub<int32_t>("ref1", 0);
    ASSERT_TRUE(ref1);
    Reference<int32_t> *ref2 = dynamic_cast<Reference<int32_t> *>(allocator.cloneReference(ref1, "ref2"));
    ASSERT_TRUE(ref2);
    allocator.extend();
    EXPECT_EQ((size_t)16, allocator.getDocSize());
    MatchDoc doc1 = allocator.allocate(1);
    allocator.allocateSub(doc1, 1);
    ref1->set(doc1, 10);
    ASSERT_EQ(10, ref1->get(doc1));
    ref2->cloneConstruct(doc1, doc1, ref1);
    ASSERT_EQ(10, ref2->get(doc1));
}

TEST_F(MatchDocAllocatorSubDocTest, testCloneSubRefWithFlat) {
    Reference<int32_t> *ref1 = allocator.declareSub<int32_t>("ref1", 0);
    ASSERT_TRUE(ref1);
    Reference<int32_t> *ref2 = dynamic_cast<Reference<int32_t> *>(allocator.cloneReference(ref1, "ref2"));
    ASSERT_TRUE(ref2);
    allocator.extend();
    MatchDoc doc1 = allocator.allocate(1);
    allocator.allocateSub(doc1, 1);
    ref1->set(doc1, 10);
    allocator.allocateSub(doc1, 2);
    ref1->set(doc1, 11);
    MatchDoc doc2 = allocator.allocate(1);
    allocator.allocateSub(doc2, 3);
    ref1->set(doc2, 12);
    allocator.allocateSub(doc2, 4);
    ref1->set(doc2, 13);
    vector<MatchDoc> outputMatchDocs;
    std::function<void(MatchDoc)> processNothing = [&outputMatchDocs](MatchDoc newDoc) {
        outputMatchDocs.push_back(newDoc);
    };
    auto accessor = allocator.getSubDocAccessor();
    accessor->foreachFlatten(doc1, &allocator, processNothing);
    accessor->foreachFlatten(doc2, &allocator, processNothing);
    for (auto doc : outputMatchDocs) {
        ref2->cloneConstruct(doc, doc, ref1);
    }
    ASSERT_EQ(4, outputMatchDocs.size());
    for (size_t i = 0; i < outputMatchDocs.size(); i++) {
        ASSERT_EQ(10 + i, ref2->get(outputMatchDocs[i]));
    }
}

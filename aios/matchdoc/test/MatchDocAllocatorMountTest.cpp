#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"
#include "matchdoc/toolkit/MatchDocTestHelper.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace matchdoc;
using namespace testing;

class MatchDocAllocatorMountTest : public testing::Test {
public:
    void SetUp() override { pool = std::make_shared<Pool>(); }

protected:
    typedef MatchDocTestHelper TestHelper;

protected:
    std::shared_ptr<Pool> pool;
};

TEST_F(MatchDocAllocatorMountTest, testSimpleProcess) {
    MountInfoPtr mountInfo(new MountInfo);
    mountInfo->insert("int_a", 1, 0);
    mountInfo->insert("float_b", 1, 4);
    mountInfo->insert("int_c", 2, 0);
    mountInfo->insert("int_e", 1, 8);

    MatchDocAllocator allocator(pool, false, mountInfo);
    Reference<int> *ref_a = allocator.declare<int>("int_a", "group_a");
    Reference<float> *ref_b = allocator.declare<float>("float_b", "group_a");
    Reference<int> *ref_c = allocator.declare<int>("int_c", "group_a");
    Reference<string> *ref_d = allocator.declare<string>("string_d", "group_a");
    Reference<int> *ref_e = allocator.declare<int>("int_e", "group_b");
    allocator.extend();

    MatchDoc matchDoc = allocator.allocate();
    ref_d->set(matchDoc, string("hello_40"));

    char *data1 = TestHelper::createMountData("int:10,float:22.2,int:50", pool.get());
    char *data2 = TestHelper::createMountData("int:30", pool.get());

    ASSERT_TRUE(ref_a->isMount());
    ASSERT_TRUE(ref_b->isMount());
    ASSERT_TRUE(ref_c->isMount());
    ASSERT_FALSE(ref_d->isMount());
    ASSERT_TRUE(ref_e->isMount());

    ASSERT_TRUE(ref_a->mount(matchDoc, data1));
    ASSERT_TRUE(ref_b->mount(matchDoc, data1));
    ASSERT_TRUE(ref_c->mount(matchDoc, data2));
    ASSERT_FALSE(ref_d->mount(matchDoc, data1));
    ASSERT_TRUE(ref_e->mount(matchDoc, data1));

    ASSERT_EQ(int(10), ref_a->get(matchDoc));
    ASSERT_EQ(float(22.2), ref_b->get(matchDoc));
    ASSERT_EQ(int(30), ref_c->get(matchDoc));
    ASSERT_EQ(string("hello_40"), ref_d->get(matchDoc));
    ASSERT_EQ(int(50), ref_e->get(matchDoc));

    // ASSERT_FALSE(ref_a->getPointer(matchDoc));
    // ASSERT_FALSE(ref_b->getPointer(matchDoc));
    // ASSERT_FALSE(ref_c->getPointer(matchDoc));
    ASSERT_TRUE(ref_d->getPointer(matchDoc));
    // ASSERT_FALSE(ref_e->getPointer(matchDoc));
}

TEST_F(MatchDocAllocatorMountTest, testSerialize) {
    string data;
    {
        MountInfoPtr mountInfo = TestHelper::createMountInfo("1:int_a,float_b,int_e;2:int_c");
        MatchDocAllocator allocator(pool, false, mountInfo);
        Reference<int> *ref_a = allocator.declare<int>("int_a", "group_a");
        Reference<float> *ref_b = allocator.declare<float>("float_b", "group_a");
        Reference<int> *ref_c = allocator.declare<int>("int_c", "group_a");
        Reference<string> *ref_d = allocator.declare<string>("string_d", "group_a");
        Reference<int> *ref_e = allocator.declare<int>("int_e", "group_b");
        allocator.extend();

        MatchDoc matchDoc = allocator.allocate();
        ref_d->set(matchDoc, string("hello_40"));

        char *data1 = TestHelper::createMountData("int:10,float:22.2,int:50", pool.get());
        char *data2 = TestHelper::createMountData("int:30", pool.get());

        ASSERT_TRUE(ref_a->mount(matchDoc, data1));
        ASSERT_TRUE(ref_b->mount(matchDoc, data1));
        ASSERT_TRUE(ref_c->mount(matchDoc, data2));
        ASSERT_FALSE(ref_d->mount(matchDoc, data1));
        ASSERT_TRUE(ref_e->mount(matchDoc, data1));
        autil::DataBuffer dataBuffer;
        vector<MatchDoc> docs;
        docs.push_back(matchDoc);
        allocator.serialize(dataBuffer, docs, 0);
        data.assign(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    {
        DataBuffer dataBuffer((void *)data.c_str(), data.size());
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool));
        vector<MatchDoc> docs;
        allocator->deserialize(dataBuffer, docs);
        ASSERT_EQ(size_t(1), docs.size());

        TestHelper::checkDocValue(allocator, docs[0], "int_a:10,float_b:22.2,int_c:30,string_d:hello_40,int_e:50");
    }
}

TEST_F(MatchDocAllocatorMountTest, testSerializeWithLevel) {
    // serialize
    MountInfoPtr mountInfo = MatchDocTestHelper::createMountInfo("1:int_a,int_b,int_c,int_d");
    MatchDocAllocatorPtr allocator =
        MatchDocTestHelper::createAllocator("group_1:int_a,group_2:int_b,group_2:int_c,group_3:int_d", pool, mountInfo);
    MatchDocTestHelper::setSerializeLevel(allocator, "int_a:0,int_b:10,int_c:1,int_d:3");

    vector<MatchDoc> docs;
    Reference<int> *aRef = allocator->findReference<int>("int_a");
    Reference<int> *bRef = allocator->findReference<int>("int_b");
    Reference<int> *cRef = allocator->findReference<int>("int_c");
    Reference<int> *dRef = allocator->findReference<int>("int_d");

    // doc0 : 1,2,3,4
    MatchDoc doc = allocator->allocate(0);
    char *data = MatchDocTestHelper::createMountData("int:1,int:2,int:3,int:4", pool.get());
    aRef->mount(doc, data);
    bRef->mount(doc, data);
    cRef->mount(doc, data);
    dRef->mount(doc, data);
    docs.push_back(doc);

    // doc1 : 5,6,7,8
    doc = allocator->allocate(1);
    data = MatchDocTestHelper::createMountData("int:5,int:6,int:7,int:8", pool.get());
    aRef->mount(doc, data);
    bRef->mount(doc, data);
    cRef->mount(doc, data);
    dRef->mount(doc, data);
    docs.push_back(doc);

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

TEST_F(MatchDocAllocatorMountTest, testCloneMatchDoc) {
    MountInfoPtr mountInfo = TestHelper::createMountInfo("1:int_a,float_b,int_e;2:int_c");
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    Reference<int> *ref_a = allocator->declare<int>("int_a", "group_a");
    Reference<float> *ref_b = allocator->declare<float>("float_b", "group_a");
    Reference<int> *ref_c = allocator->declare<int>("int_c", "group_a");
    Reference<string> *ref_d = allocator->declare<string>("string_d", "group_a");
    Reference<int> *ref_e = allocator->declare<int>("int_e", "group_b");
    allocator->extend();

    MatchDoc matchDoc = allocator->allocate();
    ref_d->set(matchDoc, string("hello_40"));

    char *data1 = TestHelper::createMountData("int:10,float:22.2,int:50", pool.get());
    char *data2 = TestHelper::createMountData("int:30", pool.get());

    ASSERT_TRUE(ref_a->mount(matchDoc, data1));
    ASSERT_TRUE(ref_b->mount(matchDoc, data1));
    ASSERT_TRUE(ref_c->mount(matchDoc, data2));
    ASSERT_FALSE(ref_d->mount(matchDoc, data1));
    ASSERT_TRUE(ref_e->mount(matchDoc, data1));

    MatchDoc clonedDoc = allocator->allocateAndClone(matchDoc);
    TestHelper::checkDocValue(allocator, matchDoc, "int_a:10,float_b:22.2,int_c:30,string_d:hello_40,int_e:50");
    TestHelper::checkDocValue(allocator, clonedDoc, "int_a:10,float_b:22.2,int_c:30,string_d:hello_40,int_e:50");
}

class MountTestSubCollector {
public:
    MountTestSubCollector(Reference<int> *ref_c, Reference<int> *ref_d) : ref_subc(ref_c), ref_subd(ref_d) {}
    void operator()(MatchDoc doc) {
        subc_field.push_back(ref_subc->get(doc));
        subd_field.push_back(ref_subd->get(doc));
    }
    Reference<int> *ref_subc;
    Reference<int> *ref_subd;
    vector<int> subc_field;
    vector<int> subd_field;
};

TEST_F(MatchDocAllocatorMountTest, testSubMatchDoc) {
    MountInfoPtr mountInfo = TestHelper::createMountInfo("1:int_a;2:int_subc");
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    Reference<int> *ref_a = allocator->declare<int>("int_a");
    Reference<int> *ref_b = allocator->declare<int>("int_b");
    Reference<int> *ref_subc = allocator->declareSub<int>("int_subc");
    Reference<int> *ref_subd = allocator->declareSub<int>("int_subd");

    allocator->extend();
    allocator->extendSub();

    char *data1 = TestHelper::createMountData("int:10", pool.get());
    char *data2 = TestHelper::createMountData("int:31", pool.get());
    char *data3 = TestHelper::createMountData("int:32", pool.get());

    MatchDoc mainDoc = allocator->allocate();
    ref_a->mount(mainDoc, data1);
    ref_b->set(mainDoc, 20);
    allocator->allocateSub(mainDoc);
    ref_subc->mount(mainDoc, data2);
    ref_subd->set(mainDoc, 41);
    allocator->allocateSub(mainDoc);
    ref_subc->mount(mainDoc, data3);
    ref_subd->set(mainDoc, 42);

    TestHelper::checkDocValue(allocator, mainDoc, "int_a:10,int_b:20");
    MountTestSubCollector collector(ref_subc, ref_subd);
    allocator->getSubDocAccessor()->foreach (mainDoc, collector);
    EXPECT_THAT(collector.subc_field, UnorderedElementsAre(31, 32));
    EXPECT_THAT(collector.subd_field, UnorderedElementsAre(41, 42));
}

TEST_F(MatchDocAllocatorMountTest, testGetReferenceWithoutMount) {
    MountInfoPtr mountInfo = TestHelper::createMountInfo("1:int;");
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    Reference<int> *ref = allocator->declare<int>("int");
    ASSERT_TRUE(ref->isMount());
    allocator->extend();
    MatchDoc doc = allocator->allocate();
    int &value = ref->getReference(doc);
    ASSERT_EQ((int)0, value);
}

#include "matchdoc/toolkit/MatchDocJoiner.h"

#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/Reference.h"
#include "matchdoc/toolkit/MatchDocTestHelper.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace matchdoc;

namespace matchdoc {

class MatchDocJoinerTest : public testing::Test {
public:
    MatchDocJoinerTest() : pool(std::make_shared<autil::mem_pool::Pool>()), leftAllocator(pool), rightAllocator(pool) {}

protected:
    typedef MatchDocTestHelper TestHelper;

protected:
    std::shared_ptr<autil::mem_pool::Pool> pool;
    MatchDocAllocator leftAllocator;
    MatchDocAllocator rightAllocator;
};

TEST_F(MatchDocJoinerTest, testSimpleProcess) {
    Reference<int> *field1 = leftAllocator.declare<int>("field1", "group_a");
    ASSERT_TRUE(field1);
    Reference<string> *field2 = leftAllocator.declare<string>("field2", "group_b");
    ASSERT_TRUE(field2);
    leftAllocator.extend();

    Reference<char> *field3 = rightAllocator.declare<char>("field3");
    ASSERT_TRUE(field1);
    Reference<float> *field4 = rightAllocator.declare<float>("field4");
    ASSERT_TRUE(field2);
    rightAllocator.extend();

    MatchDoc doc1 = leftAllocator.allocate();
    field1->set(doc1, 10);
    field2->set(doc1, "hello2");
    MatchDoc doc2 = rightAllocator.allocate();
    field3->set(doc2, '3');
    field4->set(doc2, 4.4);

    MatchDocJoiner matchDocJoiner;
    ASSERT_TRUE(matchDocJoiner.init(&leftAllocator, &rightAllocator, pool.get()));
    MatchDocAllocatorPtr joinAllocator = matchDocJoiner.getJoinMatchDocAllocator();
    ASSERT_TRUE(joinAllocator.get());

    Reference<int> *ref1 = joinAllocator->findReference<int>("field1");
    ASSERT_TRUE(ref1);
    Reference<string> *ref2 = joinAllocator->findReference<string>("field2");
    ASSERT_TRUE(ref2);
    Reference<char> *ref3 = joinAllocator->findReference<char>("field3");
    ASSERT_TRUE(ref1);
    Reference<float> *ref4 = joinAllocator->findReference<float>("field4");
    ASSERT_TRUE(ref2);

    MatchDoc joinedDoc = matchDocJoiner.join(doc1, doc2);
    ASSERT_EQ(10, ref1->get(joinedDoc));
    ASSERT_EQ(string("hello2"), ref2->get(joinedDoc));
    ASSERT_EQ('3', ref3->get(joinedDoc));
    ASSERT_EQ(float(4.4), ref4->get(joinedDoc));
}

TEST_F(MatchDocJoinerTest, testJoinWithDuplicateFieldName) {
    MatchDocAllocatorPtr leftAllocator = TestHelper::createAllocator("int_a,int_b", pool);
    MatchDocAllocatorPtr rightAllocator = TestHelper::createAllocator("int_a,int_c", pool);
    MatchDocJoiner matchDocJoiner;
    ASSERT_FALSE(matchDocJoiner.init(leftAllocator.get(), rightAllocator.get(), pool.get()));
}

TEST_F(MatchDocJoinerTest, testCascadeJoin) {
    MatchDocAllocatorPtr allocator1 = TestHelper::createAllocator("int_a,int_b", pool);
    MatchDoc matchDoc11 = TestHelper::createMatchDoc(allocator1, "int_a:4,int_b:6");

    MatchDocAllocatorPtr allocator2 = TestHelper::createAllocator("string_c,string_d", pool);
    MatchDoc matchDoc21 = TestHelper::createMatchDoc(allocator2, "string_c:hello,string_d:kitty");

    MatchDocJoiner matchDocJoiner;
    ASSERT_TRUE(matchDocJoiner.init(allocator1.get(), allocator2.get(), pool.get()));

    MatchDoc joinedDoc1 = matchDocJoiner.join(matchDoc11, matchDoc21);
    MatchDocAllocatorPtr joinedAllocator1 = matchDocJoiner.getJoinMatchDocAllocator();
    TestHelper::checkDocValue(joinedAllocator1, joinedDoc1, "int_a:4,int_b:6,string_c:hello,string_d:kitty");
    MatchDocAllocatorPtr allocator3 = TestHelper::createAllocator("int_e", pool);
    MatchDoc matchDoc31 = TestHelper::createMatchDoc(allocator3, "int_e:7");

    ASSERT_TRUE(matchDocJoiner.init(joinedAllocator1.get(), allocator3.get(), pool.get()));
    MatchDoc joinedDoc2 = matchDocJoiner.join(joinedDoc1, matchDoc31);
    MatchDocAllocatorPtr joinedAllocator2 = matchDocJoiner.getJoinMatchDocAllocator();

    TestHelper::checkDocValue(
        matchDocJoiner.getJoinMatchDocAllocator(), joinedDoc2, "int_a:4,int_b:6,string_c:hello,string_d:kitty,int_e:7");
}

TEST_F(MatchDocJoinerTest, testSerialize) {
    string data;
    {
        MatchDocAllocatorPtr allocator1 = TestHelper::createAllocator("int_a,int_b", pool);
        MatchDoc matchDoc11 = TestHelper::createMatchDoc(allocator1, "int_a:4,int_b:6");
        MatchDoc matchDoc12 = TestHelper::createMatchDoc(allocator1, "int_a:8,int_b:9");

        MatchDocAllocatorPtr allocator2 = TestHelper::createAllocator("string_c,string_d", pool);
        MatchDoc matchDoc21 = TestHelper::createMatchDoc(allocator2, "string_c:hello,string_d:kitty");
        MatchDoc matchDoc22 = TestHelper::createMatchDoc(allocator2, "string_c:when,string_d:where");

        MatchDocJoiner matchDocJoiner;
        ASSERT_TRUE(matchDocJoiner.init(allocator1.get(), allocator2.get(), pool.get()));

        MatchDoc joinedDoc1 = matchDocJoiner.join(matchDoc11, matchDoc21);
        MatchDoc joinedDoc2 = matchDocJoiner.join(matchDoc12, matchDoc22);
        MatchDocAllocatorPtr joinedAllocator = matchDocJoiner.getJoinMatchDocAllocator();
        vector<MatchDoc> docs;
        docs.push_back(joinedDoc1);
        docs.push_back(joinedDoc2);
        DataBuffer dataBuffer;
        joinedAllocator->serialize(dataBuffer, docs, 0);
        data.assign(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    {
        DataBuffer dataBuffer((void *)data.c_str(), data.size());
        MatchDocAllocatorPtr allocator3(new MatchDocAllocator(pool));
        vector<MatchDoc> docs;
        allocator3->deserialize(dataBuffer, docs);
        ASSERT_EQ((size_t)2, docs.size());
        TestHelper::checkDocValue(allocator3, docs[0], "int_a:4,int_b:6,string_c:hello,string_d:kitty");
        TestHelper::checkDocValue(allocator3, docs[1], "int_a:8,int_b:9,string_c:when,string_d:where");
    }
}

TEST_F(MatchDocJoinerTest, testAliase) {
    MatchDocAllocatorPtr allocator1 = TestHelper::createAllocator("int_a,int_b", pool);
    MatchDoc matchDoc11 = TestHelper::createMatchDoc(allocator1, "int_a:4,int_b:6");

    MatchDocAllocatorPtr allocator2 = TestHelper::createAllocator("string_c,string_d", pool);
    MatchDoc matchDoc21 = TestHelper::createMatchDoc(allocator2, "string_c:hello,string_d:kitty");

    MatchDocJoiner matchDocJoiner;
    MatchDocJoiner::AliasMap leftMap;
    leftMap["int_a"] = "int_m";
    MatchDocJoiner::AliasMap rightMap;
    rightMap["string_c"] = "string_g";

    ASSERT_TRUE(matchDocJoiner.init(allocator1.get(), allocator2.get(), pool.get(), leftMap, rightMap));
    MatchDoc joinedDoc = matchDocJoiner.join(matchDoc11, matchDoc21);
    MatchDocAllocatorPtr joinedAllocator = matchDocJoiner.getJoinMatchDocAllocator();
    TestHelper::checkDocValue(joinedAllocator, joinedDoc, "int_m:4,int_b:6,string_g:hello,string_d:kitty");
}

TEST_F(MatchDocJoinerTest, testJoinDocWithMountField) {
    MountInfoPtr mountInfo1 = TestHelper::createMountInfo("1:int_a,float_b,int_e;2:int_c");
    MatchDocAllocator allocator1(pool, false, mountInfo1);
    Reference<int> *ref_a = allocator1.declare<int>("int_a");
    Reference<float> *ref_b = allocator1.declare<float>("float_b");
    Reference<int> *ref_c = allocator1.declare<int>("int_c");
    Reference<string> *ref_d = allocator1.declare<string>("string_d");
    Reference<int> *ref_e = allocator1.declare<int>("int_e");
    allocator1.extend();

    MatchDoc matchDoc1 = allocator1.allocate();
    char *data11 = TestHelper::createMountData("int:10,float:22.2,int:50", pool.get());
    char *data12 = TestHelper::createMountData("int:30", pool.get());

    ASSERT_TRUE(ref_a->mount(matchDoc1, data11));
    ASSERT_TRUE(ref_b->mount(matchDoc1, data11));
    ASSERT_TRUE(ref_e->mount(matchDoc1, data11));
    ASSERT_TRUE(ref_c->mount(matchDoc1, data12));
    ref_d->set(matchDoc1, string("hello_40"));

    MountInfoPtr mountInfo2 = TestHelper::createMountInfo("1:int_f,int_g;2:int_h,float_i");
    MatchDocAllocator allocator2(pool, false, mountInfo2);
    Reference<int> *ref_f = allocator2.declare<int>("int_f");
    Reference<int> *ref_g = allocator2.declare<int>("int_g");
    Reference<int> *ref_h = allocator2.declare<int>("int_h");
    Reference<float> *ref_i = allocator2.declare<float>("float_i");
    allocator2.extend();

    MatchDoc matchDoc2 = allocator2.allocate();
    char *data21 = TestHelper::createMountData("int:60,int:70", pool.get());
    char *data22 = TestHelper::createMountData("int:80,float:90.9", pool.get());

    ASSERT_TRUE(ref_f->mount(matchDoc2, data21));
    ASSERT_TRUE(ref_g->mount(matchDoc2, data21));
    ASSERT_TRUE(ref_h->mount(matchDoc2, data22));
    ASSERT_TRUE(ref_i->mount(matchDoc2, data22));

    MatchDocJoiner matchDocJoiner;
    ASSERT_TRUE(matchDocJoiner.init(&allocator1, &allocator2, pool.get()));
    MatchDocAllocatorPtr joinAllocator = matchDocJoiner.getJoinMatchDocAllocator();
    ASSERT_TRUE(joinAllocator.get());

    MatchDoc joinedDoc = matchDocJoiner.join(matchDoc1, matchDoc2);
    TestHelper::checkDocValue(joinAllocator,
                              joinedDoc,
                              "int_a:10,float_b:22.2,int_c:30,string_d:hello_40,int_e:50,"
                              "int_f:60,int_g:70,int_h:80,float_i:90.9");
}

} // namespace matchdoc

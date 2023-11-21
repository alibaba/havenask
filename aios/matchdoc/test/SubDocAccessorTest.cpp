#define GTEST_USE_OWN_TR1_TUPLE 0
#include "matchdoc/SubDocAccessor.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <functional>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"

using namespace std;
using namespace autil;
using namespace testing;

namespace matchdoc {
struct UnSupportCloneObj {
public:
    UnSupportCloneObj() : i(0), b(nullptr) {}
    ~UnSupportCloneObj() {
        if (b != nullptr) {
            delete b;
        }
        i = 0;
    }

private:
    UnSupportCloneObj(const UnSupportCloneObj &other) = delete;
    UnSupportCloneObj &operator=(const UnSupportCloneObj &other) = delete;

private:
    uint32_t i = 0;
    char *b = nullptr;
};
} // namespace matchdoc

namespace matchdoc {

class SubDocAccessorTest : public testing::Test {
public:
    void SetUp() {
        ReferenceTypesWrapper::getInstance()->registerType<UnSupportCloneObj>();
        _pool.reset(new autil::mem_pool::Pool());
        _docs.clear();
    }

    void TearDown() { ReferenceTypesWrapper::getInstance()->unregisterType<UnSupportCloneObj>(); }

    void flattenCollector(MatchDoc matchDoc) { _docs.push_back(matchDoc); }

    std::shared_ptr<autil::mem_pool::Pool> _pool;
    vector<MatchDoc> _docs;
};

TEST_F(SubDocAccessorTest, testForeachFlatten) {
    MatchDocAllocator allocator(_pool, true);
    Reference<int32_t> *ref1 = allocator.declare<int32_t>("var1");
    Reference<string> *ref2 = allocator.declare<string>("var2");
    Reference<UnSupportCloneObj> *ref3 = allocator.declare<UnSupportCloneObj>("var3");
    ASSERT_TRUE(ref3 != nullptr);

    Reference<int32_t> *subRef1 = allocator.declareSub<int32_t>("sub_var1");
    Reference<string> *subRef2 = allocator.declareSub<string>("sub_var2");
    allocator.extend();
    allocator.extendSub();

    SubDocAccessor *accessor = allocator.getSubDocAccessor();
    MatchDoc orgDoc = allocator.allocate();
    ref1->set(orgDoc, 1);
    ref2->set(orgDoc, string("abc"));

    allocator.allocateSub(orgDoc);
    subRef1->set(orgDoc, 11);
    subRef2->set(orgDoc, "111");

    allocator.allocateSub(orgDoc);
    subRef1->set(orgDoc, 22);
    subRef2->set(orgDoc, "222");

    allocator.allocateSub(orgDoc);
    subRef1->set(orgDoc, 33);
    subRef2->set(orgDoc, "333");

    std::function<void(MatchDoc)> func = std::bind(&SubDocAccessorTest::flattenCollector, this, std::placeholders::_1);
    accessor->foreachFlatten(orgDoc, &allocator, func);
    ASSERT_EQ(size_t(3), _docs.size());
    ASSERT_EQ(int32_t(1), ref1->get(_docs[0]));
    ASSERT_EQ(string("abc"), ref2->get(_docs[0]));
    ASSERT_EQ(int32_t(11), subRef1->get(_docs[0]));
    ASSERT_EQ(string("111"), subRef2->get(_docs[0]));

    ASSERT_EQ(int32_t(1), ref1->get(_docs[1]));
    ASSERT_EQ(string("abc"), ref2->get(_docs[1]));
    ASSERT_EQ(int32_t(22), subRef1->get(_docs[1]));
    ASSERT_EQ(string("222"), subRef2->get(_docs[1]));

    ASSERT_EQ(int32_t(1), ref1->get(_docs[2]));
    ASSERT_EQ(string("abc"), ref2->get(_docs[2]));
    ASSERT_EQ(int32_t(33), subRef1->get(_docs[2]));
    ASSERT_EQ(string("333"), subRef2->get(_docs[2]));

    ref1->set(_docs[0], 2);
    ASSERT_EQ(int32_t(2), ref1->get(_docs[0]));
    ASSERT_EQ(int32_t(1), ref1->get(_docs[1]));

    for (size_t i = 0; i < _docs.size(); ++i) {
        allocator.deallocate(_docs[i]);
    }
}

TEST_F(SubDocAccessorTest, testGetSubDocValues) {
    MatchDocAllocator allocator(_pool, true);
    Reference<int32_t> *ref1 = allocator.declare<int32_t>("var1");
    Reference<string> *ref2 = allocator.declare<string>("var2");

    Reference<int32_t> *subRef1 = allocator.declareSub<int32_t>("sub_var1");
    Reference<string> *subRef2 = allocator.declareSub<string>("sub_var2");
    allocator.extend();
    allocator.extendSub();

    SubDocAccessor *accessor = allocator.getSubDocAccessor();
    MatchDoc orgDoc = allocator.allocate();
    ref1->set(orgDoc, 1);
    ref2->set(orgDoc, string("abc"));

    allocator.allocateSub(orgDoc);
    subRef1->set(orgDoc, 11);
    subRef2->set(orgDoc, "111");

    allocator.allocateSub(orgDoc);
    subRef1->set(orgDoc, 22);
    subRef2->set(orgDoc, "222");

    allocator.allocateSub(orgDoc);
    subRef1->set(orgDoc, 33);
    subRef2->set(orgDoc, "333");

    vector<int32_t> intValues;
    accessor->getSubDocValues(orgDoc, subRef1, intValues);
    EXPECT_THAT(intValues, ElementsAre(11, 22, 33));

    vector<string> strValues;
    accessor->getSubDocValues(orgDoc, subRef2, strValues);
    EXPECT_THAT(strValues, ElementsAre("111", "222", "333"));
}

class SubSum {
public:
    SubSum(const Reference<uint32_t> *subRef) : _sum(0), _subRef(subRef) {}

public:
    void operator()(MatchDoc doc) { _sum += _subRef->getReference(doc); }
    uint32_t get() const { return _sum; }

private:
    uint32_t _sum;
    const Reference<uint32_t> *_subRef;
};

TEST_F(SubDocAccessorTest, testForeach) {
    MatchDocAllocator allocator(_pool, true);
    Reference<uint32_t> *ref = allocator.declare<uint32_t>("var");
    Reference<uint32_t> *subRef = allocator.declareSub<uint32_t>("sub_var");
    auto multiSubRef = allocator.declareSub<autil::MultiInt8>("sub_var_multi");
    allocator.extend();
    allocator.extendSub();

    SubDocAccessor *accessor = allocator.getSubDocAccessor();
    MatchDoc orgDoc = allocator.allocate();
    ref->set(orgDoc, 111);

    allocator.allocateSub(orgDoc);
    subRef->set(orgDoc, 3);

    allocator.allocateSub(orgDoc);
    subRef->set(orgDoc, 5);

    allocator.allocateSub(orgDoc);
    subRef->set(orgDoc, 7);
    int8_t dataArray[] = {(int8_t)4, 3};
    char *buf = MultiValueCreator::createMultiValueBuffer(dataArray, 2, _pool.get());
    MultiValueType<int8_t> multiValue(buf);
    multiSubRef->set(orgDoc, multiValue);
    SubSum ssum(subRef);
    accessor->foreach (orgDoc, ssum);
    ASSERT_EQ(15, ssum.get());
}

TEST_F(SubDocAccessorTest, testGetSubDocIds) {
    MatchDocAllocator allocator(_pool, true);
    allocator.extend();
    allocator.extendSub();

    SubDocAccessor *accessor = allocator.getSubDocAccessor();
    MatchDoc orgDoc = allocator.allocate();
    allocator.allocateSub(orgDoc, 11);
    allocator.allocateSub(orgDoc, 22);
    allocator.allocateSub(orgDoc, 33);

    vector<int32_t> subDocIds;
    size_t count = accessor->getSubDocIds(orgDoc, subDocIds);
    ASSERT_EQ(3, count);
    EXPECT_THAT(subDocIds, ElementsAre(11, 22, 33));
}

} // namespace matchdoc

#include "matchdoc/toolkit/MatchDocDefaultValueSetter.h"

#include "gtest/gtest.h"
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/MultiValueType.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/MountInfo.h"

namespace matchdoc {

using namespace std;
using namespace autil;

class MatchDocDefaultValueSetterTest : public testing::Test {
public:
    void SetUp() override { _pool = std::make_shared<autil::mem_pool::Pool>(); }

protected:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
};

TEST_F(MatchDocDefaultValueSetterTest, testInitEmpty) {
    autil::mem_pool::Pool pool;
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool, false, nullptr));
    MatchDocDefaultValueSetter assigner(allocator, _pool.get());
    {
        map<string, string> defaultValues;
        ASSERT_FALSE(assigner.init(defaultValues));
    }
    {
        map<string, string> defaultValues = {{"attr1", ""}, {"attr2", ""}};
        ASSERT_FALSE(assigner.init(defaultValues));
    }
}

TEST_F(MatchDocDefaultValueSetterTest, testInit) {
    MountInfoPtr mountInfo(new MountInfo);
    mountInfo->insert("int_a", 1, 0);
    mountInfo->insert("float_b", 1, 4);
    mountInfo->insert("int_c", 2, 0);
    mountInfo->insert("int_e", 1, 8);

    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool, false, mountInfo));
    ASSERT_TRUE(nullptr != allocator->declare<int>("int_a"));
    ASSERT_TRUE(nullptr != allocator->declare<float>("float_b"));
    ASSERT_TRUE(nullptr != allocator->declare<int>("int_c"));
    ASSERT_TRUE(nullptr != allocator->declare<string>("string_d"));
    ASSERT_TRUE(nullptr != allocator->declare<int>("int_e"));
    allocator->extend();

    map<string, string> defaultValues = {{"int_a", "1"}, {"float_b", "2.0"}, {"int_c", "3"}, {"int_e", "7"}};
    MatchDocDefaultValueSetter assigner(allocator, _pool.get());
    ASSERT_TRUE(assigner.init(defaultValues));
}

TEST_F(MatchDocDefaultValueSetterTest, testInitWithMountMultiValue) {
    MountInfoPtr mountInfo(new MountInfo);
    mountInfo->insert("int_a", 1, 0);
    mountInfo->insert("float_b", 1, 4);
    mountInfo->insert("multi_int_c", 1, 8);

    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool, false, mountInfo));
    ASSERT_TRUE(nullptr != allocator->declare<int>("int_a"));
    ASSERT_TRUE(nullptr != allocator->declare<float>("float_b"));
    ASSERT_TRUE(nullptr != allocator->declare<MultiValueType<int32_t>>("multi_int_c"));
    allocator->extend();

    map<string, string> defaultValues = {{"int_a", "1"}, {"float_b", "2.0"}};
    MatchDocDefaultValueSetter assigner(allocator, _pool.get());
    ASSERT_TRUE(assigner.init(defaultValues));
}

TEST_F(MatchDocDefaultValueSetterTest, testInitMountWithImpactFormat) {
    MountInfoPtr mountInfo(new MountInfo);
    autil::PackOffset pOffset(100, 1, 2, true, true);

    mountInfo->insert("int_a", 1, 0);
    mountInfo->insert("float_b", 1, 4);
    mountInfo->insert("multi_int_c", 1, pOffset.toUInt64());

    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool, false, mountInfo));
    ASSERT_TRUE(nullptr != allocator->declare<int>("int_a"));
    ASSERT_TRUE(nullptr != allocator->declare<float>("float_b"));
    ASSERT_TRUE(nullptr != allocator->declare<MultiValueType<int32_t>>("multi_int_c"));
    allocator->extend();

    map<string, string> defaultValues = {{"int_a", "1"}, {"float_b", "2.0"}};
    MatchDocDefaultValueSetter assigner(allocator, _pool.get());
    ASSERT_FALSE(assigner.init(defaultValues));
}

TEST_F(MatchDocDefaultValueSetterTest, testSetMountData) {
    MountInfoPtr mountInfo(new MountInfo);
    mountInfo->insert("int_a", 1, 0);
    mountInfo->insert("float_b", 1, 4);
    mountInfo->insert("int_c", 2, 0);
    mountInfo->insert("int_d", 1, 8);
    mountInfo->insert("multi_int_e", 1, 12);

    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool, false, mountInfo));
    auto ref_a = allocator->declare<int>("int_a");
    auto ref_b = allocator->declare<float>("float_b");
    auto ref_c = allocator->declare<int>("int_c");
    auto ref_d = allocator->declare<int>("int_d");
    auto ref_e = allocator->declare<MultiValueType<int32_t>>("multi_int_e");
    allocator->extend();

    map<string, string> defaultValues = {{"int_a", "1"}, {"float_b", "2.0"}, {"int_c", "3"}, {"int_d", "7"}};
    MatchDocDefaultValueSetter assigner(allocator, _pool.get());
    ASSERT_TRUE(assigner.init(defaultValues));

    MatchDoc matchDoc = allocator->allocate();
    assigner.setDefaultValues(matchDoc);

    ASSERT_EQ(int(1), ref_a->get(matchDoc));
    ASSERT_EQ(float(2.0), ref_b->get(matchDoc));
    ASSERT_EQ(int(3), ref_c->get(matchDoc));
    ASSERT_EQ(int(7), ref_d->get(matchDoc));
    MultiValueType<int32_t> multiValue = ref_e->get(matchDoc);
    ASSERT_EQ(0, multiValue.size());
}

TEST_F(MatchDocDefaultValueSetterTest, testSetNoMountData) {
    MountInfoPtr mountInfo(new MountInfo);

    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool, false, mountInfo));
    auto ref_a = allocator->declare<int>("int_a");
    auto ref_b = allocator->declare<float>("float_b");
    auto ref_c = allocator->declare<int>("int_c");
    auto ref_d = allocator->declare<int>("int_d");
    auto ref_e = allocator->declare<MultiValueType<int32_t>>("multi_int_b");
    allocator->extend();

    map<string, string> defaultValues = {{"int_a", "10"}, {"float_b", "20.0"}, {"int_c", "30"}, {"int_d", "70"}};

    MatchDocDefaultValueSetter assigner(allocator, _pool.get());
    ASSERT_TRUE(assigner.init(defaultValues));

    MatchDoc matchDoc = allocator->allocate();
    assigner.setDefaultValues(matchDoc);

    ASSERT_EQ(int(10), ref_a->get(matchDoc));
    ASSERT_EQ(float(20.0), ref_b->get(matchDoc));
    ASSERT_EQ(int(30), ref_c->get(matchDoc));
    ASSERT_EQ(int(70), ref_d->get(matchDoc));
    MultiValueType<int32_t> multiValue = ref_e->get(matchDoc);
    ASSERT_EQ(0, multiValue.size());
}

} // namespace matchdoc

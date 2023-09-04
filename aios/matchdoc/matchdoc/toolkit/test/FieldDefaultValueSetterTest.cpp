

#include "matchdoc/toolkit/FieldDefaultValueSetter.h"

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

class FieldDefaultValueSetterTest : public testing::Test {};

TEST_F(FieldDefaultValueSetterTest, testInit) {
    auto pool = std::make_shared<autil::mem_pool::Pool>();
    MountInfoPtr mountInfo(new MountInfo);
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    auto ref_a = allocator->declare<int>("int_a");
    auto ref_b = allocator->declare<float>("float_b");
    auto ref_c = allocator->declare<MultiValueType<int32_t>>("multi_int_c");
    allocator->extend();

    {
        FieldDefaultValueSetter assigner(pool.get(), ref_a);
        ASSERT_TRUE(assigner.init("1"));
    }
    {
        FieldDefaultValueSetter assigner(pool.get(), ref_b);
        ASSERT_TRUE(assigner.init("2.0"));
    }
    {
        FieldDefaultValueSetter assigner(pool.get(), ref_c);
        ASSERT_FALSE(assigner.init("2,3"));
    }
}

TEST_F(FieldDefaultValueSetterTest, testInitWithMount) {
    auto pool = std::make_shared<autil::mem_pool::Pool>();
    MountInfoPtr mountInfo(new MountInfo);
    mountInfo->insert("int_a", 1, 0);
    mountInfo->insert("float_b", 1, 4);
    mountInfo->insert("multi_int_c", 1, 8);

    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    auto ref_a = allocator->declare<int>("int_a");
    auto ref_b = allocator->declare<float>("float_b");
    auto ref_c = allocator->declare<MultiValueType<int32_t>>("multi_int_c");
    allocator->extend();

    char *mountBuffer = new char[1024];

    {
        FieldDefaultValueSetter assigner(pool.get(), ref_a);
        ASSERT_TRUE(assigner.init("1", mountBuffer));
    }
    {
        FieldDefaultValueSetter assigner(pool.get(), ref_b);
        ASSERT_TRUE(assigner.init("2.0", mountBuffer));
    }
    {
        FieldDefaultValueSetter assigner(pool.get(), ref_c);
        ASSERT_FALSE(assigner.init("2,3", mountBuffer));
    }
    delete[] mountBuffer;
}

TEST_F(FieldDefaultValueSetterTest, testSetDefaultValue) {
    auto pool = std::make_shared<autil::mem_pool::Pool>();
    MountInfoPtr mountInfo(new MountInfo);
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    auto ref_a = allocator->declare<int>("int_a");
    auto ref_b = allocator->declare<float>("float_b");
    auto ref_c = allocator->declare<MultiValueType<int32_t>>("multi_int_c");
    allocator->extend();

    auto doc = allocator->allocate();

    {
        FieldDefaultValueSetter assigner(pool.get(), ref_a);
        ASSERT_TRUE(assigner.init("1"));
        assigner.setDefaultValue(doc);
        ASSERT_EQ(1, ref_a->get(doc));
    }
    {
        FieldDefaultValueSetter assigner(pool.get(), ref_b);
        ASSERT_TRUE(assigner.init("2.0"));
        assigner.setDefaultValue(doc);
        ASSERT_EQ(2.0, ref_b->get(doc));
    }
    {
        FieldDefaultValueSetter assigner(pool.get(), ref_c);
        ASSERT_FALSE(assigner.init("2,3"));
    }
}

TEST_F(FieldDefaultValueSetterTest, testSetWithMount) {
    auto pool = std::make_shared<autil::mem_pool::Pool>();
    MountInfoPtr mountInfo(new MountInfo);
    mountInfo->insert("int_a", 1, 0);
    mountInfo->insert("float_b", 1, 4);
    mountInfo->insert("multi_int_c", 1, 8);

    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    auto ref_a = allocator->declare<int>("int_a");
    auto ref_b = allocator->declare<float>("float_b");
    auto ref_c = allocator->declare<MultiValueType<int32_t>>("multi_int_c");
    allocator->extend();
    auto doc = allocator->allocate();

    char *mountBuffer = new char[1024];

    {
        FieldDefaultValueSetter assigner(pool.get(), ref_a);
        ASSERT_TRUE(assigner.init("1", mountBuffer));
        assigner.setDefaultValue(doc);
        ASSERT_EQ(1, ref_a->get(doc));
    }
    {
        FieldDefaultValueSetter assigner(pool.get(), ref_b);
        ASSERT_TRUE(assigner.init("2.0", mountBuffer));
        assigner.setDefaultValue(doc);
        ASSERT_EQ(2.0, ref_b->get(doc));
    }
    {
        FieldDefaultValueSetter assigner(pool.get(), ref_c);
        ASSERT_FALSE(assigner.init("2,3", mountBuffer));
    }
    delete[] mountBuffer;
}

} // namespace matchdoc

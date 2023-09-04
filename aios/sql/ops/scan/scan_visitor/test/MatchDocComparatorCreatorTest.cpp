#include "sql/ops/scan/MatchDocComparatorCreator.h"

#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/Comparator.h"
#include "ha3/rank/ReferenceComparator.h" // IWYU pragma: keep
#include "matchdoc/MatchDocAllocator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace isearch::rank;
using namespace suez::turing;

namespace sql {

class MatchDocComparatorCreatorTest : public TESTBASE {
public:
    MatchDocComparatorCreatorTest();
    ~MatchDocComparatorCreatorTest();

public:
    void setUp();
    void tearDown();

protected:
    template <typename T>
    void innerTestOneRef(const std::string typeStr, bool isSuccess);
    template <typename T1, typename T2>
    void innerTestTwoRef(const std::string typeStr1, const std::string typeStr2, bool isSuccess);
    template <typename T>
    void createTwoRef(const std::string typeStr, bool isSuccess);
    template <typename T>
    void innerTestRef(const std::string typeStr, bool isSuccess);

protected:
    autil::mem_pool::Pool *_pool;
    std::shared_ptr<matchdoc::MatchDocAllocator> _allocator;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, MatchDocComparatorCreatorTest);

MatchDocComparatorCreatorTest::MatchDocComparatorCreatorTest() {
    _pool = new autil::mem_pool::Pool(1024);
    _allocator.reset(new matchdoc::MatchDocAllocator(_pool));
    _allocator->declare<uint32_t>("uint32");
    _allocator->declare<int32_t>("int32");
    _allocator->declare<int64_t>("int64");
    _allocator->declare<double>("double");
    _allocator->declare<float>("float");
    _allocator->declare<MultiChar>("string");
    _allocator->declare<MultiInt32>("mInt32");
    _allocator->declare<MultiInt64>("mInt64");
    _allocator->declare<MultiString>("mString");
}

MatchDocComparatorCreatorTest::~MatchDocComparatorCreatorTest() {
    delete _pool;
}

void MatchDocComparatorCreatorTest::setUp() {}

void MatchDocComparatorCreatorTest::tearDown() {}

TEST_F(MatchDocComparatorCreatorTest, testCreateComparators) {
    MatchDocComparatorCreator creator(_pool, _allocator.get());
    vector<bool> flags1 = {true};
    vector<bool> flags2 = {true, false};
    vector<bool> flags3 = {true, true, true};
    {
        Comparator *comp = creator.createComparator({"int32"}, flags1);
        ASSERT_TRUE(dynamic_cast<OneRefComparatorTyped<int32_t> *>(comp) != nullptr);
        POOL_DELETE_CLASS(comp);
    }
    {
        Comparator *comp = creator.createComparator({"uint32"}, flags1);
        ASSERT_TRUE(dynamic_cast<OneRefComparatorTyped<uint32_t> *>(comp) == nullptr);
        ASSERT_TRUE(dynamic_cast<ComboComparator *>(comp) != nullptr);
        POOL_DELETE_CLASS(comp);
    }
    {
        Comparator *comp = creator.createComparator({"int32", "int64"}, flags2);
        ASSERT_TRUE((dynamic_cast<TwoRefComparatorTyped<int32_t, int64_t> *>(comp) != nullptr));
        POOL_DELETE_CLASS(comp);
    }
    {
        Comparator *comp = creator.createComparator({"int32", "uint32"}, flags2);
        ASSERT_TRUE((dynamic_cast<TwoRefComparatorTyped<int32_t, uint32_t> *>(comp) == nullptr));
        ASSERT_TRUE(dynamic_cast<ComboComparator *>(comp) != nullptr);
        POOL_DELETE_CLASS(comp);
    }
    {
        Comparator *comp = creator.createComparator({"not_exist"}, flags1);
        ASSERT_TRUE(comp == nullptr);
    }
    {
        Comparator *comp = creator.createComparator({"int32", "int32", "int32"}, flags3);
        ASSERT_TRUE(dynamic_cast<ComboComparator *>(comp) != nullptr);
        POOL_DELETE_CLASS(comp);
    }
}

template <typename T>
void MatchDocComparatorCreatorTest::innerTestOneRef(const std::string typeStr, bool isSuccess) {
    bool flags[2] = {true, false};
    MatchDocComparatorCreator creator(_pool, _allocator.get());

    for (size_t i = 0; i < 2; ++i) {
        auto *comp = creator.createOptimizedComparator(typeStr, flags[i]);
        if (isSuccess) {
            ASSERT_TRUE(dynamic_cast<OneRefComparatorTyped<T> *>(comp) != nullptr);
            POOL_DELETE_CLASS(comp);
        } else {
            ASSERT_FALSE(comp);
        }
    }
}

TEST_F(MatchDocComparatorCreatorTest, testCreateOptimizedComparatorOneRef) {
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<uint32_t>("uint32", false));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<int32_t>("not_exist", false));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<int32_t>("int32", true));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<int64_t>("int64", true));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<double>("double", true));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<float>("float", false));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<MultiChar>("string", false));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<MultiInt32>("mInt32", false));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<MultiInt64>("mInt64", false));
    ASSERT_NO_FATAL_FAILURE(innerTestOneRef<MultiString>("mString", false));
}

template <typename T1, typename T2>
void MatchDocComparatorCreatorTest::innerTestTwoRef(const std::string typeStr1,
                                                    const std::string typeStr2,
                                                    bool isSuccess) {
    bool flags[2] = {true, false};
    MatchDocComparatorCreator creator(_pool, _allocator.get());
    for (size_t i = 0; i < 2; ++i) {
        for (size_t j = 0; j < 2; ++j) {
            auto *comp = creator.createOptimizedComparator(typeStr1, typeStr2, flags[i], flags[j]);
            if (isSuccess) {
                auto *twoRefComp = dynamic_cast<TwoRefComparatorTyped<T1, T2> *>(comp);
                ASSERT_TRUE(twoRefComp != nullptr);
                POOL_DELETE_CLASS(comp);
            } else {
                ASSERT_FALSE(comp);
            }
        }
    }
}

template <typename T>
void MatchDocComparatorCreatorTest::createTwoRef(const std::string typeStr, bool isSuccess) {
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, uint32_t>(typeStr, "uint32", false)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, int32_t>(typeStr, "not_exist", false)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, int32_t>(typeStr, "int32", isSuccess)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, int64_t>(typeStr, "int64", isSuccess)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, double>(typeStr, "double", isSuccess)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, float>(typeStr, "float", false)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, MultiChar>(typeStr, "string", false)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, MultiInt32>(typeStr, "mInt32", false)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, MultiInt64>(typeStr, "mInt64", false)));
    ASSERT_NO_FATAL_FAILURE((innerTestTwoRef<T, MultiString>(typeStr, "mString", false)));
}

TEST_F(MatchDocComparatorCreatorTest, testCreateOptimizedComparatorTwoRef) {
    ASSERT_NO_FATAL_FAILURE(createTwoRef<uint32_t>("uint32", false));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<int32_t>("not_exist", false));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<int32_t>("int32", true));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<int64_t>("int64", true));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<double>("double", true));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<float>("float", false));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<MultiChar>("string", false));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<MultiInt32>("mInt32", false));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<MultiInt64>("mInt64", false));
    ASSERT_NO_FATAL_FAILURE(createTwoRef<MultiString>("mString", false));
}

template <typename T>
void MatchDocComparatorCreatorTest::innerTestRef(const std::string typeStr, bool isSuccess) {
    bool flags[2] = {true, false};
    MatchDocComparatorCreator creator(_pool, _allocator.get());

    for (size_t i = 0; i < 2; ++i) {
        auto *comp = creator.createComparator(typeStr, flags[i]);
        if (isSuccess) {
            ASSERT_TRUE(dynamic_cast<ReferenceComparator<T> *>(comp) != nullptr);
            POOL_DELETE_CLASS(comp);
        } else {
            ASSERT_FALSE(comp);
        }
    }
}

TEST_F(MatchDocComparatorCreatorTest, testCreateComparator) {
    ASSERT_NO_FATAL_FAILURE(innerTestRef<uint32_t>("uint32", true));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<int32_t>("not_exist", false));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<int32_t>("int32", true));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<int64_t>("int64", true));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<double>("double", true));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<float>("float", true));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<MultiChar>("string", true));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<MultiInt32>("mInt32", true));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<MultiInt64>("mInt64", true));
    ASSERT_NO_FATAL_FAILURE(innerTestRef<MultiString>("mString", true));
}

} // namespace sql

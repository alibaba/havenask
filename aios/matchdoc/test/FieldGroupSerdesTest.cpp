#include "matchdoc/FieldGroupSerdes.h"

#include "gtest/gtest.h"

#include "matchdoc/FieldGroupBuilder.h"

namespace matchdoc {

class FieldGroupSerdesTest : public testing::Test {
public:
    void SetUp() { _pool = std::make_shared<autil::mem_pool::Pool>(); }

    std::string serializeGroup(bool order, bool sort) {
        auto builder = std::make_unique<FieldGroupBuilder>("group_name", _pool);
        std::string result;
        Reference<int32_t> *ref1, *ref2;
        if (order) {
            ref1 = builder->declare<int32_t>("int1", true);
            ref2 = builder->declare<int32_t>("int2", true);
            builder->declare<int32_t>("int3", true);
        } else {
            builder->declare<int32_t>("int3", true);
            ref1 = builder->declare<int32_t>("int1", true);
            ref2 = builder->declare<int32_t>("int2", true);
        }
        auto group = builder->finalize();
        group->growToSize(VectorStorage::CHUNKSIZE);
        MatchDoc doc0(0);
        group->constructDoc(doc0);
        MatchDoc doc1(1);
        group->constructDoc(doc1);
        ref1->set(doc0, 1);
        ref2->set(doc0, 2);
        autil::DataBuffer dataBuffer;
        FieldGroupSerdes::serializeMeta(group.get(), dataBuffer, 0, sort);
        FieldGroupSerdes::serializeData(group.get(), dataBuffer, doc0, 0);
        FieldGroupSerdes::serializeData(group.get(), dataBuffer, doc1, 0);
        result.assign(dataBuffer.getData(), dataBuffer.getDataLen());
        return result;
    }

protected:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
};

TEST_F(FieldGroupSerdesTest, testAlphabetOrder) {
    ASSERT_NE(serializeGroup(true, false), serializeGroup(false, false));
    ASSERT_EQ(serializeGroup(true, true), serializeGroup(false, true));
}

TEST_F(FieldGroupSerdesTest, testSerializeDeserizlize) {
    auto str = serializeGroup(true, false);
    autil::DataBuffer dataBuffer((void *)str.c_str(), str.length(), nullptr);
    auto group = FieldGroupSerdes::deserializeMeta(_pool.get(), dataBuffer);
    ASSERT_TRUE(group);
    Reference<int32_t> *ref1 = group->findReference<int32_t>("int1");
    ASSERT_TRUE(ref1);
    Reference<int32_t> *ref2 = group->findReference<int32_t>("int2");
    ASSERT_TRUE(ref2);
    Reference<int32_t> *ref3 = group->findReference<int32_t>("int3");
    ASSERT_TRUE(ref3);
    ASSERT_EQ(sizeof(int32_t) * 3, group->getDocSize());
    group->growToSize(2);
    MatchDoc doc0(0);
    FieldGroupSerdes::deserializeData(group.get(), dataBuffer, doc0);
    ASSERT_EQ(1, ref1->get(doc0));
    ASSERT_EQ(2, ref2->get(doc0));
    ASSERT_EQ(0, ref3->get(doc0));
    MatchDoc doc1(1);
    FieldGroupSerdes::deserializeData(group.get(), dataBuffer, doc1);
    ASSERT_EQ(0, ref1->get(doc1));
    ASSERT_EQ(0, ref2->get(doc1));
    ASSERT_EQ(0, ref3->get(doc1));
}

} // namespace matchdoc

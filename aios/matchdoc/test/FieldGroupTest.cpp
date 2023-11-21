#include "matchdoc/FieldGroup.h"

#include "gtest/gtest.h"
#include <cstdint>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/FieldGroupBuilder.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ReferenceTypesWrapper.h"
#include "matchdoc/Trait.h"
#include "matchdoc/VectorStorage.h"

using namespace std;
using namespace autil;

namespace matchdoc {

class UserDefType {
public:
    UserDefType() : _str(new string("abc")) {}
    UserDefType &operator=(const UserDefType &other) {
        delete _str;
        _str = new string(*other._str);
        return *this;
    }
    ~UserDefType() { delete _str; }
    std::string *_str;
};

class FieldGroupTest : public testing::Test {
public:
    void SetUp() {
        _pool = std::make_shared<autil::mem_pool::Pool>();
        _builder = std::make_unique<FieldGroupBuilder>("group_name", _pool);
        ReferenceTypesWrapper::getInstance()->registerType<UserDefType>();
    }

    void TearDown() { ReferenceTypesWrapper::getInstance()->unregisterType<UserDefType>(); }

protected:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::unique_ptr<FieldGroupBuilder> _builder;
};

TEST_F(FieldGroupTest, testEmpty) {
    auto group = _builder->finalize();
    ASSERT_TRUE(group);
    ASSERT_FALSE(group->needConstruct());
    ASSERT_FALSE(group->needDestruct());
    ASSERT_EQ(0, group->getDocSize());
    ASSERT_EQ(0, group->getReferenceSet().size());
}

TEST_F(FieldGroupTest, testSimpleProcess) {
    Reference<int32_t> *refInt = _builder->declare<int32_t>("int32", true);
    ASSERT_TRUE(refInt->needConstructMatchDoc());
    Reference<string> *refstr = _builder->declare<string>("string", true);
    ASSERT_TRUE(refstr != nullptr);
    ASSERT_TRUE(refstr->needConstructMatchDoc());
    ASSERT_TRUE(refstr->needDestructMatchDoc());
    Reference<UserDefType> *refu = _builder->declare<UserDefType>("u", true);
    ASSERT_TRUE(refu != nullptr);
    ASSERT_TRUE(refu->needConstructMatchDoc());
    ASSERT_TRUE(refu->needDestructMatchDoc());
    auto group = _builder->finalize();
    ASSERT_TRUE(group);
    ASSERT_TRUE(group->needConstruct());
    ASSERT_TRUE(group->needDestruct());
    group->growToSize(VectorStorage::CHUNKSIZE);
    MatchDoc doc1(0);
    group->constructDoc(doc1);
    refInt->set(doc1, 100);
    string str = "test string";
    refstr->set(doc1, str);
    refu->set(doc1, UserDefType());
    EXPECT_EQ(100, refInt->get(doc1));
    EXPECT_EQ(str, refstr->get(doc1));
    EXPECT_EQ(string("abc"), *(refu->getReference(doc1)._str));
    group->destructDoc(doc1);
}

TEST_F(FieldGroupTest, testNeedNotConstructor) {
    Reference<int32_t> *refInt = _builder->declare<int32_t>("int32", false);
    ASSERT_FALSE(refInt->needConstructMatchDoc());
    auto group = _builder->finalize();
    ASSERT_TRUE(group);
    ASSERT_FALSE(group->needConstruct());
    ASSERT_FALSE(group->needDestruct());
}

TEST_F(FieldGroupTest, testResetSerializeLevel) {
    Reference<int32_t> *refInt = _builder->declare<int32_t>("int32", true);
    refInt->setSerializeLevel(2);
    Reference<string> *refstr = _builder->declare<string>("string", true);
    refstr->setSerializeLevel(3);
    auto group = _builder->finalize();
    group->resetSerializeLevel(0);

    ASSERT_EQ((uint8_t)0, refInt->getSerializeLevel());
    ASSERT_EQ((uint8_t)0, refstr->getSerializeLevel());
}

TEST_F(FieldGroupTest, testFromBuffer) {
    vector<int32_t> values{1, 2, 3, 4};
    auto group = _builder->fromBuffer(values.data(), values.size());
    ASSERT_TRUE(group);
    ASSERT_EQ(sizeof(int32_t), group->getDocSize());
    EXPECT_FALSE(group->needDestruct());
    EXPECT_FALSE(group->needConstruct());

    Reference<int32_t> *ref = group->findReference<int32_t>("group_name");
    ASSERT_TRUE(ref != nullptr);
    EXPECT_EQ("group_name", ref->getName());
    EXPECT_EQ(0, ref->_offset);
    EXPECT_FALSE(ref->needDestructMatchDoc());
}

TEST_F(FieldGroupTest, testGrowToSize) {
    _builder->declare<int32_t>("int3", true);
    auto group = _builder->finalize();
    ASSERT_EQ(0u, group->_storage->_chunks.size());
    group->growToSize(5 * VectorStorage::CHUNKSIZE);
    ASSERT_EQ(5u, group->_storage->_chunks.size());
}

TEST_F(FieldGroupTest, testInitMountAttr) {
    auto ref1 = _builder->declareMount<int32_t>("int1", 0, 0, true);
    ASSERT_TRUE(ref1->isMount());
    auto group = _builder->finalize();
    group->growToSize(VectorStorage::CHUNKSIZE);
    MatchDoc doc0(0);
    group->constructDoc(doc0);
    ASSERT_EQ(0, ref1->get(doc0));
    int val0 = 1024;
    ref1->mount(doc0, (char *)&val0);
    ASSERT_EQ(1024, ref1->get(doc0));

    group->constructDoc(doc0);
    ASSERT_EQ(0, ref1->get(doc0));
}

} // namespace matchdoc

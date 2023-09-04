#include "indexlib/index/common/DictHasher.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib::index {

class DictHasherTest : public INDEXLIB_TESTBASE
{
public:
    DictHasherTest();
    ~DictHasherTest();

    DECLARE_CLASS_NAME(DictHasherTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DictHasherTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.index, DictHasherTest);

DictHasherTest::DictHasherTest() {}

DictHasherTest::~DictHasherTest() {}

void DictHasherTest::CaseSetUp() {}

void DictHasherTest::CaseTearDown() {}

void DictHasherTest::TestSimpleProcess()
{
    util::KeyValueMap func {{"dict_hash_func", "LayerHash"}, {"separator", "."}};
    {
        TokenHasher layerHash(func, ft_text);
        TokenHasher defaultHash(util::KeyValueMap(), ft_text);
        ASSERT_TRUE(layerHash._layerHasher);
        ASSERT_FALSE(defaultHash._layerHasher);
        dictkey_t key1, key2;
        layerHash.CalcHashKey("index.test1", key1);
        defaultHash.CalcHashKey("index.test1", key2);
        ASSERT_NE(key1, key2);
    }
    {
        IndexDictHasher layerHash(func, it_number_int8);
        IndexDictHasher defaultHash(util::KeyValueMap(), it_number_int8);
        ASSERT_FALSE(layerHash._layerHasher);
        ASSERT_FALSE(defaultHash._layerHasher);
        DictKeyInfo key1, key2;
        Term term("1", "index");
        layerHash.GetHashKey(term, key1);
        defaultHash.GetHashKey(term, key2);
        ASSERT_EQ(key1, key2);
    }
}

} // namespace indexlib::index

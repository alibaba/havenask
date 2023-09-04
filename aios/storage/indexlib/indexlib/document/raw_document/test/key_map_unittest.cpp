#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "indexlib/common_define.h"
#include "indexlib/document/raw_document/key_map.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;

namespace indexlib { namespace document {

class KeyMapTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() {}
    void CaseTearDown() {}

protected:
    static vector<pair<string, size_t>> insertMap(KeyMap* hashMap, const string& token, size_t count, bool findRequire);
    static void checkHashReturn(KeyMap* hashMap, const vector<pair<string, size_t>>& vec);
    static void checkHashReturnMultiThread(KeyMap* hashMap, const vector<pair<string, size_t>>& vec);
    static void multiThreadTest(KeyMap* hashMap, const string& token, size_t count);
};

TEST_F(KeyMapTest, testSimple)
{
    KeyMap hashMap(2000);

    string token1 = "token1";
    size_t index_invalid = hashMap.find(StringView(token1));
    EXPECT_EQ(size_t(-1), index_invalid);
    size_t index1 = hashMap.findOrInsert(StringView(token1));
    size_t index11 = hashMap.find(StringView(token1));
    ASSERT_EQ(size_t(0), index1);
    ASSERT_EQ(size_t(0), index11);
    ASSERT_EQ(size_t(1), hashMap.size());

    string token2 = "token2";
    size_t index2 = hashMap.insert(StringView(token2));
    size_t index22 = hashMap.findOrInsert(StringView(token2));
    ASSERT_EQ(size_t(1), index2);
    ASSERT_EQ(size_t(1), index22);
    ASSERT_EQ(size_t(2), hashMap.size());
}

TEST_F(KeyMapTest, testMultiValue)
{
    KeyMap hashMap;
    vector<pair<string, size_t>> vec11, vec12, vec21, vec22;

    vec11 = insertMap(&hashMap, "Test1", 100, false);
    checkHashReturn(&hashMap, vec11);

    vec12 = insertMap(&hashMap, "Test1", 200, true);
    checkHashReturn(&hashMap, vec12);

    vec22 = insertMap(&hashMap, "Test2", 200, false);
    vec21 = insertMap(&hashMap, "Test2", 100, true);
    checkHashReturn(&hashMap, vec21);
    checkHashReturn(&hashMap, vec22);
}

TEST_F(KeyMapTest, testClone)
{
    KeyMap hashMap;
    hashMap.insert(StringView(string("Test1")));
    hashMap.insert(StringView(string("Test2")));

    KeyMap copyedHashMap(hashMap);
    ASSERT_EQ(hashMap._keyFields, copyedHashMap._keyFields);

    KeyMapPtr clonedHashMap(hashMap.clone());
    ASSERT_TRUE(clonedHashMap.get() != &hashMap);
    ASSERT_EQ(hashMap._keyFields, clonedHashMap->_keyFields);
}

TEST_F(KeyMapTest, testMerge)
{
    KeyMap hashMap0;
    KeyMap hashMap1;
    // merge no key
    hashMap1.merge(hashMap0);
    ASSERT_EQ(hashMap1.size(), size_t(0));

    // merge 200 keys
    insertMap(&hashMap1, "Key1", 200, true);
    hashMap0.merge(hashMap1);
    ASSERT_EQ(hashMap0.size(), size_t(200));
    ASSERT_EQ(hashMap0._keyFields, hashMap1._keyFields);
    // merge 100 old keys
    KeyMap hashMap3;
    insertMap(&hashMap3, "Key1", 100, true);
    hashMap0.merge(hashMap3);
    ASSERT_EQ(hashMap0.size(), size_t(200));
    ASSERT_EQ(hashMap0._keyFields, hashMap1._keyFields);
    // merge 100 old + 100 new keys
    insertMap(&hashMap3, "Key2", 100, true);
    hashMap0.merge(hashMap3);
    ASSERT_EQ(hashMap0.size(), size_t(300));
}

TEST_F(KeyMapTest, testGetKeyFields)
{
    KeyMap hashMap;
    vector<pair<string, size_t>> vec;
    vector<string> strs;

    vec = insertMap(&hashMap, "Test1", 100, false);
    vector<StringView> copyedKeyVec;
    copyedKeyVec = hashMap.getKeyFields();
    EXPECT_TRUE(&copyedKeyVec != &hashMap._keyFields);
    EXPECT_TRUE(copyedKeyVec == hashMap._keyFields);
}

vector<pair<string, size_t>> KeyMapTest::insertMap(KeyMap* hashMap, const string& token, size_t count, bool findRequire)
{
    if (findRequire) {
        vector<pair<string, size_t>> vec;
        for (size_t i = 0; i < count; ++i) {
            string str = token + StringUtil::toString(i);
            size_t index = hashMap->findOrInsert(StringView(str));
            vec.push_back(make_pair(str, index));
        }
        return vec;
    } else {
        vector<pair<string, size_t>> vec;
        for (size_t i = 0; i < count; ++i) {
            string str = token + StringUtil::toString(i);
            size_t index = hashMap->insert(StringView(str));
            vec.push_back(make_pair(str, index));
        }
        return vec;
    }
}

void KeyMapTest::checkHashReturn(KeyMap* hashMap, const vector<pair<string, size_t>>& vec)
{
    size_t oldSize = hashMap->size();
    for (size_t i = 0; i < vec.size(); ++i) {
        EXPECT_EQ(vec[i].second, hashMap->find(StringView(vec[i].first)));
    }
    EXPECT_EQ(oldSize, hashMap->size());
}

TEST_F(KeyMapTest, testPerformance)
{
    struct StringHasher {
        size_t operator()(const autil::StringView& str) const
        {
            return std::_Hash_bytes(str.data(), str.size(), static_cast<size_t>(0xc70f6907UL));
        }
    };

    // typedef std::unordered_map<std::string, size_t, StringHasher> OldHashMap;
    // typedef absl::flat_hash_map<std::string, size_t, StringHasher> NewHashMap;

    typedef autil::flat_hash_map<autil::StringView, size_t, StringHasher> flatHashMap;
    typedef autil::bytell_hash_map<autil::StringView, size_t, StringHasher> bytellHashMap;
    typedef std::unordered_map<autil::StringView, size_t, StringHasher> stdHashMap;

    size_t prefix = 1636620011;
    size_t circleCnt = 1000;
    size_t sampleCnt = 300;
    size_t keymapSize = 4096;

    {
        int64_t totalTs = 0;
        flatHashMap newKeyMap(keymapSize);
        for (size_t j = 0; j < circleCnt; ++j) {
            newKeyMap.clear();
            size_t key = prefix;
            std::vector<std::string> datas;
            for (size_t i = key; i < key + sampleCnt; ++i) {
                datas.push_back(std::to_string(i));
                newKeyMap[autil::StringView(datas.back().data(), datas.back().size())] = i;
            }
            auto beginTs = autil::TimeUtility::currentTime();
            for (size_t i = prefix; i < prefix + sampleCnt / 3; ++i) {
                newKeyMap.find(autil::StringView(datas[i - prefix].data(), datas[i - prefix].size()));
            }
            auto endTs = autil::TimeUtility::currentTime();
            totalTs += endTs - beginTs;
        }
    }

    {
        int64_t totalTs = 0;
        bytellHashMap newKeyMap(keymapSize);
        for (size_t j = 0; j < circleCnt; ++j) {
            newKeyMap.clear();
            size_t key = prefix;
            std::vector<std::string> datas;
            for (size_t i = key; i < key + sampleCnt; ++i) {
                datas.push_back(std::to_string(i));
                newKeyMap[autil::StringView(datas.back().data(), datas.back().size())] = i;
            }
            auto beginTs = autil::TimeUtility::currentTime();
            for (size_t i = prefix; i < prefix + sampleCnt / 3; ++i) {
                newKeyMap.find(autil::StringView(datas[i - prefix].data(), datas[i - prefix].size()));
            }
            auto endTs = autil::TimeUtility::currentTime();
            totalTs += endTs - beginTs;
        }
    }

    {
        int64_t totalTs = 0;
        stdHashMap newKeyMap(keymapSize);
        for (size_t j = 0; j < circleCnt; ++j) {
            newKeyMap.clear();
            size_t key = prefix;
            std::vector<std::string> datas;
            for (size_t i = key; i < key + sampleCnt; ++i) {
                datas.push_back(std::to_string(i));
                newKeyMap[autil::StringView(datas.back().data(), datas.back().size())] = i;
            }
            auto beginTs = autil::TimeUtility::currentTime();
            for (size_t i = prefix; i < prefix + sampleCnt / 3; ++i) {
                newKeyMap.find(autil::StringView(datas[i - prefix].data(), datas[i - prefix].size()));
            }
            auto endTs = autil::TimeUtility::currentTime();
            totalTs += endTs - beginTs;
        }
    }
}

}} // namespace indexlib::document

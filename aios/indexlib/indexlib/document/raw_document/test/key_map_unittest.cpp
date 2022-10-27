#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/raw_document/key_map.h"
#include <autil/StringUtil.h>
#include <autil/Thread.h>

using namespace std;
using namespace autil;
using namespace testing;

IE_NAMESPACE_BEGIN(document);

class KeyMapTest : public INDEXLIB_TESTBASE {
public:
    void CaseSetUp() {}
    void CaseTearDown() {}
protected:
    static vector<pair<string, size_t> > insertMap(
            KeyMap *hashMap, const string &token, size_t count,
            bool findRequire);
    static void checkHashReturn(
            KeyMap *hashMap,
            const vector<pair<string, size_t> > &vec);
    static void checkHashReturnMultiThread(
            KeyMap *hashMap,
            const vector<pair<string, size_t> > &vec);
    static void multiThreadTest(
            KeyMap *hashMap,const string &token, size_t count);
};

TEST_F(KeyMapTest, testSimple) {
    KeyMap hashMap(2000);
    ASSERT_TRUE(hashMap._hashMap.bucket_count() >= 2000);

    string token1 = "token1";
    size_t index_invalid = hashMap.find(ConstString(token1));
    EXPECT_EQ(size_t(-1), index_invalid);
    size_t index1 = hashMap.findOrInsert(ConstString(token1));
    size_t index11 = hashMap.find(ConstString(token1));
    ASSERT_EQ(size_t(0), index1);
    ASSERT_EQ(size_t(0), index11);
    ASSERT_EQ(size_t(1), hashMap.size());

    string token2 = "token2";
    size_t index2 = hashMap.insert(ConstString(token2));
    size_t index22 = hashMap.findOrInsert(ConstString(token2));
    ASSERT_EQ(size_t(1), index2);
    ASSERT_EQ(size_t(1), index22);
    ASSERT_EQ(size_t(2), hashMap.size());
}

TEST_F(KeyMapTest, testMultiValue) {
    KeyMap hashMap;
    vector<pair<string, size_t> > vec11,vec12,vec21,vec22;

    vec11 = insertMap(&hashMap, "Test1", 100, false);
    checkHashReturn(&hashMap, vec11);

    vec12 = insertMap(&hashMap, "Test1", 200, true);
    checkHashReturn(&hashMap, vec12);
 
    vec22 = insertMap(&hashMap, "Test2", 200, false);
    vec21 = insertMap(&hashMap, "Test2", 100, true);
    checkHashReturn(&hashMap, vec21);
    checkHashReturn(&hashMap, vec22);
}

TEST_F(KeyMapTest, testClone){
    KeyMap hashMap;
    hashMap.insert( ConstString(string("Test1")) );
    hashMap.insert( ConstString(string("Test2")) );

    KeyMap copyedHashMap(hashMap);
    ASSERT_EQ(hashMap._keyFields, copyedHashMap._keyFields);
    
    KeyMapPtr clonedHashMap(hashMap.clone());
    ASSERT_TRUE(clonedHashMap.get() != &hashMap);
    ASSERT_EQ(hashMap._keyFields, clonedHashMap->_keyFields);
}

TEST_F(KeyMapTest, testMerge){
    KeyMap hashMap0;
    KeyMap hashMap1;
    // merge no key
    hashMap1.merge(hashMap0);
    ASSERT_EQ(hashMap1.size(),size_t(0));

    // merge 200 keys
    insertMap(&hashMap1, "Key1", 200, true);
    hashMap0.merge(hashMap1);
    ASSERT_EQ(hashMap0.size(),size_t(200));
    ASSERT_EQ(hashMap0._keyFields, hashMap1._keyFields);
    // merge 100 old keys
    KeyMap hashMap3;
    insertMap(&hashMap3, "Key1", 100, true);
    hashMap0.merge(hashMap3);
    ASSERT_EQ(hashMap0.size(),size_t(200));
    ASSERT_EQ(hashMap0._keyFields, hashMap1._keyFields);
    // merge 100 old + 100 new keys
    insertMap(&hashMap3, "Key2", 100, true);
    hashMap0.merge(hashMap3);
    ASSERT_EQ(hashMap0.size(),size_t(300));
}

TEST_F(KeyMapTest, testGetKeyFields) {
    KeyMap hashMap;
    vector<pair<string, size_t> > vec;
    vector<string> strs;

    vec = insertMap(&hashMap, "Test1", 100, false);
    vector<ConstString> copyedKeyVec;
    copyedKeyVec = hashMap.getKeyFields();
    EXPECT_TRUE(&copyedKeyVec != &hashMap._keyFields);
    EXPECT_TRUE(copyedKeyVec == hashMap._keyFields);
}

vector<pair<string, size_t> > KeyMapTest::insertMap(
        KeyMap *hashMap, 
        const string &token, size_t count,
        bool findRequire)
{ 
    if ( findRequire ) {
        vector<pair<string, size_t> > vec;
        for (size_t i = 0; i < count; ++i) {
            string str = token + StringUtil::toString(i);
            size_t index = hashMap->findOrInsert(ConstString(str));
            vec.push_back(make_pair(str, index));
        }
        return vec;
    } else {
        vector<pair<string, size_t> > vec;
        for (size_t i = 0; i < count; ++i) {
            string str = token + StringUtil::toString(i);
            size_t index = hashMap->insert(ConstString(str));
            vec.push_back(make_pair(str, index));
        }
        return vec;
    }
}

void KeyMapTest::checkHashReturn(
        KeyMap *hashMap,
        const vector<pair<string, size_t> > &vec)
{
    size_t oldSize = hashMap->size();
    for (size_t i = 0; i < vec.size(); ++i) {
        EXPECT_EQ(vec[i].second, hashMap->find(ConstString(vec[i].first)));
    }
    EXPECT_EQ(oldSize, hashMap->size());
}

IE_NAMESPACE_END(document);

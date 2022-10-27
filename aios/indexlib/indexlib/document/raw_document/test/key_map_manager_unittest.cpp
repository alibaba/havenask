#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/raw_document/key_map.h"
#include "indexlib/document/raw_document/key_map_manager.h"

#include <autil/ConstString.h>
using namespace std;
using namespace testing;
using namespace autil;

IE_NAMESPACE_BEGIN(document);

class KeyMapManagerTest : public INDEXLIB_TESTBASE {
public:
    void CaseSetUp() {}
    void CaseTearDown() {}
};

TEST_F(KeyMapManagerTest, testSimple){
    // ASSERT_TRUE(false) << "!not implemented!";
    size_t maxKeySize = 4096;
    KeyMapManager hashMapManager(maxKeySize);
    // test the first get
    KeyMapPtr hashMapEmpty(new KeyMap());
    KeyMapPtr hashMapPtr0 = hashMapManager.getHashMapPrimary();
    ASSERT_TRUE(hashMapPtr0 != NULL);
    ASSERT_FALSE(hashMapManager._fastUpdateable);
    // test get when primary not update.
    KeyMapPtr hashMapPtr1 = hashMapManager.getHashMapPrimary();
    ASSERT_EQ(hashMapPtr1, hashMapPtr0);
    ASSERT_EQ(size_t(0), hashMapPtr0->size());

    // do different update
    hashMapPtr0.reset(new KeyMap());
    hashMapPtr1.reset(new KeyMap());

    hashMapManager.updatePrimary(hashMapPtr0);
    ASSERT_FALSE(hashMapManager._fastUpdateable);
    ASSERT_EQ(size_t(0), hashMapManager.getHashMapPrimary()->size());
    
    hashMapPtr0->insert(ConstString(string("key0")));
    hashMapPtr0->insert(ConstString(string("key1")));
    hashMapPtr1->insert(ConstString(string("key1")));

    // test update primary
    hashMapManager.updatePrimary(hashMapPtr0);
    ASSERT_TRUE(hashMapManager._fastUpdateable);
    KeyMapPtr hashMapPtr2 = hashMapManager.getHashMapPrimary();
    ASSERT_FALSE(hashMapManager._fastUpdateable);
    ASSERT_TRUE(hashMapPtr1 != hashMapPtr2);
    ASSERT_EQ(hashMapPtr0->getKeyFields(), hashMapPtr2->getKeyFields());
    ASSERT_EQ(size_t(2), hashMapPtr2->size());
    
    // test update primary
    hashMapManager.updatePrimary(hashMapPtr1);
    ASSERT_TRUE(hashMapManager._fastUpdateable);
    KeyMapPtr hashMapPtr3 = hashMapManager.getHashMapPrimary();
    ASSERT_EQ(hashMapPtr0->getKeyFields(), hashMapPtr3->getKeyFields());
    ASSERT_EQ(size_t(2), hashMapPtr3->size());

    // test full
    for (size_t i = 0; i < maxKeySize; ++i)
    {
        hashMapPtr0->insert(ConstString(string("key")
                        + StringUtil::toString(i + 2)));
    }
    ASSERT_EQ(maxKeySize + 2, hashMapPtr0->size());
    hashMapManager.updatePrimary(hashMapPtr0);
    ASSERT_TRUE(hashMapManager._fastUpdateable);
    KeyMapPtr hashMapPtrFull0 = hashMapManager.getHashMapPrimary();
    ASSERT_FALSE(hashMapManager._fastUpdateable);
    ASSERT_EQ(maxKeySize + 2, hashMapPtrFull0->size());
    hashMapPtr1->insert(ConstString(string("newKey0")));
    hashMapManager.updatePrimary(hashMapPtr1);
    ASSERT_FALSE(hashMapManager._fastUpdateable);
    KeyMapPtr hashMapPtrFull1 = hashMapManager.getHashMapPrimary();
    ASSERT_FALSE(hashMapPtrFull1);
}

IE_NAMESPACE_END(document);

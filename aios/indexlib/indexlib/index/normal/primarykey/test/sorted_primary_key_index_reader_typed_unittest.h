#ifndef __INDEXLIB_SORTED_PRIMARY_KEY_INDEX_READER_TYPED_UNITTEST_H
#define __INDEXLIB_SORTED_PRIMARY_KEY_INDEX_READER_TYPED_UNITTEST_H

#include "indexlib/index/normal/primarykey/test/primary_key_index_reader_typed_unittest.h"
#include "indexlib/file_system/buffered_file_reader.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class FakePrimaryKeyIndexReaderTyped : public PrimaryKeyIndexReaderTyped<Key>
{
public:
    typedef typename std::tr1::shared_ptr<std::vector<PKPair<Key> > > PKPairVecPtr;
public:
    void DeserializePKVec(PKPairVecPtr& pkVecPtr,
                          file_system::BufferedFileReaderPtr& fileReader)
    {
        // should not be here
        // load sorted pk does not need to sort again
        INDEXLIB_TEST_TRUE(false);
    }
};

class SortedPrimaryKeyIndexReaderTypedTest : public PrimaryKeyIndexReaderTypedTest
{
public:
    SortedPrimaryKeyIndexReaderTypedTest() {};
    virtual ~SortedPrimaryKeyIndexReaderTypedTest() {};

    DECLARE_CLASS_NAME(SortedPrimaryKeyIndexReaderTypedTest);

public:
    void TestCaseForSortedPKWrite();
    void TestCaseForSortedPKRead();

protected:
    virtual config::IndexConfigPtr CreateIndexConfig(const uint64_t key,
            const std::string& indexName, bool hasPKAttribute = true) override;
    virtual config::IndexConfigPtr CreateIndexConfig(const autil::uint128_t key,
            const std::string& indexName, bool hasPKAttribute = true) override;
};

IE_NAMESPACE_END(index);

#endif // __INDEXLIB_SORTED_PRIMARY_KEY_INDEX_READER_TYPED_UNITTEST_H

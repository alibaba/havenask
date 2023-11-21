#pragma once

#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/index/normal/primarykey/test/primary_key_index_reader_typed_unittest.h"

namespace indexlib { namespace index {

template <typename Key>
class FakePrimaryKeyIndexReaderTyped : public PrimaryKeyIndexReaderTyped<Key>
{
public:
    typedef typename std::shared_ptr<std::vector<PKPair<Key>>> PKPairVecPtr;

public:
    void DeserializePKVec(PKPairVecPtr& pkVecPtr, file_system::BufferedFileReaderPtr& fileReader)
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
    virtual config::IndexConfigPtr CreateIndexConfig(const uint64_t key, const std::string& indexName,
                                                     bool hasPKAttribute = true) override;
    virtual config::IndexConfigPtr CreateIndexConfig(const autil::uint128_t key, const std::string& indexName,
                                                     bool hasPKAttribute = true) override;
};
}} // namespace indexlib::index

#ifndef __INDEXLIB_HASHPRIMARYKEYFORMATTERTEST_H
#define __INDEXLIB_HASHPRIMARYKEYFORMATTERTEST_H

#include <autil/StringUtil.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator_creator.h"
#include "indexlib/util/hash_string.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"

IE_NAMESPACE_BEGIN(index);

class HashPrimaryKeyFormatterTest : public INDEXLIB_TESTBASE
{
public:
    HashPrimaryKeyFormatterTest();
    ~HashPrimaryKeyFormatterTest();

    DECLARE_CLASS_NAME(HashPrimaryKeyFormatterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDeserializeToSliceFile();
    void TestCalculatePkSliceFileLen();
    void TestSerializeFromHashMap();

private:
    template<typename Key>
    void CheckSliceFile(const file_system::SliceFilePtr& sliceFile,
                        const std::string& docs, const std::string& expectDocids);

    template <typename Key>
    std::tr1::shared_ptr<PrimaryKeyIterator<Key> >
    CreatePkIterator(const config::PrimaryKeyIndexConfigPtr& pkConfig,
                     const index_base::PartitionDataPtr& partitionData);

    void InnerTestDeserializeToSliceFile(
        const std::string& docString,
        size_t pkCount, size_t docCount,
        const std::string& pkString, const std::string& docidStr);

    void InnerTestDeserializeFromPkIndexType(
        PrimaryKeyIndexType pkIndexType, bool isUint128,
        const std::string& docString, size_t pkCount,
        size_t docCount, const std::string& pkString,
        const std::string& docidStr);

private:
    file_system::DirectoryPtr mRootDirectory;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyFormatterTest, TestDeserializeToSliceFile);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyFormatterTest, TestCalculatePkSliceFileLen);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyFormatterTest, TestSerializeFromHashMap);

///////////////////////////////////////////
template <typename Key>
std::tr1::shared_ptr<PrimaryKeyIterator<Key> >
HashPrimaryKeyFormatterTest::CreatePkIterator(
    const config::PrimaryKeyIndexConfigPtr& pkConfig,
    const index_base::PartitionDataPtr& partitionData)
{
    std::tr1::shared_ptr<PrimaryKeyIterator<Key> > pkIterator =
        PrimaryKeyIteratorCreator::Create<Key>(pkConfig);

    std::vector<index_base::SegmentData> segDataVec;
    index_base::PartitionData::Iterator iter = partitionData->Begin();
    for (; iter != partitionData->End(); ++iter)
    {
        segDataVec.push_back(*iter);
    }
    pkIterator->Init(segDataVec);
    return pkIterator;
}

template<typename Key>
inline void HashPrimaryKeyFormatterTest::CheckSliceFile(
        const file_system::SliceFilePtr& sliceFile,
        const std::string& docs, const std::string& expectDocids)
{

    file_system::SliceFileReaderPtr sliceFileReader = sliceFile->CreateSliceFileReader();
    char* baseAddress = (char*)sliceFileReader->GetBaseAddress();
    
    std::vector<std::string> docsVec;
    autil::StringUtil::fromString(docs, docsVec, ",");

    std::vector<docid_t> expectDocidVec;
    autil::StringUtil::fromString(expectDocids, expectDocidVec, ",");

    ASSERT_EQ(docsVec.size(), expectDocidVec.size());
    util::HashString hashFunc;
    HashPrimaryKeyFormatter<Key> formatter;
    for (size_t i = 0; i < docsVec.size(); i++)
    {
        Key hashKey;
        hashFunc.Hash(hashKey, docsVec[i].c_str(), docsVec[i].size());
        docid_t docid = formatter.Find(baseAddress, hashKey);
        ASSERT_EQ(expectDocidVec[i], docid);
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_HASHPRIMARYKEYFORMATTERTEST_H

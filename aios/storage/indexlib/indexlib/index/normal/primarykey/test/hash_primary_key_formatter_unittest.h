#ifndef __INDEXLIB_HASHPRIMARYKEYFORMATTERTEST_H
#define __INDEXLIB_HASHPRIMARYKEYFORMATTERTEST_H

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator_creator.h"
#include "indexlib/index/normal/primarykey/test/mock_primary_key_loader_plan.h"
#include "indexlib/index/normal/primarykey/test/primary_key_formatter_helper.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/HashString.h"

using namespace std;
namespace indexlib { namespace index {

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
    void TestEstimateDirectLoadSizeUint64();
    void TestEstimateDirectLoadSizeUint128();

private:
    template <typename Key>
    void InnerTestEstimateDirectLoadSize(bool isLock, const vector<PKPair<Key>>& data);

    template <typename Key>
    void CheckSliceFile(const file_system::FileWriterPtr& sliceFile, const std::string& docs,
                        const std::string& expectDocids);

    template <typename Key>
    std::shared_ptr<PrimaryKeyIterator<Key>> CreatePkIterator(const config::PrimaryKeyIndexConfigPtr& pkConfig,
                                                              const index_base::PartitionDataPtr& partitionData);

    void InnerTestDeserializeToSliceFile(const std::string& docString, size_t pkCount, size_t docCount,
                                         const std::string& pkString, const std::string& docidStr);

    void InnerTestDeserializeFromPkIndexType(PrimaryKeyIndexType pkIndexType, bool isUint128,
                                             const std::string& docString, size_t pkCount, size_t docCount,
                                             const std::string& pkString, const std::string& docidStr);

private:
    file_system::DirectoryPtr mRootDirectory;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyFormatterTest, TestDeserializeToSliceFile);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyFormatterTest, TestCalculatePkSliceFileLen);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyFormatterTest, TestSerializeFromHashMap);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyFormatterTest, TestEstimateDirectLoadSizeUint64);
INDEXLIB_UNIT_TEST_CASE(HashPrimaryKeyFormatterTest, TestEstimateDirectLoadSizeUint128);

///////////////////////////////////////////

template <typename Key>
void HashPrimaryKeyFormatterTest::InnerTestEstimateDirectLoadSize(bool isLock, const vector<PKPair<Key>>& data)
{
    DirectoryPtr directory =
        PrimaryKeyFormatterHelper::PrepareMmapDirectory(isLock, GET_TEMP_DATA_PATH() + "/hash_pk/");
    HashPrimaryKeyFormatter<Key> formatter;
    string pkFileName = PrimaryKeyFormatterHelper::PreparePkData<Key>(data, formatter, directory);

    MockPrimaryKeyLoaderPlanPtr newLoadPlan(new MockPrimaryKeyLoaderPlan());
    EXPECT_CALL(*newLoadPlan, GetTargetFileName()).WillRepeatedly(Return(pkFileName));
    EXPECT_CALL(*newLoadPlan, GetTargetFileDirectory()).WillRepeatedly(Return(directory));
    if (isLock) {
        ASSERT_LE(data.size() * sizeof(PKPair<Key>), formatter.EstimateDirectLoadSize(newLoadPlan));
    } else {
        ASSERT_EQ(0, formatter.EstimateDirectLoadSize(newLoadPlan));
    }
}

template <typename Key>
std::shared_ptr<PrimaryKeyIterator<Key>>
HashPrimaryKeyFormatterTest::CreatePkIterator(const config::PrimaryKeyIndexConfigPtr& pkConfig,
                                              const index_base::PartitionDataPtr& partitionData)
{
    std::shared_ptr<PrimaryKeyIterator<Key>> pkIterator = PrimaryKeyIteratorCreator::Create<Key>(pkConfig);

    std::vector<index_base::SegmentData> segDataVec;
    index_base::PartitionData::Iterator iter = partitionData->Begin();
    for (; iter != partitionData->End(); ++iter) {
        segDataVec.push_back(*iter);
    }
    pkIterator->Init(segDataVec);
    return pkIterator;
}

template <typename Key>
inline void HashPrimaryKeyFormatterTest::CheckSliceFile(const file_system::FileWriterPtr& sliceFile,
                                                        const std::string& docs, const std::string& expectDocids)
{
    char* baseAddress = (char*)sliceFile->GetBaseAddress();

    std::vector<std::string> docsVec;
    autil::StringUtil::fromString(docs, docsVec, ",");

    std::vector<docid_t> expectDocidVec;
    autil::StringUtil::fromString(expectDocids, expectDocidVec, ",");

    ASSERT_EQ(docsVec.size(), expectDocidVec.size());
    util::HashString hashFunc;
    HashPrimaryKeyFormatter<Key> formatter;
    for (size_t i = 0; i < docsVec.size(); i++) {
        Key hashKey;
        hashFunc.Hash(hashKey, docsVec[i].c_str(), docsVec[i].size());
        docid_t docid = formatter.Find(baseAddress, hashKey);
        ASSERT_EQ(expectDocidVec[i], docid);
    }
}
}} // namespace indexlib::index

#endif //__INDEXLIB_HASHPRIMARYKEYFORMATTERTEST_H

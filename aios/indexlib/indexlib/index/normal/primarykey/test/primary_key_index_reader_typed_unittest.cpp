#include "indexlib/index/normal/primarykey/test/primary_key_index_reader_typed_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/util/hash_string.h"

using namespace autil;
using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestCaseForOpenWithSegmentSort);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestCaseForOpenUInt64);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestCaseForOpenWithoutPKAttributeUInt64);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestCaseForOpenFailedUInt64);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestCaseForOpenUInt128);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestCaseForOpenWithoutPKAttributeUInt128);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestCaseForOpenFailedUInt128);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestLookupWithType);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexReaderTypedTest, TestLookupWithTypeFor128);


void PrimaryKeyIndexReaderTypedTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH() + '/';
    mDeletionMapReader.reset(new DeletionMapReader());
}

void PrimaryKeyIndexReaderTypedTest::CaseTearDown()
{
}

void PrimaryKeyIndexReaderTypedTest::Reset()
{
    TestDataPathTearDown();
    TestDataPathSetup();
    mDeletionMapReader.reset(new DeletionMapReader());
}

void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenWithSegmentSort()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int64", SFP_PK_INDEX);
    provider.Build("1,2,3#1,2,3#1,2,4,5,6,10,11#4,7,8,9", SFP_OFFLINE);

    {
        //test sort by base docid
        PrimaryKeyIndexReaderTyped<uint64_t> pkReader;
        pkReader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
        ASSERT_EQ((size_t)4, pkReader.mSegmentList.size());
        ASSERT_EQ((docid_t)13, pkReader.mSegmentList[0].first);
        ASSERT_EQ((docid_t)6, pkReader.mSegmentList[1].first);
        ASSERT_EQ((docid_t)3, pkReader.mSegmentList[2].first);
        ASSERT_EQ((docid_t)0, pkReader.mSegmentList[3].first);
    }
    
    {
        //test sort by doc count
        PrimaryKeyIndexConfigPtr pkIndexConfig = 
            DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, provider.GetIndexConfig());
        pkIndexConfig->SetPrimaryKeyLoadParam(
                PrimaryKeyLoadStrategyParam::SORTED_VECTOR, false, "");
        PrimaryKeyIndexReaderTyped<uint64_t> pkReader;
        pkReader.Open(pkIndexConfig, provider.GetPartitionData());
        ASSERT_EQ((size_t)4, pkReader.mSegmentList.size());
        ASSERT_EQ((docid_t)6, pkReader.mSegmentList[0].first);
        ASSERT_EQ((docid_t)13, pkReader.mSegmentList[1].first);
        ASSERT_EQ((docid_t)3, pkReader.mSegmentList[2].first);
        ASSERT_EQ((docid_t)0, pkReader.mSegmentList[3].first);
    }
}

void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenUInt64()
{
    TestCaseForOpenTyped<uint64_t>();
}

void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenWithoutPKAttributeUInt64()
{
    TestCaseForOpenTyped<uint64_t>(false);
}

void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenFailedUInt64()
{
    TestCaseForOpenFailedTyped<uint64_t>(true);
    Reset();
    TestCaseForOpenFailedTyped<uint64_t>(false);
}

void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenUInt128()
{
    TestCaseForOpenTyped<uint128_t>();
}

void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenWithoutPKAttributeUInt128()
{
    TestCaseForOpenTyped<uint128_t>(false);
}

void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenFailedUInt128()
{
    TestCaseForOpenFailedTyped<uint128_t>(true);
    Reset();
    TestCaseForOpenFailedTyped<uint128_t>(false);
}

IndexConfigPtr PrimaryKeyIndexReaderTypedTest::CreateIndexConfig(
        const uint64_t key, const string& indexName, 
        bool hasPKAttribute)
{
    return PrimaryKeyTestCaseHelper::CreateIndexConfig<uint64_t>("pk", pk_sort_array,
            hasPKAttribute);
}

IndexConfigPtr PrimaryKeyIndexReaderTypedTest::CreateIndexConfig(
        const uint128_t key, const string& indexName,
        bool hasPKAttribute)
{
    return PrimaryKeyTestCaseHelper::CreateIndexConfig<uint128_t>("pk",
            pk_sort_array, hasPKAttribute);
}

UInt64InMemPKSegmentReaderPtr PrimaryKeyIndexReaderTypedTest::CreateInMemPkSegmentReader(
        const vector<vector<uint32_t> >& rtDocInfos, docid_t baseDocId, 
        DeletionMapReaderPtr deletionMapReader)
{
    typedef HashMap<uint64_t, docid_t> PKHashMap;
    typedef std::tr1::shared_ptr<PKHashMap> PKHashMapPtr;

    assert(deletionMapReader);
    if (rtDocInfos.empty())
    {
        return UInt64InMemPKSegmentReaderPtr();
    }

    PKHashMapPtr hashMap(new PKHashMap(HASHMAP_INIT_SIZE));
    for (size_t i = 0; i < rtDocInfos.size(); ++i)
    {
        assert(rtDocInfos[i].size() == 3);
        docid_t localDocId = rtDocInfos[i][0];
        hashMap->FindAndInsert((uint64_t)rtDocInfos[i][1], localDocId);
        if (rtDocInfos[i][2] == 1)
        {
            deletionMapReader->Delete(baseDocId + localDocId);
        }
    }
    return UInt64InMemPKSegmentReaderPtr(
            new UInt64InMemPKSegmentReader(hashMap));
}

void PrimaryKeyIndexReaderTypedTest::TestSimpleProcess()
{
    mIndexConfig = CreateIndexConfig((uint64_t)0, "pk", false);
    mSchema = PrimaryKeyTestCaseHelper::CreatePrimaryKeySchema<uint64_t>(false);

    uint32_t urls[] = {1, 2, 4, 8, 16, 32, 64};
    uint32_t urlCount = sizeof(urls) / sizeof(urls[0]);
    uint32_t urlsPerSegment[] = {3, 2, 2};
    segmentid_t segmentIds[] = {0, 1, 2};
    uint32_t segmentCount = sizeof(segmentIds) / sizeof(segmentIds[0]);
    versionid_t versionId = 0;
    Version version = MakeOneVersion<uint64_t>(urls, urlsPerSegment, segmentIds, 
            segmentCount, versionId);
        
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    index_base::PartitionDataPtr partitionData = 
        test::PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, mSchema);

    PrimaryKeyIndexReaderTyped<uint64_t> reader;
    reader.Open(mIndexConfig, partitionData);
    mDeletionMapReader->Open(partitionData.get());
    reader.mDeletionMapReader = mDeletionMapReader;

    autil::mem_pool::Pool pool;
    for (uint32_t i = 0; i < urlCount; ++i)
    {
        std::stringstream ss;
        ss << "url" << urls[i];
        string pkString = ss.str();

        INDEXLIB_TEST_EQUAL((docid_t)i, reader.Lookup(pkString));
        common::Term term(pkString, "");
        PostingIterator *iter = reader.Lookup(term, 1000, pt_default, &pool);
        INDEXLIB_TEST_TRUE(iter);
        INDEXLIB_TEST_EQUAL((docid_t)i, iter->SeekDoc(0));
        INDEXLIB_TEST_EQUAL((docid_t)INVALID_DOCID, iter->SeekDoc(i));
        PostingIterator* cloneIter = iter->Clone();
        INDEXLIB_TEST_TRUE(cloneIter);
        INDEXLIB_TEST_EQUAL((docid_t)i, cloneIter->SeekDoc(0));
        INDEXLIB_TEST_EQUAL((docid_t)INVALID_DOCID, cloneIter->SeekDoc(i));
        IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, iter);
        IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, cloneIter);
    }
}

void PrimaryKeyIndexReaderTypedTest::doTestLookupWithType(
        bool enableNumberPkHash)
{
    stringstream oss;
    oss << "EnableNumberPKHash:" << (enableNumberPkHash ? "TRUE" : "FALSE");
    SCOPED_TRACE(oss.str());

    TearDown();
    SetUp();

    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_PK_INDEX);

    PrimaryKeyIndexConfigPtr pkIndexConfig = 
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, provider.GetIndexConfig());
    if (enableNumberPkHash)
    {
        pkIndexConfig->SetPrimaryKeyHashType(pk_number_hash);
    }
    provider.Build("1#2,3,4", SFP_OFFLINE);
    provider.Build("5", SFP_REALTIME);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    PrimaryKeyIndexReaderTyped<uint64_t> pkReader;
    pkReader.Open(provider.GetIndexConfig(), partitionData);

    // test template of value
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<int8_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<uint8_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<int16_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<uint16_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<int32_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<uint32_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<int64_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<uint64_t>(2));
    //ASSERT_EQ((docid_t)1, pkReader.LookupWithType<float>(2));//compile error
    //ASSERT_EQ((docid_t)1, pkReader.LookupWithType<double>(2));//compile error
    //ASSERT_EQ((docid_t)1, pkReader.LookupWithType<string>("2"));//compile error
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType(string("2")));

    // test normal
    ASSERT_EQ((docid_t)0, pkReader.LookupWithType((uint64_t)1));
    ASSERT_EQ((docid_t)4, pkReader.LookupWithType((uint32_t)5));
    ASSERT_EQ((docid_t)2, pkReader.LookupWithType(string("3")));
    ASSERT_EQ((docid_t)3, pkReader.LookupWithType((uint8_t)4));
    //ASSERT_EQ((docid_t)3, pkReader.LookupWithType((double)4.0));//compile error
}

void PrimaryKeyIndexReaderTypedTest::doTestLookupWithTypeFor128(
        bool enableNumberPkHash)
{
    stringstream oss;
    oss << "EnableNumberPKHash:" << (enableNumberPkHash ? "TRUE" : "FALSE");
    SCOPED_TRACE(oss.str());

    TearDown();
    SetUp();

    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "uint64", SFP_PK_128_INDEX);

    PrimaryKeyIndexConfigPtr pkIndexConfig = 
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, provider.GetIndexConfig());
    if (enableNumberPkHash)
    {
        pkIndexConfig->SetPrimaryKeyHashType(pk_number_hash);
    }
    provider.Build("1#2,3,4", SFP_OFFLINE);
    provider.Build("5", SFP_REALTIME);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    PrimaryKeyIndexReaderTyped<autil::uint128_t> pkReader;
    pkReader.Open(provider.GetIndexConfig(), partitionData);

    // test template of value
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<int8_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<uint8_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<int16_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<uint16_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<int32_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<uint32_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<int64_t>(2));
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType<uint64_t>(2));
    //ASSERT_EQ((docid_t)1, pkReader.LookupWithType<float>(2));//compile error
    //ASSERT_EQ((docid_t)1, pkReader.LookupWithType<double>(2));//compile error
    //ASSERT_EQ((docid_t)1, pkReader.LookupWithType<string>("2"));//compile error
    ASSERT_EQ((docid_t)1, pkReader.LookupWithType(string("2")));

    // test normal
    ASSERT_EQ((docid_t)0, pkReader.LookupWithType((uint64_t)1));
    ASSERT_EQ((docid_t)4, pkReader.LookupWithType((uint32_t)5));
    ASSERT_EQ((docid_t)2, pkReader.LookupWithType(string("3")));
    ASSERT_EQ((docid_t)3, pkReader.LookupWithType((uint8_t)4));
    //ASSERT_EQ((docid_t)3, pkReader.LookupWithType((double)4.0));//compile error
}

void PrimaryKeyIndexReaderTypedTest::TestLookupWithType()
{
    doTestLookupWithType(true);
    doTestLookupWithType(false);
}

void PrimaryKeyIndexReaderTypedTest::TestLookupWithTypeFor128()
{
    doTestLookupWithTypeFor128(true);
    doTestLookupWithTypeFor128(false);
}

template <typename Key>
void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenTyped(bool hasPKAttr)
{
    // make document
    mIndexConfig = CreateIndexConfig((Key)0, "pk", hasPKAttr);
    mSchema = PrimaryKeyTestCaseHelper::CreatePrimaryKeySchema<Key>(hasPKAttr);
        
    uint32_t urls[] = {1, 2, 4};
    uint32_t urlCount = sizeof(urls) / sizeof(urls[0]);
    uint32_t urlsPerSegment[] = {3};
    segmentid_t segmentIds[] = {0};
    uint32_t segmentCount = sizeof(segmentIds) / sizeof(segmentIds[0]);
    versionid_t versionId = 0;
    Version version = MakeOneVersion<Key>(urls, urlsPerSegment, segmentIds, 
            segmentCount, versionId);
        
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);
    index_base::PartitionDataPtr partitionData = 
        test::PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, mSchema);

    PrimaryKeyIndexReaderTyped<Key> reader;
    reader.Open(mIndexConfig, partitionData);
    mDeletionMapReader->Open(partitionData.get());
    reader.mDeletionMapReader = mDeletionMapReader;

    AttributeReaderPtr pkAttrReader = reader.GetPKAttributeReader();
    if (hasPKAttr)
    {
        INDEXLIB_TEST_TRUE(pkAttrReader);
        INDEXLIB_TEST_EQUAL(false, pkAttrReader->IsMultiValue());
        if (typeid(Key) == typeid(uint64_t))
        {
            INDEXLIB_TEST_EQUAL(AT_UINT64, pkAttrReader->GetType());
        }
        else if (typeid(Key) == typeid(autil::uint128_t))
        {
            INDEXLIB_TEST_EQUAL(AT_HASH_128, pkAttrReader->GetType());
        }
    }
    else
    {
        INDEXLIB_TEST_TRUE(!pkAttrReader);            
    }
    autil::mem_pool::Pool pool;
    for (uint32_t i = 0; i < urlCount; ++i)
    {
        std::stringstream ss;
        ss << "url" << urls[i];
        INDEXLIB_TEST_EQUAL((docid_t)i, reader.Lookup(ss.str()));
        common::Term term(ss.str(), "");
        PostingIterator *iter = reader.Lookup(term, 1000, pt_default, &pool);
        INDEXLIB_TEST_TRUE(iter);
        INDEXLIB_TEST_EQUAL((docid_t)i, iter->SeekDoc(0));
        INDEXLIB_TEST_EQUAL((docid_t)INVALID_DOCID, iter->SeekDoc(i));
        PostingIterator* cloneIter = iter->Clone();
        INDEXLIB_TEST_TRUE(cloneIter);
        INDEXLIB_TEST_EQUAL((docid_t)i, cloneIter->SeekDoc(0));
        INDEXLIB_TEST_EQUAL((docid_t)INVALID_DOCID, cloneIter->SeekDoc(i));
        IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, iter);
        IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, cloneIter);
        if (hasPKAttr)
        {
            std::string strKey;
            pkAttrReader->Read((docid_t)i, strKey);
            util::HashString hashFunc;
            Key expectHashKey;
            hashFunc.Hash(expectHashKey, ss.str().c_str(), ss.str().size());
            std::stringstream ss;
            ss << expectHashKey;
            INDEXLIB_TEST_EQUAL(ss.str(), strKey);
        }
    }

    INDEXLIB_TEST_EQUAL(INVALID_DOCID, reader.Lookup("url_not_exist"));
    common::Term term("url_not_exist", "");
    PostingIterator *iter = reader.Lookup(term, 1000, pt_default, &pool);
    INDEXLIB_TEST_TRUE(!iter);
        
}

template <typename Key>
void PrimaryKeyIndexReaderTypedTest::TestCaseForOpenFailedTyped(bool hasPKAttr)
{
    mIndexConfig =  CreateIndexConfig((Key)0, "pk", hasPKAttr);
    mSchema = PrimaryKeyTestCaseHelper::CreatePrimaryKeySchema<Key>(hasPKAttr);        
        
    uint32_t urls[] = {1, 2, 4};
    uint32_t urlsPerSegment[] = {3};
    segmentid_t segmentIds[] = {0};
    uint32_t segmentCount = sizeof(segmentIds) / sizeof(segmentIds[0]);
    versionid_t versionId = 0;
    Version version = MakeOneVersion<Key>(urls, urlsPerSegment, segmentIds, 
            segmentCount, versionId);

    std::string fileToDelete = mRootDir + "segment_0_level_0/index/pk/";
    if (hasPKAttr)
    {
        fileToDelete += std::string(PK_ATTRIBUTE_DIR_NAME_PREFIX) +
                        "_pk/" + ATTRIBUTE_DATA_FILE_NAME;
    }
    else
    {
        fileToDelete += PRIMARY_KEY_DATA_FILE_NAME;
    }
    storage::FileSystemWrapper::DeleteFile(fileToDelete);

    // check data
    PrimaryKeyIndexReaderTyped<Key> reader;
    bool hasException = false;

    config::IndexPartitionOptions options;
    options.SetIsOnline(false);
    index_base::PartitionDataPtr partitionData = 
        test::PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, mSchema);

    try 
    {
        reader.Open(mIndexConfig, partitionData);
        mDeletionMapReader->Open(partitionData.get());
        reader.mDeletionMapReader = mDeletionMapReader;
    }
    catch (const misc::ExceptionBase &e)
    {
        hasException = true;
    }
    INDEXLIB_TEST_TRUE(hasException);
}

template<typename Key>
inline void PrimaryKeyIndexReaderTypedTest::PkPairUniq(
        typename PrimaryKeyIndexReaderTyped<Key>::PKPairVec& pkPairVec)
{
    std::sort(pkPairVec.begin(), pkPairVec.end());
    Key lastKey = 0;
    size_t cursor = 0;
    for (size_t i = 0; i < pkPairVec.size(); ++i)
    {
        if (i == 0)
        { 
            lastKey = pkPairVec[i].key; 
            continue; 
        }

        if (lastKey == pkPairVec[i].key)
        {
            pkPairVec[cursor] = pkPairVec[i];
        }
        else
        {
            pkPairVec[++cursor] = pkPairVec[i];    
            lastKey = pkPairVec[i].key;
        }
    }
    pkPairVec.resize(cursor + 1);
}



IE_NAMESPACE_END(index);

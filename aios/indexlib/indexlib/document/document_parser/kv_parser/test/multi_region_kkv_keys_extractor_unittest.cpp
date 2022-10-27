#include "indexlib/document/document_parser/kv_parser/test/multi_region_kkv_keys_extractor_unittest.h"
#include "indexlib/test/region_schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/common/multi_region_rehasher.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, MultiRegionKkvKeysExtractorTest);

MultiRegionKkvKeysExtractorTest::MultiRegionKkvKeysExtractorTest()
{
}

MultiRegionKkvKeysExtractorTest::~MultiRegionKkvKeysExtractorTest()
{
}

void MultiRegionKkvKeysExtractorTest::CaseSetUp()
{
}

void MultiRegionKkvKeysExtractorTest::CaseTearDown()
{
}

void MultiRegionKkvKeysExtractorTest::TestSimpleProcess()
{
    string field = "uid:string;friendid:int32;value:uint32;";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "uid", "friendid", "value;friendid;", ""); 
    maker.AddRegion("region2", "friendid", "uid", "value;uid;", "");
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    MultiRegionKKVKeysExtractor extractor(schema);

    string uid, friendid;
    uint64_t uidHash, friendHash;
    uint64_t expUidHash, expFriendHash;

    // test valid case
    {
        uid = "superman";
        friendid = "1";
        ASSERT_TRUE(extractor.HashPrefixKey(uid, uidHash, 0));
        ASSERT_TRUE(GetHashKey(hft_murmur, ConstString(uid), expUidHash));
        ASSERT_EQ(expUidHash, uidHash);
        ASSERT_TRUE(extractor.HashSuffixKey(friendid, friendHash, 0));
        ASSERT_TRUE(GetHashKey(hft_int64, ConstString(friendid), expFriendHash));
        ASSERT_EQ(expFriendHash, friendHash);

        string docStr = "cmd=add,uid=superman,friendid=1,value=100;";
        NormalDocumentPtr doc1 = DocumentCreator::CreateKVDocument(
            schema, docStr, false, "region1");
        NormalDocumentPtr doc2 = DocumentCreator::CreateKVDocument(
            schema, docStr, false, "region2");

        KKVKeysExtractor::OperationType opType;
        ASSERT_TRUE(extractor.GetKeys(doc1, uidHash, friendHash, opType));
        ASSERT_EQ(MultiRegionRehasher::GetRehashKey(expUidHash, 0), uidHash);
        ASSERT_EQ(expFriendHash, friendHash);
        EXPECT_EQ(KKVKeysExtractor::OperationType::ADD, opType);

        ASSERT_TRUE(extractor.HashPrefixKey(friendid, friendHash, 1));
        ASSERT_TRUE(GetHashKey(hft_int64, ConstString(friendid), expFriendHash));
        ASSERT_EQ(expFriendHash, friendHash);
        ASSERT_TRUE(extractor.HashSuffixKey(uid, uidHash, 1));
        ASSERT_TRUE(GetHashKey(hft_murmur, ConstString(uid), expUidHash));
        ASSERT_EQ(expUidHash, uidHash);

        ASSERT_TRUE(extractor.GetKeys(doc2, friendHash, uidHash, opType));
        ASSERT_EQ(MultiRegionRehasher::GetRehashKey(expFriendHash, 1), friendHash); 
        ASSERT_EQ(expUidHash, uidHash);
        EXPECT_EQ(KKVKeysExtractor::OperationType::ADD, opType);        
    }
    // test valid regiondid
    {
        uid = "superman";
        friendid = "1";
        ASSERT_FALSE(extractor.HashPrefixKey(uid, uidHash, 2));
        ASSERT_FALSE(extractor.HashSuffixKey(friendid, friendHash, -1));

        string docStr = "cmd=add,uid=superman,friendid=1,value=100;";
        NormalDocumentPtr doc1 = DocumentCreator::CreateKVDocument(
            schema, docStr, false, "region1");

        KKVKeysExtractor::OperationType opType;
        doc1->SetRegionId(2);
        ASSERT_FALSE(extractor.GetKeys(doc1, uidHash, friendHash, opType));
    }    
    
}

IE_NAMESPACE_END(document);


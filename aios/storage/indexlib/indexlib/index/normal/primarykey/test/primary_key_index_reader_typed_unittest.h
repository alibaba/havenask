#pragma once

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_writer_typed.h"
#include "indexlib/index/normal/primarykey/test/primary_key_test_case_helper.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
namespace indexlib { namespace index {

typedef InMemPrimaryKeySegmentReaderTyped<uint64_t> UInt64InMemPKSegmentReader;
typedef shared_ptr<UInt64InMemPKSegmentReader> UInt64InMemPKSegmentReaderPtr;

class PrimaryKeyIndexReaderTypedTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    PrimaryKeyIndexReaderTypedTest() {};
    virtual ~PrimaryKeyIndexReaderTypedTest() {};

    DECLARE_CLASS_NAME(PrimaryKeyIndexReaderTypedTest);

public:
    virtual void CaseSetUp() override;
    virtual void CaseTearDown() override;

    virtual void TestCaseForOpenUInt64();
    virtual void TestCaseForOpenWithoutPKAttributeUInt64();
    virtual void TestCaseForOpenFailedUInt64();
    virtual void TestCaseForOpenUInt128();
    virtual void TestCaseForOpenWithoutPKAttributeUInt128();
    virtual void TestCaseForOpenFailedUInt128();

    void TestCaseForOpenWithSegmentSort();
    void TestSimpleProcess();
    void TestLookupWithType();
    void TestLookupWithTypeFor128();

protected:
    virtual config::IndexConfigPtr CreateIndexConfig(const uint64_t key, const string& indexName,
                                                     bool hasPKAttribute = true);
    virtual config::IndexConfigPtr CreateIndexConfig(const autil::uint128_t key, const string& indexName,
                                                     bool hasPKAttribute = true);
    virtual vector<string> GetSupporReadtMode();
    void ResetFileSystem(string readMode);
    void Reset();

private:
    void doTestLookupWithType(bool enableNumberPkHash);
    void doTestLookupWithTypeFor128(bool enableNumberPkHash);

    template <typename Key>
    void TestCaseForOpenTyped(bool hasPKAttr = true);

    template <typename Key>
    void InnerTestCaseForOpenTyped(bool hasPKAttr, string readMode, bool async);

    template <typename Key>
    void TestCaseForOpenFailedTyped(bool hasPKAttr = true);

    template <typename Key>
    void PkPairUniq(typename PrimaryKeyIndexReaderTyped<Key>::PKPairVec& pkPairVec);

protected:
    template <typename Key>
    index_base::Version MakeOneVersion(uint32_t urls[], uint32_t urlsPerSegment[], segmentid_t segmentIds[],
                                       uint32_t segmentCount, versionid_t versionId,
                                       const index_base::Version& lastVersion = index_base::Version());

    template <typename Key>
    void MakeOneSegment(uint32_t urls[], uint32_t urlCount, segmentid_t segmentId);

private:
    // incPkString: (segment_id, doc_id, pk, is_deleted);
    // rtPkString:  (doc_id, pk, is_deleted);
    UInt64PrimaryKeyIndexReaderPtr CreatePrimaryIndexReader(const string& incPkString, const string& rtPkString = "");

    UInt64InMemPKSegmentReaderPtr CreateInMemPkSegmentReader(const vector<vector<uint32_t>>& docInfos,
                                                             docid_t baseDocId, DeletionMapReaderPtr deletionMapReader);

protected:
    config::IndexConfigPtr mIndexConfig;
    string mRootDir;
    DeletionMapReaderPtr mDeletionMapReader;
    config::IndexPartitionSchemaPtr mSchema;
    util::SimplePool mSimplePool;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////////////////
template <typename Key>
inline index_base::Version PrimaryKeyIndexReaderTypedTest::MakeOneVersion(uint32_t urls[], uint32_t urlsPerSegment[],
                                                                          segmentid_t segmentIds[],
                                                                          uint32_t segmentCount, versionid_t versionId,
                                                                          const index_base::Version& lastVersion)
{
    index_base::Version version = lastVersion;
    version.SetVersionId(versionId);

    uint32_t curOffsetInUrl = 0;
    for (uint32_t i = 0; i < segmentCount; i++) {
        MakeOneSegment<Key>(urls + curOffsetInUrl, urlsPerSegment[i], segmentIds[i]);
        curOffsetInUrl += urlsPerSegment[i];
        version.AddSegment(segmentIds[i]);
    }

    version.Store(GET_PARTITION_DIRECTORY(), false);
    return version;
}

template <typename Key>
inline void PrimaryKeyIndexReaderTypedTest::MakeOneSegment(uint32_t urls[], uint32_t urlCount, segmentid_t segmentId)
{
    string docStr;
    PrimaryKeyTestCaseHelper::MakeFakePrimaryKeyDocStr(urls, urlCount, docStr);

    DocumentMaker::DocumentVect docVect;
    DocumentMaker::MockIndexPart mockIndexPart;
    DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);

    // build primary key index
    PrimaryKeyWriterTyped<Key> writer;
    writer.Init(mIndexConfig, NULL);
    for (size_t i = 0; i < docVect.size(); ++i) {
        writer.EndDocument(*(docVect[i]->GetIndexDocument().get()));
    }

    file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0";
    file_system::DirectoryPtr segDirectory = rootDirectory->MakeDirectory(ss.str());
    file_system::DirectoryPtr indexDirectory = segDirectory->MakeDirectory(INDEX_DIR_NAME);
    segDirectory->MakeDirectory(DELETION_MAP_DIR_NAME);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = urlCount;
    segInfo.Store(segDirectory);
    writer.Dump(indexDirectory, &mSimplePool);
}
}} // namespace indexlib::index

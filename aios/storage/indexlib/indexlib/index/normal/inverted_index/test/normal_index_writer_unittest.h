#pragma once

#include <memory>
#include <set>

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/section.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryTypedFactory.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

#define VOL_NAME "writer_vol1"
#define HIGH_FREQ_TOKEN "token0"
#define VOL_CONTENT "token0"
#define NUMBER_HIGH_FREQ_TOKEN "0"
#define NUMBER_VOL_CONTENT "0"

class NormalIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    typedef index::IndexDocumentMaker::PostingAnswerMap AnswerMap;
    typedef index::IndexDocumentMaker::KeyAnswer KeyAnswer;
    typedef index::IndexDocumentMaker::Answer Answer;
    typedef index::IndexDocumentMaker::KeyAnswerInDoc KeyAnswerInDoc;
    typedef index::IndexDocumentMaker::HashKeyToStrMap HashKeyToStrMap;

public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 10240;
    static const uint32_t FIELD_NUM_FOR_PACK = 10;
    static const uint32_t FIELD_NUM_FOR_EXPACK = 8;

    DECLARE_CLASS_NAME(NormalIndexWriterTest);

public:
    NormalIndexWriterTest();
    ~NormalIndexWriterTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForDumpDictInlinePosting();
    void TestCaseForNumberIndexWithOneDoc();
    void TestCaseForNumberIndexWithMultiDoc();
    void TestCaseForTextIndexWithOneDoc();
    void TestCaseForTextIndexWithMultiDoc();
    void TestCaseForExpackIndexWithOneDoc();
    void TestCaseForExpackIndexWithMultiDoc();
    void TestCaseForPackIndexWithOneDoc();
    void TestCaseForPackIndexWithMultiDoc();
    void TestCaseForPackIndexWithoutSectionAttribute();
    void TestCaseForCreateInMemReader();
    void TestCaseForCreateInMemReaderWithSection();
    void TestCaseForCreateInMemReaderWithBitmap();
    void TestCaseForCreateBitmapIndexWriter();
    void TestCaseForDumpIndexFormatOffline();
    void TestCaseForDumpIndexFormatOnline();
    void TestCaseForEstimateDumpMemoryUse();
    void TestCaseForNullTerm();

public:
    void DoTest(InvertedIndexType indexType, uint32_t docNum, uint32_t fieldNum, uint32_t secNum,
                optionflag_t optionFlag, bool hasVol = false, index::TokenType tokenType = index::tt_text,
                FieldType fieldType = ft_text);

    void CreateIndexConfig(InvertedIndexType indexType, uint32_t fieldNum, FieldType fieldType = ft_text,
                           bool hasVol = false);

    void CreateIndexWriter();

    void CheckFieldMap(docid_t docId, const HashKeyToStrMap& hashKeyToStrMap, Answer* answer);

    void BuildIndex(uint32_t docNum, uint32_t fieldNum, uint32_t secNum, bool hasVol, index::TokenType tokenType);

    void SetVocabulary(InvertedIndexType indexType);

    void CheckBitmap(Answer* answer);

    void CheckDistinctTermCount(uint32_t expectedTermCount)
    {
        INDEXLIB_TEST_EQUAL(expectedTermCount, mIndexWriter->GetDistinctTermCount());
    }

    void CheckPostingWriter(const HashKeyToStrMap& hashKeyToStrMap, Answer* answer);

    void CheckData(bool hasVol);

    // number index should override this
    void CheckNormalTokens();

    void CheckHighFreqToken();

    bool CheckKey(DictionaryReader* reader, dictkey_t key);

protected:
    void CreatePackIndexConfig(InvertedIndexType indexType, const std::string& indexName, uint32_t fieldNum,
                               bool hasVol);

    void CreateSingleFieldIndexConfig(InvertedIndexType indexType, const std::string& indexName, FieldType fieldType,
                                      bool hasVol = false);

private:
    void AddToken(const std::vector<document::Section*>& sectionVec);

    void InnerTestCaseForCreateInMemReader(bool hasSection, bool hasHighVol);

    void ResetCaseDirectory();

protected:
    util::SimplePool mSimplePool;
    config::IndexConfigPtr mIndexConfig;
    config::IndexPartitionSchemaPtr mSchema;
    index::IndexWriterPtr mIndexWriter;
    std::shared_ptr<config::HighFrequencyVocabulary> mHighFreqVol;
    file_system::DirectoryPtr mTestDir;
    file_system::IFileSystemPtr mFileSystem;
    std::set<dictkey_t> mTokens;
    index::TokenType mTokenType;
    optionflag_t mOptionFlag;
    uint64_t mDfSum;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForDumpDictInlinePosting);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForNumberIndexWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForNumberIndexWithMultiDoc);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForTextIndexWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForTextIndexWithMultiDoc);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForExpackIndexWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForExpackIndexWithMultiDoc);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForPackIndexWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForPackIndexWithMultiDoc);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForPackIndexWithoutSectionAttribute);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForCreateInMemReader);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForCreateInMemReaderWithSection);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForCreateInMemReaderWithBitmap);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForCreateBitmapIndexWriter);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForDumpIndexFormatOnline);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForDumpIndexFormatOffline);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForEstimateDumpMemoryUse);
INDEXLIB_UNIT_TEST_CASE(NormalIndexWriterTest, TestCaseForNullTerm);
}} // namespace indexlib::index

#include "indexlib/index/normal/inverted_index/test/normal_index_writer_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_encoder.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalIndexWriterTest);

NormalIndexWriterTest::NormalIndexWriterTest()
{
}

NormalIndexWriterTest::~NormalIndexWriterTest()
{
}

void NormalIndexWriterTest::CaseSetUp()
{
    mFileSystem = GET_FILE_SYSTEM();
    mTestDir = GET_SEGMENT_DIRECTORY();
}

void NormalIndexWriterTest::CaseTearDown()
{
    mIndexWriter.reset();
}

void NormalIndexWriterTest::TestCaseForDumpDictInlinePosting()
{
    NormalIndexWriter writer(1000, IndexPartitionOptions());
    IndexPartitionSchemaPtr indexPartitionSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(indexPartitionSchema,
            // field schema
            "test_field:uint8",
            // index schema
            "test_index:number:test_field",
            "", "");

    IndexConfigPtr indexConfig = 
        indexPartitionSchema->GetIndexSchema()->GetIndexConfig("test_index");
    indexConfig->SetDictInlineCompress(true);

    writer.Init(indexConfig, NULL);

    FieldSchemaPtr fieldSchema = indexPartitionSchema->GetFieldSchema();
    Pool pool;
    Field *field = DocumentMaker::MakeField(fieldSchema, "[test_field:(12)]", &pool);
    IndexDocument doc(&pool);
    doc.SetDocId(1);
    writer.AddField(field);
    writer.EndDocument(doc);
    writer.Dump(mTestDir, &mSimplePool);
    mFileSystem->Sync(true);
    string indexPath = indexConfig->GetIndexName() + "/";
    string postingFilePath = indexPath + POSTING_FILE_NAME;
    string dictFilePath = indexPath + DICTIONARY_FILE_NAME;

    INDEXLIB_TEST_TRUE(mTestDir->IsExist(postingFilePath));
    INDEXLIB_TEST_EQUAL((size_t)0, mTestDir->GetFileLength(postingFilePath));

    TieredDictionaryReaderTyped<uint8_t> reader;
    reader.Open(mTestDir->GetDirectory(indexConfig->GetIndexName(), true), 
                DICTIONARY_FILE_NAME);

    dictvalue_t value = 0;
    INDEXLIB_TEST_TRUE(reader.Lookup(12, value));
    IndexFormatOption indexFormatOption;
    indexFormatOption.Init(indexConfig);

    uint64_t compressValue = 0;
    DictInlineFormatter formatter(indexFormatOption.GetPostingFormatOption());
    formatter.SetTermPayload(0);
    formatter.SetDocId(1);
    formatter.SetDocPayload(0);
    formatter.SetTermFreq(1);
    formatter.SetFieldMap(0);
    formatter.GetDictInlineValue(compressValue);
    dictvalue_t expectValue = ShortListOptimizeUtil::
                              CreateDictInlineValue(compressValue);

    INDEXLIB_TEST_EQUAL(expectValue, value);
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, field);
}

void NormalIndexWriterTest::TestCaseForNumberIndexWithOneDoc()
{
    DoTest(it_number_int8, 1, 1, 1, NO_POSITION_LIST, false, tt_number, ft_int8);
    DoTest(it_number_int8, 1, 1, 1, NO_POSITION_LIST, true, tt_number, ft_int8);

    DoTest(it_number_uint8, 1, 1, 1, NO_POSITION_LIST, false, tt_number, ft_uint8);
    DoTest(it_number_uint8, 1, 1, 1, NO_POSITION_LIST, true, tt_number, ft_uint8);

    DoTest(it_number_int16, 1, 1, 1, NO_POSITION_LIST, false, tt_number, ft_int16);
    DoTest(it_number_int16, 1, 1, 1, NO_POSITION_LIST, true, tt_number, ft_int16);

    DoTest(it_number_uint16, 1, 1, 1, NO_POSITION_LIST, false, tt_number, ft_uint16);
    DoTest(it_number_uint16, 1, 1, 1, NO_POSITION_LIST, true, tt_number, ft_uint16);

    DoTest(it_number_int32, 1, 1, 1, NO_POSITION_LIST, false, tt_number, ft_int32);
    DoTest(it_number_int32, 1, 1, 1, NO_POSITION_LIST, true, tt_number, ft_int32);

    DoTest(it_number_uint32, 1, 1, 1, NO_POSITION_LIST, false, tt_number, ft_uint32);
    DoTest(it_number_uint32, 1, 1, 1, NO_POSITION_LIST, true, tt_number, ft_uint32);

    DoTest(it_number_int64, 1, 1, 1, NO_POSITION_LIST, false, tt_number, ft_int64);
    DoTest(it_number_int64, 1, 1, 1, NO_POSITION_LIST, true, tt_number, ft_int64);

    DoTest(it_number_uint64, 1, 1, 1, NO_POSITION_LIST, false, tt_number, ft_uint64);
    DoTest(it_number_uint64, 1, 1, 1, NO_POSITION_LIST, true, tt_number, ft_uint64);
}

void NormalIndexWriterTest::TestCaseForNumberIndexWithMultiDoc()
{
    DoTest(it_number, 10, 1, 10, NO_POSITION_LIST, false, tt_number, ft_int8);
    DoTest(it_number_int8, 10, 1, 10, NO_POSITION_LIST, true, tt_number, ft_int8);

    DoTest(it_number_uint8, 10, 1, 10, NO_POSITION_LIST, false, tt_number, ft_uint8);
    DoTest(it_number_uint8, 10, 1, 10, NO_POSITION_LIST, true, tt_number, ft_uint8);

    DoTest(it_number_int16, 10, 1, 10, NO_POSITION_LIST, false, tt_number, ft_int16);
    DoTest(it_number_int16, 10, 1, 10, NO_POSITION_LIST, true, tt_number, ft_int16);

    DoTest(it_number_uint16, 10, 1, 10, NO_POSITION_LIST, false, tt_number, ft_uint16);
    DoTest(it_number_uint16, 10, 1, 10, NO_POSITION_LIST, true, tt_number, ft_uint16);

    DoTest(it_number_int32, 10, 1, 10, NO_POSITION_LIST, false, tt_number, ft_int32);
    DoTest(it_number_int32, 10, 1, 10, NO_POSITION_LIST, true, tt_number, ft_int32);

    DoTest(it_number_uint32, 10, 1, 10, NO_POSITION_LIST, false, tt_number, ft_uint32);
    DoTest(it_number_uint32, 10, 1, 10, NO_POSITION_LIST, true, tt_number, ft_uint32);

    DoTest(it_number_int64, 10, 1, 10, NO_POSITION_LIST, false, tt_number, ft_int64);
    DoTest(it_number_int64, 10, 1, 10, NO_POSITION_LIST, true, tt_number, ft_int64);

    DoTest(it_number_uint64, 10, 1, 10, NO_POSITION_LIST, false, tt_number, ft_uint64);
    DoTest(it_number_uint64, 10, 1, 10, NO_POSITION_LIST, true, tt_number, ft_uint64);
}

void NormalIndexWriterTest::TestCaseForTextIndexWithOneDoc()
{
    DoTest(it_text, 1, 1, 1, OPTION_FLAG_ALL);
    DoTest(it_text, 1, 1, 1, OPTION_FLAG_ALL, true);

    DoTest(it_text, 1, 1, 1, NO_PAYLOAD);
    DoTest(it_text, 1, 1, 1, NO_PAYLOAD, true);

    DoTest(it_text, 1, 1, 1, NO_POSITION_LIST);
    DoTest(it_text, 1, 1, 1, NO_POSITION_LIST, true);
}

void NormalIndexWriterTest::TestCaseForTextIndexWithMultiDoc()
{
    DoTest(it_text, 10, 1, 10, OPTION_FLAG_ALL);
    DoTest(it_text, 10, 1, 10, OPTION_FLAG_ALL, true);

    DoTest(it_text, 10, 1, 10, NO_PAYLOAD);
    DoTest(it_text, 10, 1, 10, NO_PAYLOAD, true);

    DoTest(it_text, 10, 1, 10, NO_POSITION_LIST);
    DoTest(it_text, 10, 1, 10, NO_POSITION_LIST, true);
}

void NormalIndexWriterTest::TestCaseForExpackIndexWithOneDoc()
{
    DoTest(it_expack, 1, 1, 1, EXPACK_OPTION_FLAG_ALL, false);
    DoTest(it_expack, 1, 1, 1, EXPACK_OPTION_FLAG_ALL, true);

    DoTest(it_expack, 1, 1, 1, EXPACK_NO_PAYLOAD, false);
    DoTest(it_expack, 1, 1, 1, EXPACK_NO_PAYLOAD, true);

    DoTest(it_expack, 1, 1, 1, EXPACK_NO_POSITION_LIST, false);
    DoTest(it_expack, 1, 1, 1, EXPACK_NO_POSITION_LIST, true);
}

void NormalIndexWriterTest::TestCaseForExpackIndexWithMultiDoc()
{
    DoTest(it_expack, 10, 8, 10, EXPACK_OPTION_FLAG_ALL, false);
    DoTest(it_expack, 10, 8, 10, EXPACK_OPTION_FLAG_ALL, true);

    DoTest(it_expack, 10, 8, 10, EXPACK_NO_PAYLOAD, false);
    DoTest(it_expack, 10, 8, 10, EXPACK_NO_PAYLOAD, true);

    DoTest(it_expack, 10, 8, 10, EXPACK_NO_POSITION_LIST, false);
    DoTest(it_expack, 10, 8, 10, EXPACK_NO_POSITION_LIST, true);
}

void NormalIndexWriterTest::TestCaseForPackIndexWithOneDoc()
{
    DoTest(it_pack, 1, 1, 1, OPTION_FLAG_ALL, false);
    DoTest(it_pack, 1, 1, 1, OPTION_FLAG_ALL, true);

    DoTest(it_pack, 1, 1, 1, NO_PAYLOAD, false);
    DoTest(it_pack, 1, 1, 1, NO_PAYLOAD, true);

    DoTest(it_pack, 1, 1, 1, NO_POSITION_LIST, false);
    DoTest(it_pack, 1, 1, 1, NO_POSITION_LIST, true);
}

void NormalIndexWriterTest::TestCaseForPackIndexWithMultiDoc()
{
    DoTest(it_pack, 10, 10, 10, OPTION_FLAG_ALL, false);
    DoTest(it_pack, 10, 10, 10, OPTION_FLAG_ALL, true);

    DoTest(it_pack, 10, 10, 10, NO_PAYLOAD, false);
    DoTest(it_pack, 10, 10, 10, NO_PAYLOAD, true);

    DoTest(it_pack, 10, 10, 10, NO_POSITION_LIST, false);
    DoTest(it_pack, 10, 10, 10, NO_POSITION_LIST, true);
}

void NormalIndexWriterTest::TestCaseForPackIndexWithoutSectionAttribute()
{
    mTokenType = tt_text;
    mOptionFlag = OPTION_FLAG_ALL;
    mTokens.clear();

    uint32_t fieldNum = 10;
    CreateIndexConfig(it_pack, fieldNum, ft_text, false);
    PackageIndexConfigPtr packIndexConfig = 
        dynamic_pointer_cast<PackageIndexConfig>(mIndexConfig);
    INDEXLIB_TEST_TRUE(packIndexConfig.get() != NULL);
    packIndexConfig->SetHasSectionAttributeFlag(false);
    CreateIndexWriter();
    BuildIndex(10, fieldNum, 10, false, tt_text);
        
    string indexName = mIndexConfig->GetIndexName();
    string sectionName = indexName + "_section";
    string sectionPath = FileSystemWrapper::JoinPath(mTestDir->GetPath(), sectionName);
    INDEXLIB_TEST_TRUE(!mFileSystem->IsExist(sectionPath));

    CheckData(false);
}

void NormalIndexWriterTest::TestCaseForCreateInMemReader()
{
    InnerTestCaseForCreateInMemReader(false, false);
}

void NormalIndexWriterTest::TestCaseForCreateInMemReaderWithSection()
{
    InnerTestCaseForCreateInMemReader(true, false);
}

void NormalIndexWriterTest::TestCaseForCreateInMemReaderWithBitmap()
{
    InnerTestCaseForCreateInMemReader(false, true);
}

void NormalIndexWriterTest::InnerTestCaseForCreateInMemReader(
        bool hasSection, bool hasHighVol)
{
    mOptionFlag = OPTION_FLAG_ALL;
    IndexType indexType = hasSection ? it_pack : it_text;
    CreateIndexConfig(indexType, 1, ft_text, hasHighVol);

    CreateIndexWriter();
    IndexSegmentReaderPtr reader = mIndexWriter->CreateInMemReader();
    ASSERT_TRUE(reader);
    if (hasSection)
    {
        ASSERT_TRUE(reader->GetSectionAttributeSegmentReader());
    }
    else
    {
        ASSERT_FALSE(reader->GetSectionAttributeSegmentReader());
    }
    if (hasHighVol)
    {
        ASSERT_TRUE(reader->GetBitmapSegmentReader());
    }
    else
    {
        ASSERT_FALSE(reader->GetBitmapSegmentReader());
    }

    IndexFormatOption indexFormatOption;
    indexFormatOption.Init(mIndexConfig);

    // normal return false
    SegmentPosting segmentPosting(indexFormatOption.GetPostingFormatOption());
    ASSERT_FALSE(reader->GetSegmentPosting(0, 0, segmentPosting, NULL));
}


void NormalIndexWriterTest::SetVocabulary(IndexType indexType)
{
    mHighFreqVol.reset(new config::HighFrequencyVocabulary);
    switch(indexType)
    {
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64: 
        mHighFreqVol->Init("vol", it_number_int8, NUMBER_VOL_CONTENT);
        break;
    default:
        mHighFreqVol->Init("vol", it_text, VOL_CONTENT);        
    }
}

void NormalIndexWriterTest::DoTest(
        IndexType indexType, uint32_t docNum,
        uint32_t fieldNum, uint32_t secNum,
        optionflag_t optionFlag, bool hasVol,
        TokenType tokenType, FieldType fieldType)
{
    // reset test directory
    TearDown();
    SetUp();
    SetVocabulary(indexType);

    mTokenType = tokenType;
    mOptionFlag = optionFlag;

    // reset token set
    mTokens.clear();

    // create schema and config
    CreateIndexConfig(indexType, fieldNum, fieldType, hasVol);

    // create index writer
    CreateIndexWriter();

    // build index
    BuildIndex(docNum, fieldNum, secNum, hasVol, tokenType);

    // check
    CheckData(hasVol);
}

void NormalIndexWriterTest::CheckData(bool hasVol)
{
    CheckNormalTokens();
    if (hasVol)
    {
        // check high-freq token
        CheckHighFreqToken();
    }
}


void NormalIndexWriterTest::CheckNormalTokens()
{
    DictionaryReader *reader = DictionaryCreator::CreateReader(mIndexConfig);
    reader->Open(mTestDir->GetDirectory(mIndexConfig->GetIndexName(), true), 
                 DICTIONARY_FILE_NAME);

    set<uint64_t>::const_iterator it;
    for (it = mTokens.begin(); it != mTokens.end(); it++)
    {
        INDEXLIB_TEST_TRUE(CheckKey(reader, *it));
    }
    delete reader;
}

void NormalIndexWriterTest::CheckHighFreqToken()
{
    DictionaryReader *reader = new TieredDictionaryReaderTyped<uint64_t>();
    reader->Open(mTestDir->GetDirectory(mIndexConfig->GetIndexName(), true), 
                 BITMAP_DICTIONARY_FILE_NAME);

    string highFreqToken = HIGH_FREQ_TOKEN;
    if (mTokenType == tt_number)
    {
        highFreqToken = NUMBER_HIGH_FREQ_TOKEN;
    }

    KeyHasherPtr hasher(KeyHasherFactory::Create(mIndexConfig->GetIndexType()));
    dictkey_t key;
    hasher->GetHashKey(highFreqToken.c_str(), key);

    INDEXLIB_TEST_TRUE(CheckKey(reader, key));
    delete reader;
}

void NormalIndexWriterTest::BuildIndex(
        uint32_t docNum, uint32_t fieldNum,
        uint32_t secNum, bool hasVol, TokenType tokenType)
{
    docid_t id = 0;
    Answer answer;
    Pool pool;
    for (; (uint32_t)id < docNum; id++)
    {
        IndexDocument::FieldVector fieldVec;
        IndexDocumentMaker::HashKeyToStrMap hashKeyToStrMap;
        for (uint32_t i = 0; i < fieldNum; i++)
        {
            vector<Section*> sectionVec;
            IndexDocumentMaker::CreateSections(secNum, sectionVec,
					       id, &answer, hashKeyToStrMap, tokenType, NULL, &pool);
            AddToken(sectionVec);
            Field* field = IndexDocumentMaker::CreateField(sectionVec, &pool);
            int32_t fieldIdxInPack = mIndexConfig->GetFieldIdxInPack(i);
            INDEXLIB_TEST_TRUE(fieldIdxInPack >= 0);
            field->SetFieldId((fieldid_t)i);
            mIndexWriter->AddField(field);
            fieldVec.push_back(field);
        }
	
	vector<IndexDocumentPtr> indexDocs;
        IndexDocumentPtr doc =
            IndexDocumentMaker::MakeIndexDocument(&pool,
		fieldVec, id, &answer, hashKeyToStrMap, tokenType);
	
	indexDocs.push_back(doc);
	if (mSchema)
	{
	    IndexDocumentMaker::RewriteSectionAttributeInIndexDocuments(
		indexDocs, mSchema);
	}
	
        // update fieldmap for expack index
        if (mIndexConfig->GetIndexType() == it_expack)
        {
            PackageIndexConfigPtr packIndexConf = 
                dynamic_pointer_cast<PackageIndexConfig>(mIndexConfig);
            IndexDocumentMaker::UpdateFieldMapInAnswer(
                    fieldVec, id, packIndexConf, &answer);
            // check fieldmap and firstocc
            CheckFieldMap(id, hashKeyToStrMap, &answer);
        }

        mIndexWriter->EndDocument(*doc);
        uint32_t expectedTermCount = hasVol ? (mTokens.size() + 1) : (mTokens.size());
        CheckDistinctTermCount(expectedTermCount);

        if (hasVol)
        {
            CheckBitmap(&answer);
        }
        CheckPostingWriter(hashKeyToStrMap, &answer);
    }
        
    mIndexWriter->EndSegment();
    mDfSum = mIndexWriter->GetNormalTermDfSum();
    mIndexWriter->Dump(mTestDir, &mSimplePool);
    mFileSystem->Sync(true);
}

void NormalIndexWriterTest::AddToken(const vector<Section *> &sectionVec)
{
    for (size_t i = 0; i < sectionVec.size(); i++)
    {
        Section *sec = sectionVec[i];
        for (size_t j = 0; j < sec->GetTokenCount(); j++)
        {
            Token *token = sec->GetToken(j);
            mTokens.insert(token->GetHashKey());
        }
    }
}

void NormalIndexWriterTest::CreateIndexConfig(
        IndexType indexType, uint32_t fieldNum, 
        FieldType fieldType, bool hasVol)
{
    switch (indexType) 
    {
    case it_pack:
        CreatePackIndexConfig(it_pack, "PackIndex", 
                              fieldNum, hasVol);
        break;
    case it_expack:
        CreatePackIndexConfig(it_expack, "ExpackIndex", 
                              fieldNum, hasVol);
        break;
    case it_text:
        CreateSingleFieldIndexConfig(it_text, "TextIndex", 
                fieldType, hasVol);
        break;
    case it_string:
        CreateSingleFieldIndexConfig(it_string, "StringIndex", 
                fieldType, hasVol);
        break;
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64: 
        CreateSingleFieldIndexConfig(it_number, "NumberIndex",
                fieldType, hasVol);
        break;
    default:
        INDEXLIB_THROW(misc::UnSupportedException, "unsupported index type: %d",
                       indexType);
        break;
    }
}

void NormalIndexWriterTest::CreateIndexWriter()
{
    mIndexWriter.reset(new NormalIndexWriter(HASHMAP_INIT_SIZE, mOptions));
    mIndexWriter->Init(mIndexConfig, NULL);
}

void NormalIndexWriterTest::CreateSingleFieldIndexConfig(
        IndexType indexType, const string &indexName,
        FieldType fieldType, bool hasVol)
{
    SingleFieldIndexConfigPtr indexConfig;
    IndexDocumentMaker::CreateSingleFieldIndexConfig(
            indexConfig, indexName, indexType, fieldType, mOptionFlag);
    if (hasVol)
    {
        DictionaryConfigPtr dictConfig(
                new DictionaryConfig(VOL_NAME, (fieldType == ft_text)?VOL_CONTENT:NUMBER_VOL_CONTENT));
        indexConfig->SetDictConfig(dictConfig);
        indexConfig->SetHighFreqencyTermPostingType(hp_both);
    }

    mIndexConfig = std::tr1::static_pointer_cast<config::IndexConfig>(indexConfig);
}

void NormalIndexWriterTest::CreatePackIndexConfig(
        IndexType indexType, const string &indexName,
        uint32_t fieldNum, bool hasVol)
{
    PackageIndexConfigPtr indexConf;
    IndexDocumentMaker::CreatePackageIndexConfig(
	indexConf, mSchema, fieldNum, indexType, indexName);

    if (hasVol)
    {
        DictionaryConfigPtr dictConfig(
                new DictionaryConfig(VOL_NAME, VOL_CONTENT));
        indexConf->SetDictConfig(dictConfig);
        indexConf->SetHighFreqencyTermPostingType(hp_both);
    }

    mIndexConfig = std::tr1::static_pointer_cast<IndexConfig>(indexConf);
    mIndexConfig->SetOptionFlag(mOptionFlag);
}

bool NormalIndexWriterTest::CheckKey(
        DictionaryReader *reader,
        dictkey_t key)
{
    dictvalue_t value;
    bool ret = reader->Lookup(key, value);
    return ret;
}

void NormalIndexWriterTest::CheckFieldMap(docid_t docId, const HashKeyToStrMap &hashKeyToStrMap, Answer *answer)
{
    NormalIndexWriterPtr writer = dynamic_pointer_cast<NormalIndexWriter>(mIndexWriter);
    set<dictkey_t>::const_iterator it;
    for (it = mTokens.begin(); it != mTokens.end(); it++)
    {
        dictkey_t key = *it;
        const PostingWriterImpl *postingListWriter = 
            dynamic_cast<const PostingWriterImpl*>(writer->GetPostingListWriter(key));
        INDEXLIB_TEST_TRUE(postingListWriter != NULL);

        fieldmap_t fieldMap = postingListWriter->GetFieldMap();
        const string &token = hashKeyToStrMap.find(key)->second;
        const KeyAnswerInDoc &inDocAnswer = answer->postingAnswerMap[token].inDocAnswers[docId];
        INDEXLIB_TEST_EQUAL(inDocAnswer.fieldMap, fieldMap);
    }
}

void NormalIndexWriterTest::CheckPostingWriter(const HashKeyToStrMap &hashKeyToStrMap, Answer *answer)
{
    NormalIndexWriterPtr writer = dynamic_pointer_cast<NormalIndexWriter>(mIndexWriter);
    set<uint64_t>::const_iterator it;

    for (it = mTokens.begin(); it != mTokens.end(); it++)
    {
        dictkey_t key = *it;
        const PostingWriter *postingListWriter =
            writer->GetPostingListWriter(key);
        INDEXLIB_TEST_TRUE(postingListWriter != NULL);

        df_t df = postingListWriter->GetDF();
        tf_t ttf = postingListWriter->GetTotalTF();
        const string &token = hashKeyToStrMap.find(key)->second;
        const KeyAnswer &keyAnswer = answer->postingAnswerMap[token];

        if (mOptionFlag & of_term_payload)
        {
            termpayload_t termPayload = postingListWriter->GetTermPayload();
            INDEXLIB_TEST_EQUAL(keyAnswer.termPayload, termPayload);
        }
        INDEXLIB_TEST_EQUAL((tf_t)keyAnswer.docIdList.size(), df);
        INDEXLIB_TEST_EQUAL(keyAnswer.totalTF, ttf);
    }
}

void NormalIndexWriterTest::CheckBitmap(Answer *answer)
{
    string token = HIGH_FREQ_TOKEN;
    if (mTokenType == tt_number)
    {
        token = NUMBER_HIGH_FREQ_TOKEN;
    }

    NormalIndexWriterPtr writer = dynamic_pointer_cast<NormalIndexWriter>(mIndexWriter);

    KeyHasherPtr hasher(KeyHasherFactory::Create(mIndexConfig->GetIndexType()));
    dictkey_t key;
    hasher->GetHashKey(token.c_str(), key);

    BitmapPostingWriter *bitmapWriter = writer->GetBitmapPostingWriter(key);
    INDEXLIB_TEST_TRUE(bitmapWriter != NULL);

    df_t df = bitmapWriter->GetDF();
    tf_t ttf = bitmapWriter->GetTotalTF();
    termpayload_t termPayload = bitmapWriter->GetTermPayload();

    const KeyAnswer &keyAnswer = answer->postingAnswerMap[token];
    INDEXLIB_TEST_EQUAL(keyAnswer.termPayload, termPayload);
    INDEXLIB_TEST_EQUAL((tf_t)keyAnswer.docIdList.size(), df);
    INDEXLIB_TEST_EQUAL(keyAnswer.totalTF, ttf);
}

void NormalIndexWriterTest::TestCaseForCreateBitmapIndexWriter()
{
    mOptionFlag = OPTION_FLAG_ALL;
    CreateIndexConfig(it_text, 1, ft_text, true);

    IndexPartitionOptions realtimeOption;
    realtimeOption.SetIsOnline(true);
    NormalIndexWriter realtimeWriter(HASHMAP_INIT_SIZE, realtimeOption);
    realtimeWriter.Init(mIndexConfig, NULL);
    realtimeWriter.mBitmapIndexWriter->AddToken(0, 0);

    IndexPartitionOptions normalOption;
    normalOption.SetIsOnline(false);
    NormalIndexWriter normalWriter(HASHMAP_INIT_SIZE, normalOption);
    normalWriter.Init(mIndexConfig, NULL);
    normalWriter.mBitmapIndexWriter->AddToken(0, 0);
    
    // BitmapPostingWriter::INIT_BITMAP_ITEM_NUM
    size_t initBitmapItemSize = 128 * 1024 / 8;
    size_t poolDiffSize = realtimeWriter.mByteSlicePool->getAllocatedSize()
                          - normalWriter.mByteSlicePool->getAllocatedSize();
    ASSERT_TRUE(poolDiffSize >= initBitmapItemSize);
}

void NormalIndexWriterTest::TestCaseForDumpIndexFormatOffline()
{
    // offline index does not dump index format
    mOptions.SetIsOnline(false);
    DoTest(it_pack, 1, 1, 1, NO_PAYLOAD);
    file_system::DirectoryPtr directory = mTestDir->GetDirectory(
            mIndexConfig->GetIndexName(), true);
    ASSERT_TRUE(directory->IsExist(INDEX_FORMAT_OPTION_FILE_NAME));
}

void NormalIndexWriterTest::TestCaseForDumpIndexFormatOnline()
{
    // realtime index dump index format
    mOptions.SetIsOnline(true);
    DoTest(it_pack, 1, 1, 1, NO_PAYLOAD);
    file_system::DirectoryPtr directory = mTestDir->GetDirectory(
            mIndexConfig->GetIndexName(), true);
    ASSERT_TRUE(directory->IsExist(INDEX_FORMAT_OPTION_FILE_NAME));
}

void NormalIndexWriterTest::ResetCaseDirectory()
{
    string caseDir = GET_TEST_DATA_PATH();
    FileSystemWrapper::DeleteIfExist(caseDir);
    FileSystemWrapper::MkDir(caseDir);
    mFileSystem.reset(new IndexlibFileSystemImpl(caseDir));
    FileSystemOptions fileSystemOptions;
    fileSystemOptions.needFlush = true;
    util::MemoryQuotaControllerPtr mqc(
            new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    util::PartitionMemoryQuotaControllerPtr quotaController(
            new util::PartitionMemoryQuotaController(mqc));
    fileSystemOptions.memoryQuotaController = quotaController;
    mFileSystem->Init(fileSystemOptions);
    file_system::DirectoryPtr directory(new InMemDirectory(caseDir, mFileSystem));
    mTestDir = directory->MakeDirectory("segment/index/");    
    mFileSystem->Sync(true);
}

void NormalIndexWriterTest::TestCaseForEstimateDumpMemoryUse()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);
    
    string field = "string1:string;";
    string index = "index1:string:string1;";

    IndexPartitionSchemaPtr schema = 
        SchemaMaker::MakeSchema(field, index, "", "");

    string docString;
    size_t termCount = 10000;
    size_t dfEachTerm = MAX_UNCOMPRESSED_DOC_LIST_SIZE + 1;
    char buffer[100];
    
    for (int termCnt = 0; termCnt < (int)termCount; ++termCnt)
    {
        for (int i = 0; i < (int)dfEachTerm; ++i)
        {
            snprintf(buffer, 1000, "cmd=add,string1=term_%d;", termCnt);
            docString += string(buffer);
        }
    }
    vector<NormalDocumentPtr> docs = 
        DocumentCreator::CreateDocuments(schema, docString);

    BuildResourceMetrics buildMetrics;
    buildMetrics.Init();
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    SegmentMetricsPtr metrics(new SegmentMetrics);
    metrics->SetDistinctTermCount(indexConfig->GetIndexName(), 20000);
    IndexWriterPtr indexWriter(IndexWriterFactory::CreateIndexWriter(
                    indexConfig, metrics, options, &buildMetrics,
                    plugin::PluginManagerPtr()));

    NormalIndexWriterPtr normalIndexWriter =
        dynamic_pointer_cast<NormalIndexWriter>(indexWriter);

    assert(normalIndexWriter);
    for (size_t i = 0; i < docs.size(); ++i)
    {
        const IndexDocumentPtr& indexDoc = docs[i]->GetIndexDocument();
        assert(indexDoc);
        IndexDocument::FieldVector::const_iterator iter = indexDoc->GetFieldBegin();
        IndexDocument::FieldVector::const_iterator iterEnd = indexDoc->GetFieldEnd();

        for (; iter != iterEnd; ++iter)
        {
            const Field* field = *iter;
            if (!field)
            {
                continue;
            }
            normalIndexWriter->AddField(field);
            
        }
        normalIndexWriter->EndDocument(*indexDoc);
        EXPECT_EQ(int64_t(buildMetrics.GetValue(util::BMT_CURRENT_MEMORY_USE) * 0.2),
                  buildMetrics.GetValue(util::BMT_DUMP_FILE_SIZE));
    }
    int64_t estimateDumpExpandMemUse = buildMetrics.GetValue(
            util::BMT_DUMP_EXPAND_MEMORY_SIZE);

    ASSERT_EQ(termCount * NormalIndexWriter::UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR,
              estimateDumpExpandMemUse - normalIndexWriter->mBufferPool->getUsedBytes());
}

void NormalIndexWriterTest::TestCaseForDumpPerfLongCaseTest()
{
    stringstream ss;
    uint32_t docCount = 50000;
    uint32_t tokenCount = 1000;
    uint32_t tokenTotalCount = 5000;

    for (uint32_t i = 0; i < docCount; i++)
    {
        ss << "cmd=add,s=";
        for (uint32_t j = 0; j < tokenCount; j++)
        {
            uint32_t value = rand() % tokenTotalCount;
            ss << value << " ";
        }
        ss << ";";
    }

    string field = "s:string";
    string index = "index:string:s";
    IndexPartitionSchemaPtr schema = 
        SchemaMaker::MakeSchema(field, index, "", "");

    vector<NormalDocumentPtr> docs = 
        DocumentCreator::CreateDocuments(schema, ss.str());
    IndexConfigPtr indexConf = 
        schema->GetIndexSchema()->GetIndexConfig("index");

    IndexPartitionOptions options;
    NormalIndexWriter indexWriter(1024, options);
    indexWriter.Init(indexConf, NULL);
    for (size_t i = 0; i < docs.size(); i++)
    {
        DocumentPtr doc = docs[i];
        docs[i]->SetDocId((docid_t)i);
        const IndexDocumentPtr& indexDocument = docs[i]->GetIndexDocument();
        Field* field = indexDocument->GetField(0);
        indexWriter.AddField(field);
        indexWriter.EndDocument(*indexDocument);
    }

    indexWriter.EndSegment();
    
    file_system::IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    string dirPath = GET_TEST_DATA_PATH() + "dump_path";
    fileSystem->MountInMemStorage(dirPath);
    file_system::DirectoryPtr dir(
            new file_system::InMemDirectory(dirPath, fileSystem));
    
    cout << "*******************" << endl;
    int64_t beginTime = TimeUtility::currentTime();
    indexWriter.Dump(dir, &mSimplePool);
    int64_t interval = TimeUtility::currentTime() - beginTime;
    cout << "***** time interval : " << interval / 1000 << "ms, " << endl;
}

IE_NAMESPACE_END(index);


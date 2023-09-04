#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/PostingMergerImpl.h"
#include "indexlib/index/inverted_index/builtin_index/expack/OnDiskExpackIndexIterator.h"
#include "indexlib/index/inverted_index/builtin_index/expack/test/ExpackPostingMaker.h"
#include "indexlib/index/inverted_index/builtin_index/pack/test/PackPositionMaker.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/index/inverted_index/test/PostingMaker.h"
#include "indexlib/index/test/FakeDocMapper.h"
#include "indexlib/index/test/IndexTestUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using indexlibv2::framework::FakeSegment;
using indexlibv2::framework::Segment;
using indexlibv2::framework::SegmentInfo;
using indexlibv2::framework::SegmentMeta;
using indexlibv2::index::DocMapper;
using indexlibv2::index::FakeDocMapper;
using indexlibv2::index::IIndexMerger;
} // namespace

#define INDEX_REL_PATH "index/phrase"

class ExpackPostingMergerTest : public TESTBASE
{
public:
public:
    typedef PostingMaker::DocMap DocMap;
    typedef PostingMaker::DocKey DocKey;
    typedef PostingMaker::DocRecordVector DocRecordVector;
    typedef PostingMaker::DocRecord DocRecord;
    typedef PostingMaker::PostingList PostingList;
    typedef std::vector<PostingList> PostingListVec;

    typedef ExpackPostingMaker::ExpackDocMapWithFieldMap ExpackDocMapWithFieldMap;
    typedef ExpackPostingMaker::ExpackDocRecordVector ExpackDocRecordVector;
    typedef ExpackPostingMaker::ExpackDocRecord ExpackDocRecord;
    typedef ExpackPostingMaker::ExpackPostingList ExpackPostingList;
    typedef std::vector<ExpackPostingList> ExpackPostingListVec;

    void setUp() override
    {
        _dir = GET_TEMP_DATA_PATH();
        file_system::FslibWrapper::DeleteDirE(_dir, file_system::DeleteOption::NoFence(true));
        file_system::FslibWrapper::MkDirE(_dir);

        file_system::FileSystemOptions fileSystemOptions;
        fileSystemOptions.needFlush = true;
        std::shared_ptr<util::MemoryQuotaController> mqc(
            new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        std::shared_ptr<util::PartitionMemoryQuotaController> quotaController(
            new util::PartitionMemoryQuotaController(mqc));
        fileSystemOptions.memoryQuotaController = quotaController;
        _fileSystem = file_system::FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                             /*rootPath=*/_dir, fileSystemOptions,
                                                             std::shared_ptr<util::MetricProvider>(),
                                                             /*isOverride=*/false)
                          .GetOrThrow();

        std::shared_ptr<file_system::Directory> directory =
            file_system::IDirectory::ToLegacyDirectory(std::make_shared<file_system::MemDirectory>("", _fileSystem));
        _testDir = directory->MakeDirectory("test");
        _fileSystem->Sync(true).GetOrThrow();

        _maxDocId = 0;
        _maxPos = 50;

        InitPool();
        _docMapper.reset(new FakeDocMapper("docMapper", DocMapper::GetDocMapperType()));
        _indexConfig.reset(new indexlibv2::config::PackageIndexConfig("phrase", it_pack));
    }
    void tearDown() override {}

private:
    void TestMultiMerge(optionflag_t optionFlag)
    {
        std::vector<uint32_t> docNums;
        for (uint32_t i = 0; i < 5; i++) {
            docNums.push_back(i % 300 + 2);
        }

        InternalTestMerge(docNums, optionFlag, false);
        InternalTestMerge(docNums, optionFlag, true);
    }

    void InternalTestOnePosting(optionflag_t optionFlag)
    {
        std::vector<uint32_t> docNums;
        docNums.push_back(200);
        InternalTestMerge(docNums, optionFlag, false);
        InternalTestMerge(docNums, optionFlag, true);
    }

    void InternalTestSimpleMerge(optionflag_t optionFlag)
    {
        std::vector<uint32_t> docNums;
        docNums.push_back(20);
        docNums.push_back(40);
        InternalTestMerge(docNums, optionFlag, true);
        InternalTestMerge(docNums, optionFlag, false);
    }

    void InternalTestMerge(const std::vector<uint32_t>& docNums, optionflag_t optionFlag, bool isNullTerm)
    {
        index::DictKeyInfo key = index::DictKeyInfo(0);
        if (isNullTerm) {
            key = index::DictKeyInfo::NULL_TERM;
        }

        // make data
        // FOR_EACH(i, IndexTestUtil::deleteFuncs)
        // {
        tearDown();
        setUp();

        _indexConfig->SetOptionFlag(optionFlag);
        IndexFormatOption indexFormatOption;
        indexFormatOption.Init(_indexConfig);

        PostingListVec postLists;
        ExpackPostingListVec expackPostLists;
        std::vector<std::shared_ptr<SegmentInfo>> segInfos;
        MakeData(optionFlag, docNums, postLists, expackPostLists, segInfos, isNullTerm);

        std::vector<IIndexMerger::SourceSegment> srcSegments;
        SegmentTermInfos segTermInfos;
        CreateSegmentTermInfos(segInfos, postLists, segTermInfos, srcSegments, optionFlag);

        std::shared_ptr<PostingMergerImpl> merger = CreateMerger(optionFlag);

        _docMapper.reset(new FakeDocMapper("docMapper", DocMapper::GetDocMapperType()));
        std::vector<indexlibv2::index::SrcSegmentInfo> infos;
        for (size_t i = 0; i < srcSegments.size(); i++) {
            auto& segment = srcSegments[i].segment;
            std::set<docid_t> deletedDocs = GenDeletedDocs(segment->GetSegmentInfo()->docCount, i);
            infos.emplace_back(indexlibv2::index::SrcSegmentInfo(
                segment->GetSegmentId(), segment->GetSegmentInfo()->docCount, srcSegments[i].baseDocid, deletedDocs));
        }
        _docMapper->Init(infos, 100, 1, true);

        merger->Merge(segTermInfos, _docMapper);

        std::shared_ptr<file_system::Directory> directory = _testDir;
        std::stringstream ss;
        ss << "merged.post." << 0;
        std::string mergedName = ss.str();

        IndexOutputSegmentResources outputSegmentResources;
        auto resource = std::make_shared<IndexOutputSegmentResource>();
        util::SimplePool simplePool;
        std::shared_ptr<file_system::FileWriter> mergedSegFile = directory->CreateFileWriter(mergedName);
        resource->_normalIndexDataWriter.reset(new IndexDataWriter());
        resource->_normalIndexDataWriter->postingWriter = mergedSegFile;
        resource->_normalIndexDataWriter->dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(&simplePool));
        resource->_normalIndexDataWriter->dictWriter->Open(directory, mergedName + "_dict");
        outputSegmentResources.push_back(resource);
        merger->Dump(key, outputSegmentResources);
        resource->DestroyIndexDataWriters();

        // make answer
        PostingList answerList;
        ExpackPostingList expackAnswerList;
        answerList = PostingMaker::SortByWeightMergePostingLists(postLists, *_docMapper);
        expackAnswerList = ExpackPostingMaker::SortByWeightMergePostingLists(expackPostLists, *_docMapper);
        uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(merger->GetDocFreq(), merger->GetTotalTF(),
                                                                      PFOR_DELTA_COMPRESS_MODE);
        CheckPosting(_dir + "/test/" + mergedName, answerList, expackAnswerList, compressMode, optionFlag);

        for (uint32_t j = 0; j < segTermInfos.size(); j++) {
            delete segTermInfos[j];
        }
    }

    void MakeData(optionflag_t optionFlag, const std::vector<uint32_t>& docNums, PostingListVec& postingLists,
                  ExpackPostingListVec& expackPostingLists, std::vector<std::shared_ptr<SegmentInfo>>& segInfos,
                  bool isNullTerm)
    {
        docid_t maxDocId = 0;
        postingLists.resize(docNums.size());
        expackPostingLists.resize(docNums.size());
        for (uint32_t i = 0; i < docNums.size(); i++) {
            std::stringstream ss;
            ss << "segment_" << i << "_level_0/" << INDEX_REL_PATH;
            std::string path = ss.str();
            auto segDir = _testDir->MakeDirectory(path);
            ASSERT_NE(segDir, nullptr);

            MakeOneSegment(segDir, optionFlag, docNums[i], postingLists[i], expackPostingLists[i], maxDocId,
                           isNullTerm);

            postingLists[i].first = _maxDocId;
            expackPostingLists[i].first = _maxDocId;
            _maxDocId += (maxDocId + 1);

            auto segInfo = std::make_shared<SegmentInfo>();
            segInfo->docCount = (maxDocId + 1);
            segInfos.push_back(segInfo);
        }
        // mSecAttrReader = CreateFakeSectionAttribute();
    }

    void CreateSegmentTermInfos(const std::vector<std::shared_ptr<SegmentInfo>>& segInfos,
                                const PostingListVec& postLists, SegmentTermInfos& segTermInfos,
                                std::vector<IIndexMerger::SourceSegment>& srcSegments, optionflag_t optionFlag)
    {
        for (uint32_t i = 0; i < postLists.size(); i++) {
            std::stringstream ss;
            ss << "segment_" << i << "_level_0/" << INDEX_REL_PATH;

            std::shared_ptr<file_system::Directory> indexDirectory = _testDir->GetDirectory(ss.str(), true);
            OnDiskPackIndexIterator* onDiskIter = new OnDiskPackIndexIterator(_indexConfig, indexDirectory, optionFlag);
            onDiskIter->Init();

            std::shared_ptr<IndexIterator> indexIter(onDiskIter);

            SegmentTermInfo* segTermInfo =
                new SegmentTermInfo(it_unknown, (segmentid_t)i, postLists[i].first, indexIter, /*patchIter=*/nullptr);
            segTermInfos.push_back(segTermInfo);

            IIndexMerger::SourceSegment srcSegment;
            SegmentMeta segMeta;
            srcSegment.segment = std::make_shared<FakeSegment>(Segment::SegmentStatus::ST_BUILT);
            segMeta.segmentId = (segmentid_t)i;
            srcSegment.baseDocid = postLists[i].first;
            segMeta.segmentInfo = segInfos[i];
            srcSegment.segment->TEST_SetSegmentMeta(segMeta);

            srcSegments.push_back(srcSegment);
            ASSERT_TRUE(segTermInfo->Next());
        }
    }

    uint32_t ReadVUInt32(const std::shared_ptr<file_system::MemFileNode>& fileNode, size_t& offset)
    {
        uint8_t byte;
        fileNode->Read((void*)&byte, sizeof(byte), offset++, file_system::ReadOption()).GetOrThrow();
        uint32_t value = byte & 0x7F;
        int shift = 7;

        while (byte & 0x80) {
            fileNode->Read((void*)&byte, sizeof(byte), offset++, file_system::ReadOption()).GetOrThrow();
            value |= ((byte & 0x7F) << shift);
            shift += 7;
        }
        return value;
    }

    void CheckPosting(const std::string& mergedPath, const PostingList& answerList,
                      const ExpackPostingList& expackAnswerList, uint8_t compressMode, optionflag_t optionFlag)
    {
        file_system::MemFileNodePtr fileNode(file_system::MemFileNodeCreator::TEST_Create());
        ASSERT_EQ(file_system::FSEC_OK, fileNode->Open("LOGICAL_PATH", mergedPath, file_system::FSOT_MEM, -1));
        file_system::NormalFileReader fileReader(fileNode);
        ASSERT_EQ(file_system::FSEC_OK, fileReader.Open());
        if (answerList.second.size() == 0) {
            ASSERT_EQ(0, fileReader.GetLength());
            return;
        }
        uint32_t postingLen = fileReader.ReadVUInt32().GetOrThrow();
        util::ByteSliceList* sliceList = fileReader.ReadToByteSliceList(
            postingLen, VByteCompressor::GetVInt32Length(postingLen), file_system::ReadOption());
        ASSERT_TRUE(sliceList);

        PostingFormatOption postingFormatOption(optionFlag);
        SegmentPosting segPosting(postingFormatOption);
        segPosting.Init(compressMode, util::ByteSliceListPtr(sliceList), 0, 0);

        std::shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
        segPostings->push_back(segPosting);
        std::shared_ptr<BufferedPostingIterator> iter(new BufferedPostingIterator(postingFormatOption, NULL, nullptr));
        iter->Init(segPostings, /*_secAttrReader*/ nullptr, 2000);

        // begin check
        DocRecordVector docRecordVec = answerList.second;
        ExpackDocRecordVector expackDocRecordVec = expackAnswerList.second;

        uint32_t recordCursor = 0;
        if (docRecordVec.size() == 0) {
            return;
        }

        DocRecord currDocRecord = docRecordVec[0];
        ExpackDocRecord currExpackDocRecord = expackDocRecordVec[0];

        docid_t docId = 0;
        docpayload_t docPayload = 0;
        DocMap::const_iterator inRecordCursor = currDocRecord.begin();
        ExpackDocMapWithFieldMap::const_iterator inExpackRecordCursor = currExpackDocRecord.begin();

        while ((docId = iter->SeekDoc(docId)) != INVALID_DOCID) {
            if (inRecordCursor == currDocRecord.end()) {
                recordCursor++;
                currDocRecord = docRecordVec[recordCursor];
                currExpackDocRecord = expackDocRecordVec[recordCursor];
                inRecordCursor = currDocRecord.begin();
                inExpackRecordCursor = currExpackDocRecord.begin();
            }
            DocKey docKey = inRecordCursor->first;

            ASSERT_EQ(docKey.first, docId);

            if (optionFlag & of_doc_payload) {
                docPayload = iter->GetDocPayload();
                ASSERT_EQ(docKey.second, docPayload);
            }

            TermMatchData termMatchData;
            iter->Unpack(termMatchData);

            ASSERT_EQ(inExpackRecordCursor->second, termMatchData.GetFieldMap());
            ASSERT_FALSE(termMatchData.HasFirstOcc());

            PostingMaker::PosValue posValue = inRecordCursor->second;
            if (optionFlag & of_term_frequency) {
                ASSERT_EQ(posValue.size(), (size_t)termMatchData.GetTermFreq());
            } else {
                ASSERT_EQ(1, termMatchData.GetTermFreq());
            }

            if (optionFlag & of_position_list) {
                uint32_t posCursor = 0;
                pos_t pos = 0;
                pospayload_t posPayload = 0;
                while ((pos = iter->SeekPosition(pos)) != INVALID_POSITION) {
                    ASSERT_EQ(posValue[posCursor].first, pos);
                    if (optionFlag & of_position_payload) {
                        posPayload = iter->GetPosPayload();
                        ASSERT_EQ(posValue[posCursor].second, posPayload);
                    }
                    posCursor++;
                }
            }
            inRecordCursor++;
            inExpackRecordCursor++;
        }
    }

    void InitPool()
    {
        _byteSlicePool.reset();
        _bufferPool.reset();
        _allocator.reset();

        autil::mem_pool::SimpleAllocator* allocator = new autil::mem_pool::SimpleAllocator;
        _allocator.reset(allocator);

        autil::mem_pool::Pool* byteSlicePool = new autil::mem_pool::Pool(allocator, DEFAULT_CHUNK_SIZE);
        _byteSlicePool.reset(byteSlicePool);

        autil::mem_pool::RecyclePool* bufferPool = new autil::mem_pool::RecyclePool(allocator, DEFAULT_CHUNK_SIZE);
        _bufferPool.reset(bufferPool);
    }

    std::set<docid_t> GenDeletedDocs(uint32_t docCount, size_t i)
    {
        std::set<docid_t> deletedDocs;
        auto delFunc = indexlibv2::index::IndexTestUtil::deleteFuncs[i % indexlibv2::index::IndexTestUtil::DM_MAX_MODE];
        for (size_t j = 0; j < docCount; ++j) {
            if (delFunc(j)) {
                deletedDocs.insert(j);
            }
        }
        return deletedDocs;
    }

    std::shared_ptr<PostingMergerImpl> CreateMerger(optionflag_t flag)
    {
        std::vector<std::shared_ptr<SegmentMeta>> targetSegments;
        auto segMeta = std::make_shared<SegmentMeta>();
        segMeta->segmentDir = _testDir;
        segMeta->segmentId = 100;
        targetSegments.push_back(segMeta);
        std::shared_ptr<PostingMergerImpl> merger;
        PostingFormatOption formatOption(flag);
        _writerResource.reset(
            new PostingWriterResource(&_simplePool, _byteSlicePool.get(), _bufferPool.get(), formatOption));

        merger.reset(new PostingMergerImpl(_writerResource.get(), targetSegments));
        return merger;
    }

    std::shared_ptr<PostingWriterImpl> CreateWriter(optionflag_t flag)
    {
        PostingFormatOption formatOption(flag);
        _writerResource.reset(
            new PostingWriterResource(&_simplePool, _byteSlicePool.get(), _bufferPool.get(), formatOption));
        return std::make_shared<PostingWriterImpl>(_writerResource.get());
    }

    void MakeOneSegment(const std::shared_ptr<file_system::Directory>& indexDir, optionflag_t optionFlag,
                        uint32_t docNum, PostingList& postList, ExpackPostingList& expackPostList, docid_t& maxDocId,
                        bool isNullTerm)
    {
        std::shared_ptr<PostingWriterImpl> writer = CreateWriter(optionFlag);
        uint32_t totalTF;
        postList = PostingMaker::MakePositionData(docNum, maxDocId, totalTF);
        ExpackPostingMaker::MakeOneSegmentWithWriter(*writer, postList, expackPostList);
        // PackPositionMaker::MakeOneSegmentWithWriter(*writer, postList, optionFlag);

        writer->EndSegment();

        TermMeta termMeta(writer->GetDF(), writer->GetTotalTF());
        if (optionFlag & of_term_payload) {
            termMeta.SetPayload(writer->GetTermPayload());
        }

        TermMetaDumper tmDumper;
        uint32_t postingLen = tmDumper.CalculateStoreSize(termMeta) + writer->GetDumpLength();
        AUTIL_LOG(TRACE1, "Writing Posting length: [%u] ", postingLen);

        std::shared_ptr<file_system::FileWriter> file = indexDir->CreateFileWriter(POSTING_FILE_NAME);
        file->Write((void*)(&postingLen), sizeof(uint32_t)).GetOrThrow();
        tmDumper.Dump(file, termMeta);
        writer->Dump(file);
        ASSERT_EQ(file_system::FSEC_OK, file->Close());
        writer.reset();

        util::SimplePool simplePool;
        TieredDictionaryWriter<dictkey_t> dictWriter(&simplePool);
        dictWriter.Open(indexDir, DICTIONARY_FILE_NAME);

        if (isNullTerm) {
            dictWriter.AddItem(index::DictKeyInfo::NULL_TERM, 0);
        } else {
            dictWriter.AddItem(index::DictKeyInfo(0), 0);
        }
        dictWriter.Close();
    }

    // std::string CreateFakeSectionString(docid_t maxDocId, pos_t maxPos)
    // {
    //     std::stringstream ss;
    //     for (docid_t i = 0; i <= maxDocId; i++) {
    //         pos_t secNum = maxPos / 0x7ff + 1;
    //         for (uint32_t j = 0; j < secNum; ++j) {
    //             ss << j << " 2047 1, ";
    //         }
    //         ss << ";";
    //     }
    //     return ss.str();
    // }

    // SectionAttributeReaderImplPtr CreateFakeSectionAttribute()
    // {
    //     std::string sectionStr = CreateFakeSectionString(mMaxDocId, mMaxPos);
    //     IndexPartitionSchemaPtr schema =
    //         SectionAttributeTestUtil::MakeSchemaWithPackageIndexConfig(mIndexConfig->GetIndexName(), 32);

    //     PackageIndexConfigPtr packIndexConfig = std::dynamic_pointer_cast<
    //         PackageIndexConfig>( schema->GetIndexSchema()->GetIndexConfig(mIndexConfig->GetIndexName()));

    //     SectionDataMaker::BuildOneSegmentSectionData(schema, mIndexConfig->GetIndexName(), sectionStr,
    //                                                  _rootDir, 0);

    //     SectionAttributeReaderImplPtr reader(new SectionAttributeReaderImpl);
    //     PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), 1);
    //     reader->Open(mIndexConfig, partitionData);
    //     return reader;
    // }

private:
    std::string _dir;
    std::shared_ptr<file_system::Directory> _testDir;
    std::shared_ptr<file_system::IFileSystem> _fileSystem;
    uint32_t _maxDocId;
    pos_t _maxPos;

    std::shared_ptr<FakeDocMapper> _docMapper;
    // SectionAttributeReaderImplPtr mSecAttrReader;
    std::shared_ptr<indexlibv2::config::PackageIndexConfig> _indexConfig;

    std::shared_ptr<autil::mem_pool::SimpleAllocator> _allocator;
    std::shared_ptr<autil::mem_pool::Pool> _byteSlicePool;
    std::shared_ptr<autil::mem_pool::RecyclePool> _bufferPool;
    util::SimplePool _simplePool;
    std::shared_ptr<PostingWriterResource> _writerResource;
    static const size_t DEFAULT_CHUNK_SIZE = 100 * 1024;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, ExpackPostingMergerTest);

TEST_F(ExpackPostingMergerTest, testMergeOnePosting) { InternalTestOnePosting(EXPACK_OPTION_FLAG_ALL); }

TEST_F(ExpackPostingMergerTest, testMergeOnePostingWithoutTf) { InternalTestOnePosting(EXPACK_NO_TERM_FREQUENCY); }

TEST_F(ExpackPostingMergerTest, testMergeOnePostingWithoutPosition) { InternalTestOnePosting(EXPACK_NO_POSITION_LIST); }

TEST_F(ExpackPostingMergerTest, testMergeOnePostingWithTfBitmap) { InternalTestOnePosting(EXPACK_OPTION_FLAG_ALL); }

TEST_F(ExpackPostingMergerTest, testMergeOnePostingWithTfBitmapWithoutPayload)
{
    InternalTestOnePosting(EXPACK_NO_PAYLOAD);
}

TEST_F(ExpackPostingMergerTest, testMergeOnePostingWithTfBitmapWithoutPosition)
{
    InternalTestOnePosting(EXPACK_NO_POSITION_LIST);
}

TEST_F(ExpackPostingMergerTest, testSimpleMerge) { InternalTestSimpleMerge(EXPACK_OPTION_FLAG_ALL); }

TEST_F(ExpackPostingMergerTest, testSimpleMergeWithoutTf) { InternalTestSimpleMerge(EXPACK_NO_TERM_FREQUENCY); }

TEST_F(ExpackPostingMergerTest, testSimpleMergeWithoutPosition) { InternalTestOnePosting(EXPACK_NO_POSITION_LIST); }

TEST_F(ExpackPostingMergerTest, testSimpleMergeWithTfBitmap) { InternalTestSimpleMerge(EXPACK_OPTION_FLAG_ALL); }

TEST_F(ExpackPostingMergerTest, testSimpleMergeWithTfBitmapWithoutPayload)
{
    InternalTestSimpleMerge(EXPACK_NO_PAYLOAD);
}

TEST_F(ExpackPostingMergerTest, testSimpleMergeWithTfBitmapWithoutPosition)
{
    InternalTestSimpleMerge(EXPACK_NO_POSITION_LIST | of_tf_bitmap);
}

TEST_F(ExpackPostingMergerTest, testMultiMergeWithoutPosList) { TestMultiMerge(EXPACK_NO_POSITION_LIST); }

TEST_F(ExpackPostingMergerTest, testMultiMergeWithPosList) { TestMultiMerge(EXPACK_OPTION_FLAG_ALL); }

TEST_F(ExpackPostingMergerTest, testMultiMergeWithoutPayloads) { TestMultiMerge(EXPACK_NO_PAYLOAD); }

TEST_F(ExpackPostingMergerTest, testMultiMergeWithoutTf) { TestMultiMerge(EXPACK_NO_TERM_FREQUENCY); }

TEST_F(ExpackPostingMergerTest, testMultiMergeWithTfBitmap) { TestMultiMerge(EXPACK_OPTION_FLAG_ALL); }

TEST_F(ExpackPostingMergerTest, testMultiMergeWithTfBitmapWithoutPayload) { TestMultiMerge(EXPACK_NO_PAYLOAD); }

TEST_F(ExpackPostingMergerTest, testMultiMergeWithTfBitmapWithoutPosition) { TestMultiMerge(EXPACK_NO_POSITION_LIST); }

TEST_F(ExpackPostingMergerTest, testSpecificMerge)
{
    uint32_t sampleNumbers[] = {
        1,
        MAX_DOC_PER_RECORD / 3 + 1,
        MAX_DOC_PER_RECORD + 1,
        MAX_DOC_PER_RECORD * 2 + 1,
        MAX_DOC_PER_RECORD * 2 + MAX_DOC_PER_RECORD / 3 + 1,
    };

    std::vector<uint32_t> docNums(sampleNumbers, sampleNumbers + sizeof(sampleNumbers) / sizeof(uint32_t));
    InternalTestMerge(docNums, EXPACK_OPTION_FLAG_ALL, true);
    InternalTestMerge(docNums, EXPACK_OPTION_FLAG_ALL, false);
}

} // namespace indexlib::index

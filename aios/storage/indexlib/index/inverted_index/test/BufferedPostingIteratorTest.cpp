#include "indexlib/index/inverted_index/BufferedPostingIterator.h"

#include <memory>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/mock/FakeDiskSegment.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/pack/test/PackPostingMaker.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/section_attribute/test/SectionAttributeTestUtil.h"
#include "indexlib/index/inverted_index/test/InvertedTestHelper.h"
#include "indexlib/index/inverted_index/test/PostingMaker.h"
#include "indexlib/index/inverted_index/test/SectionDataMaker.h"
#include "indexlib/index/test/IndexTestUtil.h"
#include "unittest/unittest.h"
namespace indexlib::index {

class BufferedPostingIteratorTest : public TESTBASE
{
public:
    typedef PostingMaker::DocMap AnswerMap;

public:
    void setUp() override
    {
        autil::StringUtil::serializeUInt64(0x123456789ULL, _key);
        _dir = GET_TEMP_DATA_PATH();
        file_system::FileSystemOptions options;
        options.enableAsyncFlush = false;
        options.needFlush = true;
        options.useCache = true;
        options.useRootLink = false;
        options.isOffline = false;
        _fileSystem = file_system::FileSystemCreator::Create("ut", _dir, options).GetOrThrow();
        _directory = file_system::Directory::Get(_fileSystem);
        if (_fileSystem) {
            EXPECT_EQ(file_system::FSEC_OK, _fileSystem->MountDir(_dir, "", "", file_system::FSMT_READ_WRITE, true));
        }

        _schema = SectionAttributeTestUtil::MakeSchemaWithPackageIndexConfig("test_iterator", MAX_FIELD_COUNT, it_pack,
                                                                             0, true);

        _packageConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(
            _schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, "test_iterator"));
        assert(_packageConfig);
        _testOptionFlags.clear();

        // All
        _testOptionFlags.push_back(of_doc_payload | of_position_payload | of_position_list | of_term_frequency |
                                   of_tf_bitmap | of_fieldmap);

        // no doc_payload
        _testOptionFlags.push_back(of_position_payload | of_position_list | of_term_frequency | of_tf_bitmap |
                                   of_fieldmap);

        // no position_payload
        _testOptionFlags.push_back(of_doc_payload | of_position_list | of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no position_list, no position_payload
        _testOptionFlags.push_back(of_doc_payload | of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no term_frequency, no tf_bitmap
        _testOptionFlags.push_back(of_doc_payload | of_fieldmap);

        // no fieldmap
        _testOptionFlags.push_back(of_doc_payload | of_position_payload | of_position_list | of_term_frequency |
                                   of_tf_bitmap);

        // no tf_bitmap
        _testOptionFlags.push_back(of_doc_payload | of_position_payload | of_position_list | of_term_frequency |
                                   of_fieldmap);

        // no docpayload, no position_list
        _testOptionFlags.push_back(of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no docpayload, no expack_occ
        _testOptionFlags.push_back(of_position_payload | of_position_list | of_term_frequency | of_tf_bitmap |
                                   of_fieldmap);

        // no docpayload, no tf_bitmap
        _testOptionFlags.push_back(of_position_list | of_term_frequency | of_fieldmap);

        // no docpayload, no fieldmap
        _testOptionFlags.push_back(of_position_payload | of_position_list | of_term_frequency | of_tf_bitmap);

        // no term_frequency, no fieldmap
        _testOptionFlags.push_back(of_doc_payload | of_position_payload | of_position_list | of_term_frequency |
                                   of_tf_bitmap);

        // no expack_occ, not tf_bitmap,
        _testOptionFlags.push_back(of_doc_payload | of_position_payload | of_position_list | of_term_frequency |
                                   of_fieldmap);

        // no doc_payload, no position_list, no TF
        _testOptionFlags.push_back(of_fieldmap);

        // no doc_payload, no position_list, no occ
        _testOptionFlags.push_back(of_term_frequency | of_tf_bitmap | of_fieldmap);

        // no doc_payload, no position_list, no fieldmap
        _testOptionFlags.push_back(of_doc_payload | of_term_frequency);

        // no doc payload, no occ,  no tf_bitmap
        _testOptionFlags.push_back(of_position_payload | of_position_list | of_term_frequency | of_fieldmap);

        // no doc payload, no occ,  no fieldmap
        _testOptionFlags.push_back(of_position_payload | of_position_list | of_term_frequency | of_tf_bitmap);

        // no doc payload, no tf_bitmap, no fieldmap
        _testOptionFlags.push_back(of_position_payload | of_position_list | of_term_frequency);

        // no position_list, no tf, no occ
        _testOptionFlags.push_back(of_doc_payload | of_fieldmap);

        // no position_list, no tf, no fieldmap
        _testOptionFlags.push_back(of_doc_payload);

        // no doc_payload, no position_list, no tf, no occ
        _testOptionFlags.push_back(of_fieldmap);

        // no doc_payload, no position_list, no tf_bitmap, no fieldmap
        _testOptionFlags.push_back(of_term_frequency);
        _testOptionFlags.push_back(of_term_frequency | of_tf_bitmap);
    }

    void tearDown() override
    {
        _directory.reset();
        if (_fileSystem) {
            _fileSystem->Sync(true).GetOrThrow();
            _fileSystem.reset();
        }
    }

    void TestCaseForSeekDocInOneSegment()
    {
        for (size_t i = 0; i < _testOptionFlags.size(); i++) {
            TestSeekDocInOneSegment(_testOptionFlags[i]);
        }
    }

    void TestCaseForSeekDocInTwoSegments()
    {
        for (size_t i = 0; i < _testOptionFlags.size(); i++) {
            TestSeekDocInTwoSegments(_testOptionFlags[i]);
        }
    }

    void TestCaseForSeekDocInManySegmentsTest_1()
    {
        for (size_t i = 0; i < _testOptionFlags.size() / 4; i++) {
            TestSeekDocInMultiSegments(_testOptionFlags[i]);
        }
    }

    void TestCaseForSeekDocInManySegmentsTest_2()
    {
        for (size_t i = _testOptionFlags.size() / 4; i < _testOptionFlags.size() / 2; i++) {
            TestSeekDocInMultiSegments(_testOptionFlags[i]);
        }
    }

    void TestCaseForSeekDocInManySegmentsTest_3()
    {
        for (size_t i = _testOptionFlags.size() / 2; i < _testOptionFlags.size() / 4 * 3; i++) {
            TestSeekDocInMultiSegments(_testOptionFlags[i]);
        }
    }

    void TestCaseForSeekDocInManySegmentsTest_4()
    {
        for (size_t i = _testOptionFlags.size() / 4 * 3; i < _testOptionFlags.size(); i++) {
            TestSeekDocInMultiSegments(_testOptionFlags[i]);
        }
    }

    void TestCaseForUnpack()
    {
        for (size_t i = 0; i < _testOptionFlags.size(); i++) {
            TestUnpack(_testOptionFlags[i], false);
            TestUnpack(_testOptionFlags[i], true);
        }
    }

private:
    void CheckIterator(const std::vector<std::string>& strs, const std::vector<std::string>& sectionStrs,
                       optionflag_t optionFlag)
    {
        CheckIterator(strs, sectionStrs, optionFlag, true, false);
        CheckIterator(strs, sectionStrs, optionFlag, true, true);
        CheckIterator(strs, sectionStrs, optionFlag, false, true);
    }

    void CheckIterator(const std::vector<std::string>& strs, const std::vector<std::string>& sectionStrs,
                       optionflag_t optionFlag, bool usePool, bool hasSectionAttr)
    {
        tearDown();
        setUp();

        _packageConfig->SetHasSectionAttributeFlag(hasSectionAttr);
        _packageConfig->SetOptionFlag(optionFlag);
        IndexFormatOption indexFormatOption;
        indexFormatOption.Init(_packageConfig);

        std::vector<uint32_t> docCounts = CalcDocCounts(sectionStrs);
        std::vector<docid_t> baseDocIds = CreateBaseDocIds(docCounts);
        std::vector<uint8_t> compressModes;

        std::vector<PostingMaker::DocMap> docMaps;

        indexlib::index::InvertedTestHelper::BuildMultiSegmentsFromDataStrings(
            strs, _dir, _packageConfig->GetIndexName(), baseDocIds, docMaps, compressModes, indexFormatOption);

        std::vector<file_system::MemFileNodePtr> fileReaders;
        std::shared_ptr<SegmentPostingVector> segmentPostingVect;
        PrepareSegmentPostingVector(baseDocIds, compressModes, fileReaders, segmentPostingVect,
                                    indexFormatOption.GetPostingFormatOption());

        std::shared_ptr<SectionAttributeReader> sectionReader;
        if ((optionFlag & of_position_list) && hasSectionAttr) {
            indexlibv2::index::SectionDataMaker::BuildMultiSegmentsSectionData(_schema, _packageConfig->GetIndexName(),
                                                                               sectionStrs, _directory);
            sectionReader = PrepareSectionReader(docCounts.size());
        }

        autil::mem_pool::Pool sessionPool;
        autil::mem_pool::Pool* pPool = usePool ? &sessionPool : NULL;

        PostingFormatOption postingFormatOption(optionFlag);
        BufferedPostingIterator* iter =
            IE_POOL_COMPATIBLE_NEW_CLASS(pPool, BufferedPostingIterator, postingFormatOption, pPool, nullptr);
        iter->Init(segmentPostingVect, sectionReader.get(), 10);
        CheckAnswer(docMaps, iter, strs.size(), optionFlag);

        BufferedPostingIterator* cloneIter = dynamic_cast<BufferedPostingIterator*>(iter->Clone());
        CheckAnswer(docMaps, cloneIter, strs.size(), optionFlag);

        iter->Reset();
        CheckAnswer(docMaps, iter, strs.size(), optionFlag);

        IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, iter);
        IE_POOL_COMPATIBLE_DELETE_CLASS(pPool, cloneIter);
    }

    void CheckAnswer(const std::vector<PostingMaker::DocMap>& docMaps, BufferedPostingIterator* iter, size_t segCount,
                     optionflag_t optionFlag)
    {
        docid_t oldDocId = -1;
        int count = 0;

        for (size_t i = 0; i < segCount; ++i) {
            const PostingMaker::DocMap& docMap = docMaps[i];
            uint32_t seekedDocCount = 0;
            for (PostingMaker::DocMap::const_iterator it = docMap.begin(); it != docMap.end(); ++it) {
                docid_t docId = it->first.first;
                int dif = docId - oldDocId;
                if (dif > 0) {
                    docId -= rand() % dif;
                }

                oldDocId = it->first.first;

                seekedDocCount++;
                if (++count > 10 && (rand() % 4 == 2))
                    continue;

                docid_t seekedDocId = iter->SeekDoc(docId);
                assert(it->first.first == seekedDocId);
                ASSERT_EQ(it->first.first, seekedDocId);
                if (optionFlag & of_doc_payload) {
                    ASSERT_EQ(it->first.second, iter->GetDocPayload());
                } else {
                    ASSERT_EQ(0, iter->GetDocPayload());
                }

                if (optionFlag & of_position_list) {
                    pos_t oldPos = -1;
                    for (PackPostingMaker::PosValue::const_iterator it2 = it->second.begin(); it2 != it->second.end();
                         ++it2) {
                        pos_t pos = it2->first;
                        int posdif = pos - oldPos;
                        if (posdif > 0) {
                            pos -= rand() % posdif;
                        }
                        pos_t n = iter->SeekPosition(pos);
                        ASSERT_EQ(seekedDocCount, iter->_state.GetSeekedDocCount());

                        ASSERT_EQ(it2->first, n);
                        if (optionFlag & of_position_payload) {
                            ASSERT_EQ(it2->second, iter->GetPosPayload());
                        } else {
                            ASSERT_EQ(0, iter->GetPosPayload());
                        }
                        oldPos = it2->first;
                    }
                    ASSERT_EQ(INVALID_POSITION, iter->SeekPosition(oldPos + 7));
                } else {
                    ASSERT_EQ(INVALID_POSITION, iter->SeekPosition(0));
                }
            }
        }
        ASSERT_EQ(INVALID_DOCID, iter->SeekDoc(oldDocId + 7));
    }

private:
    void InitTabletData(uint32_t segCount)
    {
        auto tabletData = std::make_shared<indexlibv2::framework::TabletData>("ut");
        std::vector<std::shared_ptr<indexlibv2::framework::Segment>> segments;
        segments.reserve(segCount);
        auto directory = file_system::Directory::GetPhysicalDirectory(_dir);
        for (size_t i = 0; i < segCount; ++i) {
            segmentid_t segId = i;
            indexlibv2::framework::SegmentMeta segMeta(segId);
            segMeta.schema = _schema;
            auto segmentName = indexlibv2::PathUtil::NewSegmentDirName(segId, 0);
            auto segDir = directory->GetDirectory(segmentName, false);
            assert(segDir);
            segMeta.segmentDir = segDir;
            auto segInfo = std::make_shared<indexlibv2::framework::SegmentInfo>();
            auto status = segInfo->Load(segDir->GetIDirectory(), {file_system::FSOT_MEM});
            assert(status.IsOK());
            segMeta.segmentInfo = segInfo;
            auto segment = std::make_shared<indexlibv2::framework::FakeDiskSegment>(segMeta);
            indexlibv2::index::IndexerParameter indexerParam;
            indexerParam.docCount = segInfo->GetDocCount();
            indexerParam.segmentInfo = segInfo;
            auto invertedDiskIndexer = std::make_shared<InvertedDiskIndexer>(indexerParam);
            auto d = segDir->GetDirectory("index", false);
            assert(d);
            auto d1 = d->GetDirectory(_packageConfig->GetIndexName(), false);
            assert(d1);
            d1->Rename("posting", d1, "posting.tmp");
            status = invertedDiskIndexer->Open(_packageConfig, d->GetIDirectory());
            assert(status.IsOK());
            d1->Rename("posting.tmp", d1, "posting");
            segment->AddIndexer(_packageConfig->GetIndexType(), _packageConfig->GetIndexName(), invertedDiskIndexer);
            segments.push_back(segment);
        }
        std::shared_ptr<indexlibv2::framework::ResourceMap> map(new indexlibv2::framework::ResourceMap());
        auto schemaGroup = std::make_shared<indexlibv2::framework::TabletDataSchemaGroup>();
        schemaGroup->writeSchema = _schema;
        schemaGroup->onDiskWriteSchema = _schema;
        schemaGroup->onDiskReadSchema = _schema;
        auto status = map->AddVersionResource(indexlibv2::framework::TabletDataSchemaGroup::NAME, schemaGroup);
        assert(status.IsOK());
        status = tabletData->Init(/*invalid version*/ indexlibv2::framework::Version(), std::move(segments), map);
        assert(status.IsOK());
        _tabletData = tabletData;
    }

    std::shared_ptr<SectionAttributeReader> PrepareSectionReader(uint32_t segCount)
    {
        auto reader = std::make_shared<SectionAttributeReaderImpl>(indexlibv2::config::sp_nosort);
        InitTabletData(segCount);
        assert(_tabletData);
        auto status = reader->Open(_packageConfig, _tabletData.get());
        return reader;
    }

    void TestUnpack(optionflag_t optionFlag, bool hasSectionAttr)
    {
        tearDown();
        setUp();
        std::string str = "1 3, (1 3, 4 6, 8 1, 12 1, 17 8); "
                          "4 1, (4 2, 17 1)";

        std::string secStr = "0 1 1;"
                             "0 2 7, 1 8 4, 2 4 3, 2 5 7;" // 2 11 16 22
                             "0 1 1;"
                             "0 1 1;"
                             "0 1 1, 1 1 2, 1 1 2, 2 4 4, 2 8 8;"; // 1 3 5 10 19
        std::string segPath = _dir + SEGMENT_FILE_NAME_PREFIX + "_0_level_0/";
        indexlibv2::index::IndexTestUtil::ResetDir(segPath);
        std::string indexPath = segPath + INDEX_DIR_NAME + "/" + _packageConfig->GetIndexName() + "/";
        indexlibv2::index::IndexTestUtil::ResetDir(indexPath);
        std::string filePath = indexPath + POSTING_FILE_NAME;

        AnswerMap answerMap; // answerMap is not used in checking, and checking is hard-coded.
        docid_t baseDocId = 0;

        _packageConfig->SetHasSectionAttributeFlag(hasSectionAttr);
        _packageConfig->SetOptionFlag(optionFlag);
        IndexFormatOption indexFormatOption;
        indexFormatOption.Init(_packageConfig);
        uint8_t compressMode = indexlib::index::InvertedTestHelper::BuildOneSegmentFromDataString(
            str, filePath, baseDocId, answerMap, indexFormatOption);
        file_system::MemFileNodePtr fileReader(file_system::MemFileNodeCreator::TEST_Create());
        ASSERT_EQ(file_system::FSEC_OK, fileReader->Open("", filePath, file_system::FSOT_MEM, -1));
        ASSERT_EQ(file_system::FSEC_OK, fileReader->Populate());

        ::fslib::FileMeta meta;
        ::fslib::fs::FileSystem::getFileMeta(filePath, meta);
        util::ByteSliceListPtr sliceListPtr(
            fileReader->ReadToByteSliceList(meta.fileLength, 0, file_system::ReadOption()));
        assert(sliceListPtr);

        std::vector<uint32_t> docCounts;
        docCounts.push_back(5);
        std::vector<docid_t> baseDocIds = CreateBaseDocIds(docCounts);
        std::vector<uint8_t> compressModes;
        compressModes.push_back(compressMode);

        std::vector<file_system::MemFileNodePtr> fileReaders;
        std::shared_ptr<SegmentPostingVector> segmentPostingVect;
        PrepareSegmentPostingVector(baseDocIds, compressModes, fileReaders, segmentPostingVect,
                                    indexFormatOption.GetPostingFormatOption());

        PostingFormatOption postingFormatOption = indexFormatOption.GetPostingFormatOption();

        std::shared_ptr<BufferedPostingIterator> iter(new BufferedPostingIterator(postingFormatOption, NULL, nullptr));

        std::shared_ptr<SectionAttributeReader> sectionReader;
        if ((optionFlag & of_position_list) && hasSectionAttr) {
            indexlibv2::index::SectionDataMaker::BuildOneSegmentSectionData(_schema, _packageConfig->GetIndexName(),
                                                                            secStr, _directory, 0);

            sectionReader = PrepareSectionReader(docCounts.size());
        }
        iter->Init(segmentPostingVect, sectionReader.get(), 10);

        ASSERT_EQ(1, iter->SeekDoc(0));
        TermMatchData tmd;
        iter->Unpack(tmd);
        ASSERT_TRUE(tmd.IsMatched());

        if (optionFlag & of_term_frequency) {
            ASSERT_EQ(5, tmd.GetTermFreq());
        } else {
            ASSERT_TRUE(tmd.GetInDocPositionState() == NULL);
        }

        if (optionFlag & of_doc_payload) {
            ASSERT_EQ(3, tmd.GetDocPayload());
        }

        if (optionFlag & of_fieldmap) {
            ASSERT_EQ(19, tmd.GetFieldMap());
        }

        std::shared_ptr<InDocPositionIterator> posIter = tmd.GetInDocPositionIterator();
        if (optionFlag & of_position_list) {
            ASSERT_TRUE(posIter != NULL);

            ASSERT_EQ((uint32_t)1, posIter->SeekPosition(0));
            if (optionFlag & of_position_payload) {
                ASSERT_EQ(3, posIter->GetPosPayload());
            }

            if (hasSectionAttr) {
                ASSERT_EQ(0, posIter->GetSectionId());
                ASSERT_EQ(2, posIter->GetSectionLength());
                ASSERT_EQ(7, posIter->GetSectionWeight());
                ASSERT_EQ(0, posIter->GetFieldId());
            }

            ASSERT_EQ((size_t)4, posIter->SeekPosition(3));
            if (optionFlag & of_position_payload) {
                ASSERT_EQ(6, posIter->GetPosPayload());
            }

            if (hasSectionAttr) {
                ASSERT_EQ(0, posIter->GetSectionId());
                ASSERT_EQ(8, posIter->GetSectionLength());
                ASSERT_EQ(4, posIter->GetSectionWeight());
                ASSERT_EQ(1, posIter->GetFieldId());
            }

            // test the second in doc position iterator
            if (optionFlag & of_position_list) {
                std::shared_ptr<InDocPositionIterator> posIter2 = tmd.GetInDocPositionIterator();

                ASSERT_EQ((size_t)1, posIter2->SeekPosition(0));
                if (optionFlag & of_position_payload) {
                    ASSERT_EQ(3, posIter2->GetPosPayload());
                }

                ASSERT_EQ((size_t)4, posIter2->SeekPosition(3));
                if (optionFlag & of_position_payload) {
                    ASSERT_EQ(6, posIter2->GetPosPayload());
                }
            }

            ASSERT_EQ((size_t)8, posIter->SeekPosition(5));
            if (optionFlag & of_position_payload) {
                ASSERT_EQ(1, posIter->GetPosPayload());
            }

            if (hasSectionAttr) {
                ASSERT_EQ(0, posIter->GetSectionId());
                ASSERT_EQ(8, posIter->GetSectionLength());
                ASSERT_EQ(4, posIter->GetSectionWeight());
                ASSERT_EQ(1, posIter->GetFieldId());
            }
            ASSERT_EQ((size_t)12, posIter->SeekPosition(10));
            if (optionFlag & of_position_payload) {
                ASSERT_EQ(1, posIter->GetPosPayload());
            }
            if (hasSectionAttr) {
                ASSERT_EQ(0, posIter->GetSectionId());
                ASSERT_EQ(4, posIter->GetSectionLength());
                ASSERT_EQ(3, posIter->GetSectionWeight());
                ASSERT_EQ(2, posIter->GetFieldId());
            }

            ASSERT_EQ((size_t)17, posIter->SeekPosition(13));
            if (optionFlag & of_position_payload) {
                ASSERT_EQ(8, posIter->GetPosPayload());
            }

            if (hasSectionAttr) {
                ASSERT_EQ(1, posIter->GetSectionId());
                ASSERT_EQ(5, posIter->GetSectionLength());
                ASSERT_EQ(7, posIter->GetSectionWeight());
                ASSERT_EQ(2, posIter->GetFieldId());
            }
            ASSERT_EQ(INVALID_POSITION, posIter->SeekPosition(999));
        } else {
            ASSERT_TRUE(posIter == NULL);
        }

        ASSERT_EQ(4, iter->SeekDoc(1));
        TermMatchData nextTmd;
        iter->Unpack(nextTmd);
        ASSERT_TRUE(nextTmd.IsMatched());
    }

    void TestSeekDocInOneSegment(optionflag_t optionFlag)
    {
        std::string str1 = "1 2, (1 3); 2 1, (5 1, 9 2); 7 0, (1 7)";
        std::vector<std::string> strs;
        strs.push_back(str1);

        std::vector<std::string> sectStrs;
        indexlibv2::index::SectionDataMaker::CreateFakeSectionString(8, 20, strs, sectStrs);
        CheckIterator(strs, sectStrs, optionFlag);
    }

    void TestSeekDocInTwoSegments(optionflag_t optionFlag)
    {
        std::string str1 = "1 2, (1 3); 2 1, (5 1, 9 2)";
        std::string str2 = "11 2, (1 3); 12 1, (5 1, 9 2); 17 0, (1 7)";
        std::vector<std::string> strs;
        strs.push_back(str1);
        strs.push_back(str2);

        std::vector<std::string> sectStrs;
        indexlibv2::index::SectionDataMaker::CreateFakeSectionString(18, 20, strs, sectStrs);
        CheckIterator(strs, sectStrs, optionFlag);
    }

    void TestSeekDocInMultiSegments(optionflag_t optionFlag)
    {
        const int32_t keyCount[] = {3, 8};
        for (size_t k = 0; k < sizeof(keyCount) / sizeof(int32_t); ++k) {
            std::vector<std::string> strs;
            int oldDocId = 0;
            int maxPos = 0;
            for (int x = 0; x < keyCount[k]; ++x) {
                std::ostringstream oss;
                int docCount = 23 + rand() % 123;
                for (int i = 0; i < docCount; ++i) {
                    oss << (oldDocId += rand() % 19 + 1) << " ";
                    oss << rand() % 54321 << ", (";
                    int posCount = rand() % 10 + 1;
                    if (rand() % 37 == 25) {
                        posCount += rand() % 124;
                    }
                    int oldPos = 0;

                    for (int j = 0; j < posCount; ++j) {
                        oss << (oldPos += rand() % 7 + 1) << " ";
                        oss << rand() % 77 << ", ";
                    }
                    oss << ");";
                    if (oldPos > maxPos) {
                        maxPos = oldPos;
                    }
                }
                strs.push_back(oss.str());
            }
            std::vector<std::string> sectStrs;
            indexlibv2::index::SectionDataMaker::CreateFakeSectionString(oldDocId, maxPos, strs, sectStrs);
            CheckIterator(strs, sectStrs, optionFlag);
        }
    }

    std::vector<uint32_t> CalcDocCounts(const std::vector<std::string>& sectionStrs)
    {
        std::vector<uint32_t> docCounts;
        for (size_t i = 0; i < sectionStrs.size(); ++i) {
            autil::StringTokenizer st(sectionStrs[i], ";",
                                      autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

            docCounts.push_back(st.getNumTokens());
        }
        return docCounts;
    }

    void PrepareSegmentPostingVector(const std::vector<docid_t>& baseDocIds, const std::vector<uint8_t>& compressModes,
                                     std::vector<file_system::MemFileNodePtr>& fileReaders,
                                     std::shared_ptr<SegmentPostingVector>& segmentPostingVect,
                                     const PostingFormatOption& postingFormatOption)
    {
        segmentPostingVect.reset(new SegmentPostingVector);
        for (size_t i = 0; i < baseDocIds.size(); ++i) {
            std::stringstream dumpDir;
            dumpDir << _dir << SEGMENT_FILE_NAME_PREFIX << "_" << i << "_level_0"
                    << "/";
            std::string filePath =
                dumpDir.str() + INDEX_DIR_NAME + "/" + _packageConfig->GetIndexName() + "/" + POSTING_FILE_NAME;

            ::fslib::FileMeta meta;
            ::fslib::fs::FileSystem::getFileMeta(filePath, meta);

            fileReaders.push_back(file_system::MemFileNodePtr(file_system::MemFileNodeCreator::TEST_Create()));
            ASSERT_EQ(file_system::FSEC_OK,
                      fileReaders[i]->Open("", util::PathUtil::NormalizePath(filePath), file_system::FSOT_MEM, -1));
            ASSERT_EQ(file_system::FSEC_OK, fileReaders[i]->Populate());
            util::ByteSliceListPtr sliceListPtr(
                fileReaders[i]->ReadToByteSliceList(meta.fileLength, 0, file_system::ReadOption()));
            assert(sliceListPtr);

            SegmentPosting segmentPosting(postingFormatOption);

            segmentPosting.Init(compressModes[i], sliceListPtr, baseDocIds[i], 0);
            segmentPostingVect->push_back(segmentPosting);
        }
    }

    std::vector<docid_t> CreateBaseDocIds(const std::vector<uint32_t>& docCounts)
    {
        std::vector<docid_t> baseDocIds;
        docid_t baseDocId = 0;
        for (size_t i = 0; i < docCounts.size(); ++i) {
            baseDocIds.push_back(baseDocId);
            baseDocId += docCounts[i];
        }
        return baseDocIds;
    }

private:
    static const uint32_t MAX_DOC_NUM = 1024 * 16;
    static const uint32_t MAX_FIELD_COUNT = 32;
    std::string _key;
    std::string _dir;
    file_system::IFileSystemPtr _fileSystem;
    file_system::DirectoryPtr _directory;
    std::vector<optionflag_t> _testOptionFlags;
    std::shared_ptr<indexlibv2::config::PackageIndexConfig> _packageConfig;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::framework::TabletData> _tabletData;
};

TEST_F(BufferedPostingIteratorTest, TestCaseForSeekDocInOneSegment) { TestCaseForSeekDocInOneSegment(); }
TEST_F(BufferedPostingIteratorTest, TestCaseForSeekDocInTwoSegments) { TestCaseForSeekDocInTwoSegments(); }
TEST_F(BufferedPostingIteratorTest, TestCaseForSeekDocInManySegmentsTest_1)
{
    TestCaseForSeekDocInManySegmentsTest_1();
}
TEST_F(BufferedPostingIteratorTest, TestCaseForSeekDocInManySegmentsTest_2)
{
    TestCaseForSeekDocInManySegmentsTest_2();
}
TEST_F(BufferedPostingIteratorTest, TestCaseForSeekDocInManySegmentsTest_3)
{
    TestCaseForSeekDocInManySegmentsTest_3();
}
TEST_F(BufferedPostingIteratorTest, TestCaseForSeekDocInManySegmentsTest_4)
{
    TestCaseForSeekDocInManySegmentsTest_4();
}
TEST_F(BufferedPostingIteratorTest, TestCaseForUnpack) { TestCaseForUnpack(); }
} // namespace indexlib::index

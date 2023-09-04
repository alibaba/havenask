#include "indexlib/index/inverted_index/OnDiskIndexIterator.h"

#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/index/inverted_index//format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/builtin_index/pack/OnDiskPackIndexIterator.h"
#include "indexlib/index/inverted_index/builtin_index/pack/test/PackPostingMaker.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "unittest/unittest.h"
namespace indexlib::index {

class OnDiskIndexIteratorTest : public TESTBASE
{
public:
    typedef PostingMaker::DocMap DocMap;

    void setUp() override
    {
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
    }

    void TestCaseForIndexIterator() { TestForIndexIterator(OPTION_FLAG_ALL); }

    void TestCaseForIndexIteratorWithoutPosList() { TestForIndexIterator(NO_POSITION_LIST); }

protected:
    void TestForIndexIterator(optionflag_t optionFlag)
    {
        uint32_t termCount = 100;
        uint32_t docCount = 800;
        std::stringstream ss;

        for (uint32_t i = 0; i < docCount; i++) {
            ss << i << " " << i % 128 << ", (";
            uint32_t posCount = i % 5 + 1;
            for (uint32_t j = 0; j < posCount; j++) {
                ss << j << " " << j;
                if (j != posCount - 1) {
                    ss << ", ";
                } else {
                    ss << ")";
                }
            }
            if (i != docCount - 1) {
                ss << "; ";
            }
        }
        std::string str = ss.str();

        std::vector<DocMap> docMapList;
        for (uint32_t i = 0; i < termCount; i++) {
            DocMap docMap = PostingMaker::MakeDocMap(str);
            docMapList.push_back(docMap);
        }

        PostingFormatOption formatOption(optionFlag);
        MakeData(formatOption, docMapList);
        CheckData(formatOption, docMapList);
    }

private:
    void MakeData(const PostingFormatOption& formatOption, const std::vector<DocMap>& docMapList)
    {
        file_system::DirectoryPtr directory = _directory;
        util::SimplePool simplePool;
        TieredDictionaryWriter<dictkey_t> dictWriter(&simplePool);
        dictWriter.Open(directory, DICTIONARY_FILE_NAME);
        file_system::FileWriterPtr postingFileWriter = directory->CreateFileWriter(POSTING_FILE_NAME);

        for (uint32_t i = 0; i < docMapList.size(); i++) {
            static const uint32_t DEFAULT_CHUNK_SIZE = 100 * 1024;
            autil::mem_pool::SimpleAllocator allocator;
            autil::mem_pool::Pool* byteSlicePool = new autil::mem_pool::Pool(&allocator, DEFAULT_CHUNK_SIZE);
            std::unique_ptr<autil::mem_pool::Pool> poolPtr1(byteSlicePool);

            autil::mem_pool::RecyclePool* bufferPool = new autil::mem_pool::RecyclePool(&allocator, DEFAULT_CHUNK_SIZE);
            std::unique_ptr<autil::mem_pool::RecyclePool> poolPtr2(bufferPool);

            PostingWriterResource writerResource(&simplePool, byteSlicePool, bufferPool, formatOption);
            PostingWriterImpl writer(&writerResource);
            PackPostingMaker::MakePostingData(writer, docMapList[i]);
            writer.EndSegment();

            TermMeta termMeta(writer.GetDF(), writer.GetTotalTF());
            termMeta.SetPayload(writer.GetTermPayload());

            uint64_t key = (uint64_t)i;
            uint64_t offset = postingFileWriter->GetLength();
            TermMetaDumper tmDumper(formatOption);
            uint32_t postingLen = tmDumper.CalculateStoreSize(termMeta) + writer.GetDumpLength();

            postingFileWriter->Write((void*)(&postingLen), sizeof(uint32_t)).GetOrThrow();
            tmDumper.Dump(postingFileWriter, termMeta);
            writer.Dump(postingFileWriter);
            dictWriter.AddItem(index::DictKeyInfo(key), offset);
        }

        ASSERT_EQ(file_system::FSEC_OK, postingFileWriter->Close());
        dictWriter.Close();
    }

    void CheckData(const PostingFormatOption& formatOption, const std::vector<DocMap>& docMapList)
    {
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig(
            new indexlibv2::config::SingleFieldIndexConfig("test_index", it_text));
        OnDiskPackIndexIterator indexIter(indexConfig, _directory, formatOption);
        indexIter.Init();

        for (uint32_t i = 0; i < docMapList.size(); i++) {
            ASSERT_TRUE(indexIter.HasNext());
            index::DictKeyInfo key;
            PostingDecoder* decoderBase = indexIter.Next(key);
            PostingDecoderImpl* decoder = dynamic_cast<PostingDecoderImpl*>(decoderBase);
            ASSERT_FALSE(key.IsNull());
            ASSERT_EQ((uint64_t)i, key.GetKey());

            std::vector<docid_t> docIds;
            std::vector<docpayload_t> docPayloads;
            std::vector<pos_t> positions;
            std::vector<pospayload_t> posPayloads;

            DocMap docMap = docMapList[i];
            for (PackPostingMaker::DocMap::const_iterator it = docMap.begin(); it != docMap.end(); ++it) {
                docid_t docId = it->first.first;
                docpayload_t docPayload = it->first.second;
                docIds.push_back(docId);
                docPayloads.push_back(docPayload);

                for (PostingMaker::PosValue::const_iterator it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
                    positions.push_back(it2->first);
                    posPayloads.push_back(it2->second);
                }
            }
            uint32_t docCursor = 0;
            uint32_t posCursor = 0;

            docid_t docBuffer[MAX_DOC_PER_RECORD];
            docid_t tfBuffer[MAX_DOC_PER_RECORD];
            docpayload_t docPayloadBuffer[MAX_DOC_PER_RECORD];
            fieldmap_t fieldMapBuffer[MAX_DOC_PER_RECORD];
            pos_t posBuffer[MAX_POS_PER_RECORD];
            pospayload_t posPayloadBuffer[MAX_POS_PER_RECORD];

            docid_t docId = 0;
            while (docCursor < docIds.size()) {
                uint32_t docNum =
                    decoder->DecodeDocList(docBuffer, tfBuffer, docPayloadBuffer, fieldMapBuffer, MAX_DOC_PER_RECORD);

                for (uint32_t k = 0; k < docNum; k++) {
                    docId += docBuffer[k];
                    ASSERT_EQ(docIds[docCursor], docId);
                    ASSERT_EQ(docPayloads[docCursor], docPayloadBuffer[k]);
                    docCursor++;
                }
            }

            if (formatOption.HasPositionList()) {
                while (posCursor < positions.size()) {
                    uint32_t posNum = decoder->DecodePosList(posBuffer, posPayloadBuffer, MAX_DOC_PER_RECORD);

                    for (uint32_t k = 0; k < posNum; k++) {
                        //                    ASSERT_EQ(positions[posCursor], posBuffer[k]);
                        ASSERT_EQ(posPayloads[posCursor], posPayloadBuffer[k]);
                        posCursor++;
                    }
                }
            }
        }
        ASSERT_TRUE(!indexIter.HasNext());
    }

private:
    std::string _postingListStr;
    std::string _dir;
    file_system::IFileSystemPtr _fileSystem;
    file_system::DirectoryPtr _directory;
};

TEST_F(OnDiskIndexIteratorTest, TestCaseForIndexIterator) { TestCaseForIndexIterator(); }
TEST_F(OnDiskIndexIteratorTest, TestCaseForIndexIteratorWithoutPosList) { TestCaseForIndexIteratorWithoutPosList(); }
} // namespace indexlib::index

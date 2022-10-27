#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_posting_maker.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder_impl.h"
#include "indexlib/index/normal/inverted_index//format/term_meta_dumper.h"
#include "indexlib/config/single_field_index_config.h"
#include <autil/mem_pool/SimpleAllocator.h>

using namespace std;
using namespace std::tr1;
using namespace autil::mem_pool;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);

class OnDiskIndexIteratorTest : public INDEXLIB_TESTBASE
{
public:
    typedef PostingMaker::DocMap DocMap;
    DECLARE_CLASS_NAME(OnDiskIndexIteratorTest);
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForIndexIterator()
    {
        TestForIndexIterator(OPTION_FLAG_ALL);
    }

    void TestCaseForIndexIteratorWithoutPosList()
    {
        TestForIndexIterator(NO_POSITION_LIST);
    }

protected:
    void TestForIndexIterator(optionflag_t optionFlag)
    {
        uint32_t termCount = 100;
        uint32_t docCount = 800;
        stringstream ss;
        
        for (uint32_t i = 0; i < docCount; i++)
        {
            ss << i << " " << i % 128 << ", (";
            uint32_t posCount = i % 5 + 1;
            for (uint32_t j = 0; j < posCount; j++)
            {
                ss << j << " " << j;
                if (j != posCount - 1)
                {
                    ss << ", ";
                }
                else
                {
                    ss << ")";
                }
            }
            if (i != docCount -1)
            {
                ss << "; ";
            }
        }
        string str = ss.str();
        
        vector<DocMap> docMapList;
        for (uint32_t i = 0; i < termCount; i++)
        {
            DocMap docMap = PostingMaker::MakeDocMap(str);
            docMapList.push_back(docMap);
        }

        PostingFormatOption formatOption(optionFlag);
        MakeData(formatOption, docMapList);
        CheckData(formatOption, docMapList);
    }

private:
    void MakeData(const PostingFormatOption& formatOption,
                  const vector<DocMap>& docMapList)
    {
        DirectoryPtr directory = DirectoryCreator::Create(mDir);
        SimplePool simplePool;
        TieredDictionaryWriter<dictkey_t> dictWriter(&simplePool);
        dictWriter.Open(directory, DICTIONARY_FILE_NAME);
        FileWriterPtr postingFileWriter = directory->CreateFileWriter(POSTING_FILE_NAME);

        for (uint32_t i = 0; i < docMapList.size(); i++)
        {
            static const uint32_t DEFAULT_CHUNK_SIZE = 100 * 1024;
            SimpleAllocator allocator;
            Pool* byteSlicePool = new Pool(&allocator, DEFAULT_CHUNK_SIZE);
            unique_ptr<Pool> poolPtr1(byteSlicePool);

            RecyclePool* bufferPool = new RecyclePool(&allocator, DEFAULT_CHUNK_SIZE);
            unique_ptr<RecyclePool> poolPtr2(bufferPool);

            PostingWriterResource writerResource(&simplePool, byteSlicePool,
                    bufferPool, formatOption);
            PostingWriterImpl writer(&writerResource, true);
            PackPostingMaker::MakePostingData(writer, docMapList[i]);
            writer.EndSegment();

            TermMeta termMeta(writer.GetDF(), writer.GetTotalTF());
            termMeta.SetPayload(writer.GetTermPayload());

            uint64_t key = (uint64_t)i;
            uint64_t offset = postingFileWriter->GetLength();
            TermMetaDumper tmDumper(formatOption);
            uint32_t postingLen = tmDumper.CalculateStoreSize(termMeta) + writer.GetDumpLength();

            postingFileWriter->Write((void*)(&postingLen), sizeof(uint32_t));
            tmDumper.Dump(postingFileWriter, termMeta);
            writer.Dump(postingFileWriter);
            dictWriter.AddItem(key, offset);
        }

        postingFileWriter->Close();
        dictWriter.Close();
    }

    void CheckData(const PostingFormatOption& formatOption,
                   const vector<DocMap>& docMapList)
    {
        IndexConfigPtr indexConfig(new SingleFieldIndexConfig("test_index", it_text));
        OnDiskPackIndexIterator indexIter(
                indexConfig, GET_PARTITION_DIRECTORY(), formatOption);
        indexIter.Init();
        
        for (uint32_t i = 0; i < docMapList.size(); i++)
        {
            INDEXLIB_TEST_TRUE(indexIter.HasNext());
            dictkey_t key;
            PostingDecoder* decoderBase = indexIter.Next(key);
            PostingDecoderImpl* decoder = 
                dynamic_cast<PostingDecoderImpl*>(decoderBase);
            INDEXLIB_TEST_EQUAL((uint64_t)i, key);

            vector<docid_t> docIds;
            vector<docpayload_t> docPayloads;
            vector<pos_t> positions;
            vector<pospayload_t> posPayloads;

            DocMap docMap = docMapList[i];
            for (PackPostingMaker::DocMap::const_iterator it = docMap.begin();
                 it != docMap.end(); ++it)
            {
                docid_t docId = it->first.first;
                docpayload_t docPayload = it->first.second;
                docIds.push_back(docId);
                docPayloads.push_back(docPayload);

                for (PostingMaker::PosValue::const_iterator it2 = it->second.begin(); 
                     it2 != it->second.end(); ++it2)
                {
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
            while (docCursor < docIds.size())
            {
                uint32_t docNum = decoder->DecodeDocList(docBuffer, tfBuffer,
                        docPayloadBuffer, fieldMapBuffer, MAX_DOC_PER_RECORD);

                for (uint32_t k = 0; k < docNum; k++)
                {
                    docId += docBuffer[k];
                    INDEXLIB_TEST_EQUAL(docIds[docCursor], docId);
                    INDEXLIB_TEST_EQUAL(docPayloads[docCursor], docPayloadBuffer[k]);
                    docCursor++;
                }
            }

            if (formatOption.HasPositionList())
            {
                while (posCursor < positions.size())
                {
                    uint32_t posNum = decoder->DecodePosList(
                            posBuffer, posPayloadBuffer, MAX_DOC_PER_RECORD);
                
                    for (uint32_t k = 0; k < posNum; k++)
                    {
//                    INDEXLIB_TEST_EQUAL(positions[posCursor], posBuffer[k]);
                        INDEXLIB_TEST_EQUAL(posPayloads[posCursor], posPayloadBuffer[k]);
                        posCursor++;
                    }
                }
            }
            
        }
        INDEXLIB_TEST_TRUE(!indexIter.HasNext());
    }

private:
    string mDir;
    string mPostingListStr; 
};


INDEXLIB_UNIT_TEST_CASE(OnDiskIndexIteratorTest, TestCaseForIndexIterator);
INDEXLIB_UNIT_TEST_CASE(OnDiskIndexIteratorTest, TestCaseForIndexIteratorWithoutPosList);

IE_NAMESPACE_END(index);

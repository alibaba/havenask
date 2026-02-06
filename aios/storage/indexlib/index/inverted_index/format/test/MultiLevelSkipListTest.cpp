#include <memory>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"
#include "indexlib/index/inverted_index/format/DocListFormatOption.h"
#include "indexlib/index/inverted_index/format/DocListSkipListFormat.h"
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"
#include "indexlib/index/inverted_index/format/skiplist/MultiLevelSkipListReader.h"
#include "indexlib/index/inverted_index/format/skiplist/MultiLevelSkipListWriter.h"
#include "indexlib/index/inverted_index/format/skiplist/PairValueSkipListReader.h"
#include "indexlib/index/inverted_index/format/skiplist/TriValueSkipListReader.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class MultiLevelSkipListTest : public TESTBASE {
public:
    MultiLevelSkipListTest() : _byteSlicePool(1024), _bufferPool(1024) {
        DocListFormatOption option(OPTION_FLAG_ALL);
        _docListSkipListFormat.reset(new DocListSkipListFormat(
            option, indexlibv2::config::InvertedIndexConfig::DEFAULT_FORMAT_VERSION));
    }
    ~MultiLevelSkipListTest() {};

public:
    void setUp() override {
        _rootDir = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "multi_level_skip_list_test");
        file_system::FslibWrapper::MkDirE(_rootDir);
    }
    void tearDown() override {}

private:
    std::shared_ptr<DocListSkipListFormat> _docListSkipListFormat;
    autil::mem_pool::Pool _byteSlicePool;
    autil::mem_pool::RecyclePool _bufferPool;
    std::string _rootDir;
};

void WriteSkipListBuffer(std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer, size_t df) {
    uint32_t c = 1;
    docid_t docid = MAX_DOC_PER_RECORD - 1;
    while (docid < df) {
        skipListBuffer->AddItem(docid, c);
        if (docid == df - 1) {
            break;
        }
        docid += MAX_DOC_PER_RECORD;
        c++;
    }
    if (docid != df - 1) {
        skipListBuffer->AddItem(df - 1, c);
    }
}

TEST_F(MultiLevelSkipListTest, testSimpleProcess) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));
    skipListBuffer->AddItem(1, 2, 3);
    // 4 bytes:
    // Content: control byte + key + value1 + value2
    // Level 0 has no childpointer
    ASSERT_EQ((size_t)4, skipListBuffer->EstimateDumpSize());
}

TEST_F(MultiLevelSkipListTest, testSingleLayer) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));

    // Construct a single-level skip list
    size_t df = MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER - 1);
    WriteSkipListBuffer(skipListBuffer, df);

    ASSERT_EQ(1, skipListBuffer->_skipLevel);

    file_system::BufferedFileWriterPtr fileWriter(new file_system::BufferedFileWriter());
    std::string filePath = _rootDir + "/dump_file_single_layer";
    ASSERT_EQ(FSEC_OK, fileWriter->Open(filePath, filePath));
    size_t dumpSize = skipListBuffer->EstimateDumpSize();
    skipListBuffer->Dump(fileWriter);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    file_system::BufferedFileReaderPtr fileReader(new file_system::BufferedFileReader());
    ASSERT_EQ(FSEC_OK, fileReader->Open(filePath));
    ASSERT_EQ(dumpSize, fileReader->GetLength());
    indexlib::util::ByteSlice* slice = indexlib::util::ByteSlice::CreateObject(dumpSize);
    ASSERT_EQ(FSEC_OK, fileReader->Read(slice->data, dumpSize, 0, ReadOption()).Code());
    indexlib::util::ByteSliceList sliceList = indexlib::util::ByteSliceList(slice);

    std::shared_ptr<MultiLevelPairValueSkipListReader> skipListReader(
        new MultiLevelPairValueSkipListReader());
    skipListReader->Load(&_byteSlicePool, &sliceList, 0, sliceList.GetTotalSize(),
                         (df - 1) / MAX_POS_PER_RECORD + 1);
    ASSERT_EQ(1, skipListReader->_numberOfSkipLevel);

    // Inspecting the data at level 0, the SkipReader::Load function prefetches the first node.
    docid_t docId = skipListReader->_metas[0].skipDoc;
    uint32_t addr = skipListReader->_metas[0].offset;
    for (uint32_t i = 1; i < SKIP_MULTIPLIER - 1; ++i) {
        uint32_t data[2];
        GroupVarintCompressor::Decode(&skipListReader->_readers[0], 2, data);
        docId += data[0];
        addr += data[1];
        ASSERT_EQ(docId, (MAX_DOC_PER_RECORD - 1) + i * MAX_DOC_PER_RECORD);
        ASSERT_EQ(data[1], i + 1);
    }
    ASSERT_EQ(skipListReader->_metas[0].levelEnd, skipListReader->_readers[0].Tell());
}

// Write data using MultiLevelSkipListWriter, then use MultiLevelPairValueSkipListReader validate
// the data at each level
TEST_F(MultiLevelSkipListTest, testDump) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));

    // Construct a 3-level skip list
    size_t df = MAX_DOC_PER_RECORD * SKIP_MULTIPLIER * SKIP_MULTIPLIER;
    WriteSkipListBuffer(skipListBuffer, df);

    ASSERT_EQ(3, skipListBuffer->_skipLevel);

    file_system::BufferedFileWriterPtr fileWriter(new file_system::BufferedFileWriter());
    std::string filePath = _rootDir + "/dump_file";
    ASSERT_EQ(FSEC_OK, fileWriter->Open(filePath, filePath));
    size_t dumpSize = skipListBuffer->EstimateDumpSize();
    skipListBuffer->Dump(fileWriter);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    file_system::BufferedFileReaderPtr fileReader(new file_system::BufferedFileReader());
    ASSERT_EQ(FSEC_OK, fileReader->Open(filePath));
    ASSERT_EQ(dumpSize, fileReader->GetLength());
    indexlib::util::ByteSlice* slice = indexlib::util::ByteSlice::CreateObject(dumpSize);
    ASSERT_EQ(FSEC_OK, fileReader->Read(slice->data, dumpSize, 0, ReadOption()).Code());
    indexlib::util::ByteSliceList sliceList = indexlib::util::ByteSliceList(slice);

    std::shared_ptr<MultiLevelPairValueSkipListReader> skipListReader(
        new MultiLevelPairValueSkipListReader());
    skipListReader->Load(&_byteSlicePool, &sliceList, 0, sliceList.GetTotalSize(),
                         (df - 1) / MAX_POS_PER_RECORD + 1);
    ASSERT_EQ(3, skipListReader->_numberOfSkipLevel);

    docid_t docId = skipListReader->_metas[0].skipDoc;
    uint32_t addr = skipListReader->_metas[0].offset;
    std::vector<docid_t> level2;
    std::vector<uint32_t> offset2;
    std::vector<uint32_t> child2;
    for (uint32_t i = 1; i < SKIP_MULTIPLIER * SKIP_MULTIPLIER; ++i) {
        uint32_t data[2];
        GroupVarintCompressor::Decode(&skipListReader->_readers[0], 2, data);
        docId += data[0];
        addr += data[1];
        ASSERT_EQ(docId, (MAX_DOC_PER_RECORD - 1) + i * MAX_DOC_PER_RECORD);
        ASSERT_EQ(data[1], i + 1);
        if ((i + 1) % SKIP_MULTIPLIER == 0) {
            level2.push_back(docId);
            offset2.push_back(addr);
            child2.push_back(skipListReader->_readers[0].Tell() -
                             skipListReader->_metas[0].levelBegin);
        }
    }
    ASSERT_EQ(skipListReader->_metas[0].levelEnd, skipListReader->_readers[0].Tell());

    // Check the level 1 data
    docId = 0;
    addr = 0;
    docid_t level3;
    uint32_t offset3;
    uint32_t child3;
    for (uint32_t i = 0; i < SKIP_MULTIPLIER; ++i) {
        if (i == SKIP_MULTIPLIER - 1) {
            child3 = skipListReader->_readers[1].Tell() - skipListReader->_metas[1].levelBegin;
        }
        uint32_t data[3];
        GroupVarintCompressor::Decode(&skipListReader->_readers[1], 3, data);
        docId += data[0];
        addr += data[1];
        if (i == SKIP_MULTIPLIER - 1) {
            level3 = docId;
            offset3 = addr;
        }
        ASSERT_EQ(level2[i], docId);
        ASSERT_EQ(offset2[i], addr);
        ASSERT_EQ(child2[i], data[2]);
    }
    ASSERT_EQ(skipListReader->_metas[1].levelEnd, skipListReader->_readers[1].Tell());

    // Check the level 2 data
    uint32_t data[3];
    GroupVarintCompressor::Decode(&skipListReader->_readers[2], 3, data);
    ASSERT_EQ(data[0], level3);
    ASSERT_EQ(data[1], offset3);
    ASSERT_EQ(data[2], child3);
    ASSERT_EQ(skipListReader->_metas[2].levelEnd, skipListReader->_readers[2].Tell());
}

TEST_F(MultiLevelSkipListTest, testSkipTo) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));

    // Construct a 3-level skip list
    size_t df = MAX_DOC_PER_RECORD * SKIP_MULTIPLIER * SKIP_MULTIPLIER * (SKIP_MULTIPLIER - 1);
    // A flush will be triggered when the document is written to the MAX_DOC_PER_RECORD - 1st
    // document.
    for (docid_t docid = MAX_DOC_PER_RECORD - 1; docid < df; docid += MAX_DOC_PER_RECORD) {
        skipListBuffer->AddItem(docid, 100);
    }

    ASSERT_EQ(3, skipListBuffer->_skipLevel);

    file_system::BufferedFileWriterPtr fileWriter(new file_system::BufferedFileWriter());
    std::string filePath = _rootDir + "/skipto_file";
    ASSERT_EQ(FSEC_OK, fileWriter->Open(filePath, filePath));
    size_t dumpSize = skipListBuffer->EstimateDumpSize();
    skipListBuffer->Dump(fileWriter);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    file_system::BufferedFileReaderPtr fileReader(new file_system::BufferedFileReader());
    ASSERT_EQ(FSEC_OK, fileReader->Open(filePath));
    ASSERT_EQ(dumpSize, fileReader->GetLength());
    indexlib::util::ByteSlice* slice = indexlib::util::ByteSlice::CreateObject(dumpSize);
    ASSERT_EQ(FSEC_OK, fileReader->Read(slice->data, dumpSize, 0, ReadOption()).Code());
    indexlib::util::ByteSliceList sliceList = indexlib::util::ByteSliceList(slice);

    std::shared_ptr<MultiLevelPairValueSkipListReader> skipListReader(
        new MultiLevelPairValueSkipListReader());
    skipListReader->Load(&_byteSlicePool, &sliceList, 0, sliceList.GetTotalSize(),
                         (df - 1) / MAX_POS_PER_RECORD + 1);

    uint32_t key = 0;
    uint32_t prevKey = 0;
    uint32_t value = 0;
    uint32_t delta = 0;
    // case 1
    ASSERT_EQ(skipListReader->SkipTo(0, key, prevKey, value, delta).second, true);
    ASSERT_EQ(key, 127);
    ASSERT_EQ(prevKey, 0);
    ASSERT_EQ(value, 0);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    // case 2
    ASSERT_EQ(skipListReader->SkipTo(MAX_DOC_PER_RECORD + 1, key, prevKey, value, delta).second,
              true);
    ASSERT_EQ(key, MAX_DOC_PER_RECORD * 2 - 1);
    ASSERT_EQ(prevKey, MAX_DOC_PER_RECORD - 1);
    ASSERT_EQ(value, 100);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    // case 3
    ASSERT_EQ(skipListReader
                  ->SkipTo(MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER) - 20, key,
                           prevKey, value, delta)
                  .second,
              true);
    ASSERT_EQ(key, MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER) - 1);
    ASSERT_EQ(prevKey, MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER - 1) - 1);
    ASSERT_EQ(value, (SKIP_MULTIPLIER * SKIP_MULTIPLIER - 1) * 100);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    // case 4
    ASSERT_EQ(skipListReader->SkipTo(df, key, prevKey, value, delta).second, false);
}

TEST_F(MultiLevelSkipListTest, testRealtimeSkipTo) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));
    size_t df = MAX_DOC_PER_RECORD * SKIP_MULTIPLIER * SKIP_MULTIPLIER * (SKIP_MULTIPLIER - 1);
    uint32_t key = 0;
    uint32_t prevKey = 0;
    uint32_t value = 0;
    uint32_t delta = 0;

    // case 1
    std::shared_ptr<MultiLevelPairValueSkipListReader> skipListReader1(
        new MultiLevelPairValueSkipListReader());
    skipListBuffer->SnapShot(skipListReader1.get(), &_byteSlicePool);
    // case 2
    skipListBuffer->AddItem(MAX_DOC_PER_RECORD - 1, 100);
    std::shared_ptr<MultiLevelPairValueSkipListReader> skipListReader2(
        new MultiLevelPairValueSkipListReader());
    skipListBuffer->SnapShot(skipListReader2.get(), &_byteSlicePool);
    // case 3
    std::shared_ptr<MultiLevelPairValueSkipListReader> skipListReader3(
        new MultiLevelPairValueSkipListReader());
    for (docid_t docid = 2 * MAX_DOC_PER_RECORD - 1; docid < df; docid += MAX_DOC_PER_RECORD) {
        skipListBuffer->AddItem(docid, 100);
    }
    skipListBuffer->SnapShot(skipListReader3.get(), &_byteSlicePool);

    // assert
    ASSERT_EQ(skipListReader1->SkipTo(0, key, prevKey, value, delta).second, false);
    ASSERT_EQ(skipListReader2->SkipTo(0, key, prevKey, value, delta).second, true);
    ASSERT_EQ(key, 127);
    ASSERT_EQ(prevKey, 0);
    ASSERT_EQ(value, 0);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader2->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader2->SkipTo(MAX_DOC_PER_RECORD + 1, key, prevKey, value, delta).second,
              false);
    ASSERT_EQ(skipListReader3->SkipTo(0, key, prevKey, value, delta).second, true);
    ASSERT_EQ(key, 127);
    ASSERT_EQ(prevKey, 0);
    ASSERT_EQ(value, 0);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader3->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader3->SkipTo(MAX_DOC_PER_RECORD + 1, key, prevKey, value, delta).second,
              true);
    ASSERT_EQ(key, MAX_DOC_PER_RECORD * 2 - 1);
    ASSERT_EQ(prevKey, MAX_DOC_PER_RECORD - 1);
    ASSERT_EQ(value, 100);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader3->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader3
                  ->SkipTo(MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER) - 20, key,
                           prevKey, value, delta)
                  .second,
              true);
    ASSERT_EQ(key, MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER) - 1);
    ASSERT_EQ(prevKey, MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER - 1) - 1);
    ASSERT_EQ(value, (SKIP_MULTIPLIER * SKIP_MULTIPLIER - 1) * 100);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader3->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader3->SkipTo(df, key, prevKey, value, delta).second, false);
}

TEST_F(MultiLevelSkipListTest, testDumpWithTTF) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));

    size_t df = MAX_DOC_PER_RECORD * SKIP_MULTIPLIER * SKIP_MULTIPLIER;
    uint32_t c = 1;
    for (docid_t docid = MAX_DOC_PER_RECORD - 1; docid < df; docid += MAX_DOC_PER_RECORD) {
        // Assuming the term appears twice in each document, then ttf = 2 * (docid + 1)
        skipListBuffer->AddItem(docid, 2 * (docid + 1), c);
        c++;
    }

    ASSERT_EQ(3, skipListBuffer->_skipLevel);

    file_system::BufferedFileWriterPtr fileWriter(new file_system::BufferedFileWriter());
    std::string filePath = _rootDir + "/dump_file_with_ttf";
    ASSERT_EQ(FSEC_OK, fileWriter->Open(filePath, filePath));
    size_t dumpSize = skipListBuffer->EstimateDumpSize();
    skipListBuffer->Dump(fileWriter);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    file_system::BufferedFileReaderPtr fileReader(new file_system::BufferedFileReader());
    ASSERT_EQ(FSEC_OK, fileReader->Open(filePath));
    ASSERT_EQ(dumpSize, fileReader->GetLength());
    indexlib::util::ByteSlice* slice = indexlib::util::ByteSlice::CreateObject(dumpSize);
    ASSERT_EQ(FSEC_OK, fileReader->Read(slice->data, dumpSize, 0, ReadOption()).Code());
    indexlib::util::ByteSliceList sliceList = indexlib::util::ByteSliceList(slice);

    std::shared_ptr<MultiLevelTriValueSkipListReader> skipListReader(
        new MultiLevelTriValueSkipListReader());
    skipListReader->Load(&_byteSlicePool, &sliceList, 0, sliceList.GetTotalSize(),
                         (df - 1) / MAX_POS_PER_RECORD + 1);
    ASSERT_EQ(3, skipListReader->_numberOfSkipLevel);

    docid_t docId = skipListReader->_metas[0].skipDoc;
    uint32_t ttf = skipListReader->_metas[0].ttf;
    uint32_t addr = skipListReader->_metas[0].offset;
    std::vector<docid_t> level2;
    std::vector<uint32_t> ttf2;
    std::vector<uint32_t> offset2;
    std::vector<uint32_t> child2;
    for (uint32_t i = 1; i < SKIP_MULTIPLIER * SKIP_MULTIPLIER; ++i) {
        uint32_t data[3];
        GroupVarintCompressor::Decode(&skipListReader->_readers[0], 3, data);
        docId += data[0];
        ttf += data[1];
        addr += data[2];
        ASSERT_EQ(docId, (MAX_DOC_PER_RECORD - 1) + i * MAX_DOC_PER_RECORD);
        ASSERT_EQ(data[1], 2 * MAX_DOC_PER_RECORD);
        ASSERT_EQ(data[2], i + 1);
        if ((i + 1) % SKIP_MULTIPLIER == 0) {
            level2.push_back(docId);
            ttf2.push_back(ttf);
            offset2.push_back(addr);
            child2.push_back(skipListReader->_readers[0].Tell() -
                             skipListReader->_metas[0].levelBegin);
        }
    }
    ASSERT_EQ(skipListReader->_metas[0].levelEnd, skipListReader->_readers[0].Tell());

    docId = 0;
    ttf = 0;
    addr = 0;
    docid_t level3;
    uint32_t ttf3;
    uint32_t offset3;
    uint32_t child3;
    for (uint32_t i = 0; i < SKIP_MULTIPLIER; ++i) {
        if (i == SKIP_MULTIPLIER - 1) {
            child3 = skipListReader->_readers[1].Tell() - skipListReader->_metas[1].levelBegin;
        }
        uint32_t data[4];
        GroupVarintCompressor::Decode(&skipListReader->_readers[1], 4, data);
        docId += data[0];
        ttf += data[1];
        addr += data[2];
        if (i == SKIP_MULTIPLIER - 1) {
            level3 = docId;
            ttf3 = ttf;
            offset3 = addr;
        }
        ASSERT_EQ(level2[i], docId);
        ASSERT_EQ(ttf2[i], ttf);
        ASSERT_EQ(offset2[i], addr);
        ASSERT_EQ(child2[i], data[3]);
    }
    ASSERT_EQ(skipListReader->_metas[1].levelEnd, skipListReader->_readers[1].Tell());

    uint32_t data[4];
    GroupVarintCompressor::Decode(&skipListReader->_readers[2], 4, data);
    ASSERT_EQ(data[0], level3);
    ASSERT_EQ(data[1], ttf3);
    ASSERT_EQ(data[2], offset3);
    ASSERT_EQ(data[3], child3);
    ASSERT_EQ(skipListReader->_metas[2].levelEnd, skipListReader->_readers[2].Tell());
}

TEST_F(MultiLevelSkipListTest, testSkipToWithTTF) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));

    size_t df = MAX_DOC_PER_RECORD * SKIP_MULTIPLIER * SKIP_MULTIPLIER * (SKIP_MULTIPLIER - 1);
    for (docid_t docid = MAX_DOC_PER_RECORD - 1; docid < df; docid += MAX_DOC_PER_RECORD) {
        skipListBuffer->AddItem(docid, 2 * (docid + 1), 100);
    }

    ASSERT_EQ(3, skipListBuffer->_skipLevel);

    file_system::BufferedFileWriterPtr fileWriter(new file_system::BufferedFileWriter());
    std::string filePath = _rootDir + "/skipto_file";
    ASSERT_EQ(FSEC_OK, fileWriter->Open(filePath, filePath));
    size_t dumpSize = skipListBuffer->EstimateDumpSize();
    skipListBuffer->Dump(fileWriter);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    file_system::BufferedFileReaderPtr fileReader(new file_system::BufferedFileReader());
    ASSERT_EQ(FSEC_OK, fileReader->Open(filePath));
    ASSERT_EQ(dumpSize, fileReader->GetLength());
    indexlib::util::ByteSlice* slice = indexlib::util::ByteSlice::CreateObject(dumpSize);
    ASSERT_EQ(FSEC_OK, fileReader->Read(slice->data, dumpSize, 0, ReadOption()).Code());
    indexlib::util::ByteSliceList sliceList = indexlib::util::ByteSliceList(slice);

    std::shared_ptr<MultiLevelTriValueSkipListReader> skipListReader(
        new MultiLevelTriValueSkipListReader());
    skipListReader->Load(&_byteSlicePool, &sliceList, 0, sliceList.GetTotalSize(),
                         (df - 1) / MAX_POS_PER_RECORD + 1);

    uint32_t key = 0;
    uint32_t prevKey = 0;
    uint32_t value = 0;
    uint32_t delta = 0;
    // case 1
    ASSERT_EQ(skipListReader->SkipTo(0, key, prevKey, value, delta).second, true);
    ASSERT_EQ(key, 127);
    ASSERT_EQ(prevKey, 0);
    ASSERT_EQ(value, 0);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader->GetCurrentTTF(), 256);
    ASSERT_EQ(skipListReader->GetPrevTTF(), 0);
    // case 2
    ASSERT_EQ(skipListReader->SkipTo(MAX_DOC_PER_RECORD + 1, key, prevKey, value, delta).second,
              true);
    ASSERT_EQ(key, MAX_DOC_PER_RECORD * 2 - 1);
    ASSERT_EQ(prevKey, MAX_DOC_PER_RECORD - 1);
    ASSERT_EQ(value, 100);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader->GetCurrentTTF(), (key + 1) * 2);
    ASSERT_EQ(skipListReader->GetPrevTTF(), (key + 1) * 2 - MAX_DOC_PER_RECORD * 2);
    // case 3
    ASSERT_EQ(skipListReader
                  ->SkipTo(MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER) - 20, key,
                           prevKey, value, delta)
                  .second,
              true);
    ASSERT_EQ(key, MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER) - 1);
    ASSERT_EQ(prevKey, MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER - 1) - 1);
    ASSERT_EQ(value, (SKIP_MULTIPLIER * SKIP_MULTIPLIER - 1) * 100);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader->GetCurrentTTF(), (key + 1) * 2);
    ASSERT_EQ(skipListReader->GetPrevTTF(), (key + 1) * 2 - MAX_DOC_PER_RECORD * 2);
    // case 4
    ASSERT_EQ(skipListReader->SkipTo(df, key, prevKey, value, delta).second, false);
}

TEST_F(MultiLevelSkipListTest, testRealtimeSkipToWithTTF) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));
    size_t df = MAX_DOC_PER_RECORD * SKIP_MULTIPLIER * SKIP_MULTIPLIER * (SKIP_MULTIPLIER - 1);
    uint32_t key = 0;
    uint32_t prevKey = 0;
    uint32_t value = 0;
    uint32_t delta = 0;

    // case 1
    std::shared_ptr<MultiLevelTriValueSkipListReader> skipListReader1(
        new MultiLevelTriValueSkipListReader());
    skipListBuffer->SnapShot(skipListReader1.get(), &_byteSlicePool);
    // case 2
    skipListBuffer->AddItem(MAX_DOC_PER_RECORD - 1, 256, 100);
    std::shared_ptr<MultiLevelTriValueSkipListReader> skipListReader2(
        new MultiLevelTriValueSkipListReader());
    skipListBuffer->SnapShot(skipListReader2.get(), &_byteSlicePool);
    // case 3
    std::shared_ptr<MultiLevelTriValueSkipListReader> skipListReader3(
        new MultiLevelTriValueSkipListReader());
    for (docid_t docid = 2 * MAX_DOC_PER_RECORD - 1; docid < df; docid += MAX_DOC_PER_RECORD) {
        skipListBuffer->AddItem(docid, 2 * (docid + 1), 100);
    }
    skipListBuffer->SnapShot(skipListReader3.get(), &_byteSlicePool);

    // assert
    ASSERT_EQ(skipListReader1->SkipTo(0, key, prevKey, value, delta).second, false);
    ASSERT_EQ(skipListReader1->_readers, nullptr);
    ASSERT_EQ(skipListReader1->GetPrevKey(), 0);
    ASSERT_EQ(skipListReader1->GetCurrentKey(), 0);
    ASSERT_EQ(skipListReader1->GetCurrentTTF(), 0);
    ASSERT_EQ(skipListReader1->GetPrevTTF(), 0);
    ASSERT_EQ(skipListReader1->GetLastKeyInBuffer(), 0);
    ASSERT_EQ(skipListReader1->GetLastValueInBuffer(), 0);
    ASSERT_EQ(skipListReader2->SkipTo(0, key, prevKey, value, delta).second, true);
    ASSERT_EQ(key, 127);
    ASSERT_EQ(prevKey, 0);
    ASSERT_EQ(value, 0);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader2->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader2->GetCurrentTTF(), 256);
    ASSERT_EQ(skipListReader2->GetPrevTTF(), 0);
    ASSERT_EQ(skipListReader2->SkipTo(128, key, prevKey, value, delta).second, false);
    ASSERT_EQ(skipListReader3->SkipTo(0, key, prevKey, value, delta).second, true);
    ASSERT_EQ(key, 127);
    ASSERT_EQ(prevKey, 0);
    ASSERT_EQ(value, 0);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader3->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader3->GetCurrentTTF(), 256);
    ASSERT_EQ(skipListReader3->GetPrevTTF(), 0);
    ASSERT_EQ(skipListReader3->SkipTo(129, key, prevKey, value, delta).second, true);
    ASSERT_EQ(key, 255);
    ASSERT_EQ(prevKey, 127);
    ASSERT_EQ(value, 100);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader3->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader3->GetCurrentTTF(), 512);
    ASSERT_EQ(skipListReader3->GetPrevTTF(), 256);
    ASSERT_EQ(skipListReader3
                  ->SkipTo(MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER) - 20, key,
                           prevKey, value, delta)
                  .second,
              true);
    ASSERT_EQ(key, MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER) - 1);
    ASSERT_EQ(prevKey, MAX_DOC_PER_RECORD * (SKIP_MULTIPLIER * SKIP_MULTIPLIER - 1) - 1);
    ASSERT_EQ(value, (SKIP_MULTIPLIER * SKIP_MULTIPLIER - 1) * 100);
    ASSERT_EQ(delta, 100);
    ASSERT_EQ(skipListReader3->GetSkippedItemCount(), (prevKey + 1) / MAX_DOC_PER_RECORD);
    ASSERT_EQ(skipListReader3->GetCurrentTTF(), (key + 1) * 2);
    ASSERT_EQ(skipListReader3->GetPrevTTF(), (key + 1) * 2 - MAX_DOC_PER_RECORD * 2);
    ASSERT_EQ(skipListReader3->SkipTo(df, key, prevKey, value, delta).second, false);
}

TEST_F(MultiLevelSkipListTest, testDiffWithOneLevelSkipList) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));
    std::shared_ptr<BufferedSkipListWriter> skipListBuffer1(
        new BufferedSkipListWriter(&_byteSlicePool, &_bufferPool));
    skipListBuffer1->Init(_docListSkipListFormat.get());

    size_t df = MAX_DOC_PER_RECORD * SKIP_MULTIPLIER * SKIP_MULTIPLIER * SKIP_MULTIPLIER *
                (SKIP_MULTIPLIER / 2);
    size_t itemCount = 0;
    for (docid_t docid = MAX_DOC_PER_RECORD - 1; docid < df; docid += MAX_DOC_PER_RECORD) {
        skipListBuffer->AddItem(docid, 2 * (docid + 1), 100);
        skipListBuffer1->AddItem(docid, 2 * (docid + 1), 100);
        itemCount++;
    }
    skipListBuffer1->FinishFlush();

    file_system::BufferedFileWriterPtr fileWriter(new file_system::BufferedFileWriter());
    std::string filePath = _rootDir + "/multilevel";
    ASSERT_EQ(FSEC_OK, fileWriter->Open(filePath, filePath));
    skipListBuffer->Dump(fileWriter);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    file_system::BufferedFileWriterPtr fileWriter1(new file_system::BufferedFileWriter());
    std::string filePath1 = _rootDir + "/onelevel";
    ASSERT_EQ(FSEC_OK, fileWriter1->Open(filePath1, filePath1));
    skipListBuffer1->Dump(fileWriter1);
    ASSERT_EQ(FSEC_OK, fileWriter1->Close());

    file_system::BufferedFileReaderPtr fileReader(new file_system::BufferedFileReader());
    ASSERT_EQ(FSEC_OK, fileReader->Open(filePath));
    indexlib::util::ByteSlice* slice =
        indexlib::util::ByteSlice::CreateObject(fileReader->GetLength());
    ASSERT_EQ(FSEC_OK,
              fileReader->Read(slice->data, fileReader->GetLength(), 0, ReadOption()).Code());
    indexlib::util::ByteSliceList sliceList = indexlib::util::ByteSliceList(slice);

    std::shared_ptr<MultiLevelTriValueSkipListReader> skipListReader(
        new MultiLevelTriValueSkipListReader());
    skipListReader->Load(&_byteSlicePool, &sliceList, 0, sliceList.GetTotalSize(),
                         (df - 1) / MAX_POS_PER_RECORD + 1);

    file_system::BufferedFileReaderPtr fileReader1(new file_system::BufferedFileReader());
    ASSERT_EQ(FSEC_OK, fileReader1->Open(filePath1));
    indexlib::util::ByteSlice* slice1 =
        indexlib::util::ByteSlice::CreateObject(fileReader1->GetLength());
    ASSERT_EQ(FSEC_OK,
              fileReader1->Read(slice1->data, fileReader1->GetLength(), 0, ReadOption()).Code());
    indexlib::util::ByteSliceList sliceList1 = indexlib::util::ByteSliceList(slice1);

    TriValueSkipListReader* skipListReader1 = new TriValueSkipListReader();
    skipListReader1->Load(&_byteSlicePool, &sliceList1, 0, sliceList1.GetTotalSize(), itemCount);

    auto asseet_func = [&](uint32_t target) {
        uint32_t key = 0;
        uint32_t prevKey = 0;
        uint32_t value = 0;
        uint32_t delta = 0;
        uint32_t key1 = 0;
        uint32_t prevKey1 = 0;
        uint32_t value1 = 0;
        uint32_t delta1 = 0;
        bool success = skipListReader->SkipTo(target, key, prevKey, value, delta).second;
        bool success1 = skipListReader1->SkipTo(target, key1, prevKey1, value1, delta1).second;
        ASSERT_EQ(success, success1);
        if (success) {
            ASSERT_EQ(key, key1);
            ASSERT_EQ(prevKey, prevKey1);
            ASSERT_EQ(value, value1);
            ASSERT_EQ(delta, delta1);
            ASSERT_EQ(skipListReader->GetSkippedItemCount(),
                      skipListReader1->GetSkippedItemCount());
            ASSERT_EQ(skipListReader->GetCurrentTTF(), skipListReader1->GetCurrentTTF());
            ASSERT_EQ(skipListReader->GetPrevTTF(), skipListReader1->GetPrevTTF());
        }
    };

    asseet_func(0);
    asseet_func(129);
    asseet_func(4000);
    asseet_func(57340);
    asseet_func(df);
}

TEST_F(MultiLevelSkipListTest, testRealtimeSkipToEnd) {
    std::shared_ptr<MultiLevelSkipListWriter> skipListBuffer(
        new MultiLevelSkipListWriter(&_byteSlicePool));

    size_t df = MAX_DOC_PER_RECORD * SKIP_MULTIPLIER;
    for (docid_t docid = MAX_DOC_PER_RECORD - 1; docid < df; docid += MAX_DOC_PER_RECORD) {
        skipListBuffer->AddItem(docid, 100);
    }
    ASSERT_EQ(2, skipListBuffer->_skipLevel);

    std::shared_ptr<MultiLevelPairValueSkipListReader> skipListReader(
        new MultiLevelPairValueSkipListReader());
    skipListBuffer->SnapShot(skipListReader.get(), &_byteSlicePool);

    uint32_t key = 0;
    uint32_t prevKey = 0;
    uint32_t value = 0;
    uint32_t delta = 0;
    ASSERT_EQ(skipListReader->SkipTo(df + 3, key, prevKey, value, delta).second, false);
    ASSERT_EQ(skipListReader->GetCurrentKey(), df - 1);
    ASSERT_EQ(skipListReader->GetLastKeyInBuffer(), df - 1);
    ASSERT_EQ(skipListReader->GetLastValueInBuffer(), SKIP_MULTIPLIER * 100);
    ASSERT_EQ(skipListReader->_skippedItemCount, SKIP_MULTIPLIER);
    ASSERT_EQ(key, df - 1);
    ASSERT_EQ(prevKey, df - 1);
    ASSERT_EQ(value, SKIP_MULTIPLIER * 100);
    ASSERT_EQ(delta, 0);
}

} // namespace indexlib::index

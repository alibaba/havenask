#include "indexlib/index/kkv/test/kkv_index_dumper_unittest.h"

#include "indexlib/common/chunk/chunk_define.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common/hash_table/hash_table_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_rewriter/pack_attribute_rewriter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/kkv/prefix_key_table_creator.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"

using namespace std;

using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVIndexDumperTest);

KKVIndexDumperTest::KKVIndexDumperTest() {}

KKVIndexDumperTest::~KKVIndexDumperTest() {}

void KKVIndexDumperTest::CaseSetUp()
{
    string fields = "single_int16:int16;single_int32:int32;single_int64:int64;"
                    "pkey:uint64;skey:uint64";
    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int16;single_int32;single_int64");

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    PKeyTableType tableType = GET_CASE_PARAM();
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    mKkvConfig->GetIndexPreference().SetHashDictParam(param);

    mFormatter.Init(mKkvConfig->GetValueConfig()->CreatePackAttributeConfig());
    mQuotaControl.reset(new util::QuotaControl(1024 * 1024 * 1024));

    // 注意: case依赖了value区变长的假设
    ASSERT_FALSE(mKkvConfig->GetValueConfig()->IsCompactFormatEnabled());
    ASSERT_EQ(-1, mKkvConfig->GetValueConfig()->GetFixedLength());
}

void KKVIndexDumperTest::CaseTearDown() {}

void KKVIndexDumperTest::TestSimpleProcess()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    KKVSegmentWriter<uint64_t> segWriter(mSchema, options);
    BuildingSegmentData segmentData;
    segmentData.SetSegmentId(0);
    std::shared_ptr<indexlib::framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    segWriter.Init(segmentData, metrics, mQuotaControl);
    size_t pkeyCount = 29;
    size_t skeyCount = BuildDocs(segWriter, pkeyCount);

    SegmentInfoPtr segmentInfo = segWriter.GetSegmentInfo();
    ASSERT_EQ(skeyCount, segmentInfo->docCount);

    file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(0);
    segWriter.EndSegment();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    segWriter.CreateDumpItems(directory, dumpItems);

    ASSERT_EQ((size_t)1, dumpItems.size());
    auto dumpItem = dumpItems[0].release();
    dumpItem->process(&mPool);
    delete dumpItem;

    file_system::DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, false);
    file_system::DirectoryPtr dumpDir = indexDir->GetDirectory(mKkvConfig->GetIndexName(), false);
    ASSERT_TRUE(dumpDir.operator bool());
    CheckData(dumpDir, pkeyCount);
}

size_t KKVIndexDumperTest::MakeDocs(size_t pkeyCount, std::string& docStr)
{
    size_t skeyCount = 0;
    stringstream ss;
    for (size_t i = 1; i <= pkeyCount; ++i) {
        for (size_t j = 1; j <= i; ++j) {
            ss << "cmd=add,single_int16=" << skeyCount << ","
               << "single_int32=" << skeyCount + 1 << ","
               << "single_int64=" << skeyCount + 2 << ","
               << "pkey=" << i << ","
               << "skey=" << j << ","
               << "ts=" << (i + j) * 1000000 << ";";
            ++skeyCount;
        }
    }
    docStr = ss.str();
    return skeyCount;
}

size_t KKVIndexDumperTest::BuildDocs(KKVSegmentWriter<uint64_t>& writer, size_t pkeyCount)
{
    string docStr;
    size_t skeyCount = MakeDocs(pkeyCount, docStr);
    auto docs = test::DocumentCreator::CreateKVDocuments(mSchema, docStr);
    assert(skeyCount == docs.size());
    for (size_t i = 0; i < docs.size(); i++) {
        writer.AddDocument(docs[i]);
    }
    return skeyCount;
}

void KKVIndexDumperTest::CheckData(const DirectoryPtr& dir, size_t pkeyCount)
{
    FileReaderPtr valueFile = dir->CreateFileReader(KKV_VALUE_FILE_NAME, FSOT_MEM);
    vector<SKeyNode> skeyNodes;
    CheckValue(valueFile, pkeyCount, skeyNodes);

    FileReaderPtr skeyFile = dir->CreateFileReader(SUFFIX_KEY_FILE_NAME, FSOT_MEM);
    vector<OnDiskPKeyOffset> pkeyOffsets;
    CheckSkey(skeyFile, pkeyCount, skeyNodes, pkeyOffsets);
    CheckPkey(dir, pkeyOffsets);
}

void KKVIndexDumperTest::CheckValue(const FileReaderPtr& valueFile, size_t pkeyCount, vector<SKeyNode>& skeyNodes)
{
    size_t chunkOffset = 0;
    size_t inChunkOffset = 0;
    ChunkMeta curChunkMeta;
    char* readAddr = (char*)valueFile->GetBaseAddress();
    curChunkMeta = *(ChunkMeta*)readAddr;
    readAddr += sizeof(ChunkMeta);
    ASSERT_TRUE(curChunkMeta.isEncoded == 0);

    uint32_t valueDataLen = sizeof(int16_t) + sizeof(int32_t) + sizeof(int64_t);
    uint32_t valueLen = sizeof(uint8_t) + // first one byte is encoded count
                        valueDataLen;
    int16_t vInt16 = 0;
    int32_t vInt32 = 0;
    int64_t vInt64 = 0;
    size_t skeyCount = 0;
    for (size_t i = 1; i <= pkeyCount; ++i) {
        for (size_t j = 1; j <= i; ++j) {
            // expect value
            vInt16 = skeyCount;
            vInt32 = skeyCount + 1;
            vInt64 = skeyCount + 2;

            if (inChunkOffset == curChunkMeta.length) {
                // read new chunk
                chunkOffset += (inChunkOffset + sizeof(ChunkMeta));
                size_t paddingSize = (chunkOffset + 7) / 8 * 8 - chunkOffset;
                chunkOffset += paddingSize;
                readAddr += paddingSize;

                curChunkMeta = *(ChunkMeta*)readAddr;
                readAddr += sizeof(ChunkMeta);
                inChunkOffset = 0;
                ASSERT_TRUE(curChunkMeta.isEncoded == 0);
            }

            SKeyNode skeyNode;
            skeyNode.skeyNode.SetSKeyNodeInfo(j, (i + j), chunkOffset / 8, inChunkOffset);
            skeyNode.skeyNode.SetLastNode(j == i);
            skeyNode.timestamp = i + j;
            skeyNodes.push_back(skeyNode);
            assert(inChunkOffset < curChunkMeta.length);

            // check encoded count
            ASSERT_EQ(valueDataLen, *(int8_t*)readAddr);
            readAddr += sizeof(int8_t);

            // check value
            ASSERT_EQ(vInt16, *(int16_t*)readAddr);
            readAddr += sizeof(int16_t);
            ASSERT_EQ(vInt32, *(int32_t*)readAddr);
            readAddr += sizeof(int32_t);
            ASSERT_EQ(vInt64, *(int64_t*)readAddr);
            readAddr += sizeof(int64_t);

            inChunkOffset += valueLen;
            ++skeyCount;
        }
    }
    ASSERT_EQ(valueFile->GetLength(), chunkOffset + inChunkOffset + sizeof(ChunkMeta));
}

void KKVIndexDumperTest::CheckSkey(const FileReaderPtr& skeyFile, size_t pkeyCount, const vector<SKeyNode>& skeyNodes,
                                   vector<OnDiskPKeyOffset>& pkeyOffsets)
{
    size_t chunkOffset = 0;
    size_t inChunkOffset = 0;
    ChunkMeta curChunkMeta;
    char* readAddr = (char*)skeyFile->GetBaseAddress();
    curChunkMeta = *(ChunkMeta*)readAddr;
    readAddr += sizeof(ChunkMeta);
    ASSERT_TRUE(curChunkMeta.isEncoded == 0);

    size_t skeyCount = 0;
    for (size_t i = 1; i <= pkeyCount; ++i) {
        for (size_t j = 1; j <= i; ++j) {
            // read new chunk
            if (inChunkOffset == curChunkMeta.length) {
                chunkOffset += (inChunkOffset + sizeof(ChunkMeta));
                curChunkMeta = *(ChunkMeta*)readAddr;
                readAddr += sizeof(ChunkMeta);
                inChunkOffset = 0;
                ASSERT_TRUE(curChunkMeta.isEncoded == 0);
            }

            if (j == 1) {
                pkeyOffsets.push_back(OnDiskPKeyOffset(chunkOffset, inChunkOffset));
            }

            SKeyNode& skeyNode = *(SKeyNode*)readAddr;
            ASSERT_EQ(skeyNodes[skeyCount], skeyNode);
            readAddr += sizeof(SKeyNode);
            inChunkOffset += sizeof(SKeyNode);
            skeyCount++;
        }
    }
    ASSERT_EQ(skeyCount, skeyNodes.size());
    ASSERT_EQ(skeyFile->GetLength(), chunkOffset + inChunkOffset + sizeof(ChunkMeta));
}

void KKVIndexDumperTest::CheckPkey(const DirectoryPtr& dir, const vector<OnDiskPKeyOffset>& pkeyOffsets)
{
    typedef PrefixKeyTableBase<OnDiskPKeyOffset> PKeyTable;
    unique_ptr<PKeyTable> table(
        PrefixKeyTableCreator<OnDiskPKeyOffset>::Create(dir, mKkvConfig->GetIndexPreference(), PKOT_READ));
    for (size_t i = 1; i <= pkeyOffsets.size(); ++i) {
        OnDiskPKeyOffset* offset = table->Find((uint64_t)i);
        ASSERT_TRUE(offset != NULL);
        ASSERT_EQ(pkeyOffsets[i - 1], *offset);
    }
    ASSERT_EQ((uint32_t)pkeyOffsets.size(), table->Size());
}
}} // namespace indexlib::index

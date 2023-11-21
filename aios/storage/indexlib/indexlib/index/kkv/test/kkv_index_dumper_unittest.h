#pragma once

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/on_disk_skey_decoder.h"
#include "indexlib/index/kkv/on_disk_skey_node.h"
#include "indexlib/partition/segment/kkv_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KKVIndexDumperTest : public INDEXLIB_TESTBASE_WITH_PARAM<PKeyTableType>
{
public:
    KKVIndexDumperTest();
    ~KKVIndexDumperTest();

    DECLARE_CLASS_NAME(KKVIndexDumperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    struct LegacySKeyNode {
        NormalOnDiskSKeyNode<uint64_t> skeyNode;
        uint32_t timestamp;
        inline bool operator==(const LegacySKeyNode& other) const
        {
            return skeyNode == other.skeyNode && timestamp == other.timestamp;
        }
    } __attribute__((packed));
    typedef LegacySKeyNode SKeyNode;

    size_t BuildDocs(partition::KKVSegmentWriter<uint64_t>& writer, size_t pkeyCount);

    size_t MakeDocs(size_t pkeyCount, std::string& docStr);

    void CheckData(const file_system::DirectoryPtr& dir, size_t pkeyCount);

    void CheckValue(const file_system::FileReaderPtr& valueFile, size_t pkeyCount, std::vector<SKeyNode>& skeyNodes);

    void CheckSkey(const file_system::FileReaderPtr& skeyFile, size_t pkeyCount, const std::vector<SKeyNode>& skeyNodes,
                   std::vector<OnDiskPKeyOffset>& pkeyOffsets);

    void CheckPkey(const file_system::DirectoryPtr& dir, const std::vector<OnDiskPKeyOffset>& pkeyOffsets);

private:
    config::IndexPartitionSchemaPtr mSchema;
    common::PackAttributeFormatter mFormatter;
    autil::mem_pool::Pool mPool;
    config::KKVIndexConfigPtr mKkvConfig;
    util::QuotaControlPtr mQuotaControl;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(p1, KKVIndexDumperTest,
                        testing::Values(index::PKT_DENSE, index::PKT_CUCKOO, index::PKT_SEPARATE_CHAIN));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVIndexDumperTest, TestSimpleProcess);
}} // namespace indexlib::index

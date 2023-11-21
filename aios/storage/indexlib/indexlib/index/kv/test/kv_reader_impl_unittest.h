#pragma once

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/index/kv/kv_reader_impl.h"
#include "indexlib/index/kv/kv_segment_reader_impl_base.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class FakeSegmentReader final : public KVSegmentReaderImplBase
{
public:
    FakeSegmentReader() : mKey(0), mValueTs(0), mIsDeleted(true) {}
    void Open(const config::KVIndexConfigPtr& kvIndexConfig, const index_base::SegmentData& segData) {}

    FL_LAZY(bool)
    Get(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t& ts, bool& isDeleted,
        autil::mem_pool::Pool* pool, KVMetricsCollector* collector = nullptr) const
    {
        if (key != mKey) {
            FL_CORETURN false;
        }

        value = mValue;
        ts = mValueTs;
        isDeleted = mIsDeleted;
        FL_CORETURN true;
    }

    void SetKeyValue(const std::string& key, const std::string& value, const std::string& isDeleted,
                     const std::string& valueTs)
    {
        mKey = autil::StringUtil::numberFromString<uint64_t>(key);
        mStrValue = value;
        mValue = autil::StringView(mStrValue.c_str(), mStrValue.size());
        if (isDeleted == "true") {
            mIsDeleted = true;
        } else {
            mIsDeleted = false;
        }
        mValueTs = autil::StringUtil::numberFromString<uint64_t>(valueTs);
    }
    bool doCollectAllCode() { return true; }

private:
    uint64_t mKey;
    uint64_t mValueTs;
    bool mIsDeleted;
    autil::StringView mValue;
    std::string mStrValue;
};
class KVReaderImplTest : public INDEXLIB_TESTBASE
{
public:
    KVReaderImplTest();
    ~KVReaderImplTest();

    DECLARE_CLASS_NAME(KVReaderImplTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGet();
    void TestMultiInMemSegments();

private:
    template <typename Reader>
    void PrepareSegmentReader(const std::string& offlineReaderStr, const std::string& onlineReaderStr, Reader& reader);
    void InnerTestGet(const std::string& offlineValues, const std::string& onlineValues, uint64_t ttl,
                      uint64_t incTimestamp, uint64_t key, uint64_t searchTs, bool success,
                      const std::string& expectValue);

    void CheckReaderValue(const KVReaderPtr& reader, const std::string& key, uint64_t value);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KVReaderImplTest, TestGet);
INDEXLIB_UNIT_TEST_CASE(KVReaderImplTest, TestMultiInMemSegments);
}} // namespace indexlib::index

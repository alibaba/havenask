#pragma once

#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

using namespace std;

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::table {

class FakeSegmentReader final : public indexlibv2::index::IKVSegmentReader
{
public:
    FakeSegmentReader() : mKey(0), mValueTs(0), mIsDeleted(true) {}
    Status Open(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory)
    {
        return Status::OK();
    }

    FL_LAZY(indexlib::util::Status)
    Get(index::keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool,
        index::KVMetricsCollector* collector = nullptr,
        autil::TimeoutTerminator* timeoutTerminator = nullptr) const override
    {
        if (key != mKey) {
            FL_CORETURN indexlib::util::NOT_FOUND;
            ;
        }

        value = mValue;
        ts = mValueTs;
        if (mIsDeleted) {
            FL_CORETURN indexlib::util::DELETED;
        } else {
            FL_CORETURN indexlib::util::OK;
        }
    }

    std::unique_ptr<index::IKVIterator> CreateIterator() override { return nullptr; }

    size_t EvaluateCurrentMemUsed() override { return 0; }

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

private:
    uint64_t mKey;
    uint64_t mValueTs;
    bool mIsDeleted;
    autil::StringView mValue;
    std::string mStrValue;
};

} // namespace indexlibv2::table

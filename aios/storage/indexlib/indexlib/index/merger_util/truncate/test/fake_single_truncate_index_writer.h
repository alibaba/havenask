#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/single_truncate_index_writer.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class FakeSingleTruncateIndexWriter : public SingleTruncateIndexWriter
{
public:
    FakeSingleTruncateIndexWriter(const config::IndexConfigPtr& indexConfig, const ReclaimMapPtr& reclaimMap);
    ~FakeSingleTruncateIndexWriter();

public:
    void InsertTruncateDictKey(const index::DictKeyInfo& dictKey);
    void SetLastDocValue(std::string value);
    void AcquireLastDocValue(const PostingIteratorPtr& postingIt, std::string& value);
    void SetDictValue(dictvalue_t dictValue);
    dictvalue_t GetDictValue(const std::shared_ptr<PostingWriter>& postingWriter, dictvalue_t value);

private:
    std::string mLastDocValue;
    dictvalue_t mDictValue;

private:
    friend class SingleTruncateIndexWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeSingleTruncateIndexWriter);
} // namespace indexlib::index::legacy

#include "indexlib/index/merger_util/truncate/test/fake_single_truncate_index_writer.h"

using namespace std;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, FakeSingleTruncateIndexWriter);

FakeSingleTruncateIndexWriter::FakeSingleTruncateIndexWriter(const config::IndexConfigPtr& indexConfig,
                                                             const ReclaimMapPtr& reclaimMap)
    : SingleTruncateIndexWriter(indexConfig, reclaimMap)
{
}

FakeSingleTruncateIndexWriter::~FakeSingleTruncateIndexWriter() {}

void FakeSingleTruncateIndexWriter::InsertTruncateDictKey(const index::DictKeyInfo& dictKey)
{
    if (dictKey.IsNull()) {
        mHasNullTerm = true;
    } else {
        mTruncateDictKeySet.insert(dictKey.GetKey());
    }
}

void FakeSingleTruncateIndexWriter::SetLastDocValue(std::string value) { mLastDocValue = value; }

void FakeSingleTruncateIndexWriter::AcquireLastDocValue(const std::shared_ptr<PostingIterator>& postingIt,
                                                        std::string& value)
{
    value = mLastDocValue;
}

void FakeSingleTruncateIndexWriter::SetDictValue(dictvalue_t dictValue) { mDictValue = dictValue; }

dictvalue_t FakeSingleTruncateIndexWriter::GetDictValue(const shared_ptr<PostingWriter>& postingWriter,
                                                        dictvalue_t value)
{
    return mDictValue;
}
} // namespace indexlib::index::legacy

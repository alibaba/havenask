#include "indexlib/index/normal/inverted_index/truncate/test/fake_single_truncate_index_writer.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, FakeSingleTruncateIndexWriter);

FakeSingleTruncateIndexWriter::FakeSingleTruncateIndexWriter(
    const config::IndexConfigPtr& indexConfig,
    const ReclaimMapPtr& reclaimMap)
    : SingleTruncateIndexWriter(indexConfig, reclaimMap)
{
}

FakeSingleTruncateIndexWriter::~FakeSingleTruncateIndexWriter() 
{
}

void FakeSingleTruncateIndexWriter::InsertTruncateDictKey(dictkey_t dictKey)
{
    mTruncateDictKeySet.insert(dictKey);
}

void FakeSingleTruncateIndexWriter::SetLastDocValue(std::string value)
{
    mLastDocValue = value;
}

void FakeSingleTruncateIndexWriter::AcquireLastDocValue(
        const PostingIteratorPtr& postingIt, std::string& value)
{
    value = mLastDocValue;
}

void FakeSingleTruncateIndexWriter::SetDictValue(dictvalue_t dictValue)
{
    mDictValue = dictValue;
}

dictvalue_t FakeSingleTruncateIndexWriter::GetDictValue(
        const PostingWriterPtr& postingWriter, dictvalue_t value)
{
    return mDictValue;
}


IE_NAMESPACE_END(index);


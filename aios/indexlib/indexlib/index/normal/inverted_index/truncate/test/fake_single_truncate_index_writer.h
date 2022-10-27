#ifndef __INDEXLIB_FAKE_SINGLE_TRUNCATE_INDEX_WRITER_H
#define __INDEXLIB_FAKE_SINGLE_TRUNCATE_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/single_truncate_index_writer.h"

IE_NAMESPACE_BEGIN(index);

class FakeSingleTruncateIndexWriter : public SingleTruncateIndexWriter
{
public:
    FakeSingleTruncateIndexWriter(const config::IndexConfigPtr& indexConfig,
                                  const ReclaimMapPtr& reclaimMap);
    ~FakeSingleTruncateIndexWriter();

public:
    void InsertTruncateDictKey(dictkey_t dictKey);
    void SetLastDocValue(std::string value);
    void AcquireLastDocValue(const PostingIteratorPtr& postingIt, std::string& value);
    void SetDictValue(dictvalue_t dictValue);
    dictvalue_t GetDictValue(const PostingWriterPtr& postingWriter,
                             dictvalue_t value);

private:
    std::string mLastDocValue;
    dictvalue_t mDictValue;

private:
    friend class SingleTruncateIndexWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeSingleTruncateIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FAKE_SINGLE_TRUNCATE_INDEX_WRITER_H

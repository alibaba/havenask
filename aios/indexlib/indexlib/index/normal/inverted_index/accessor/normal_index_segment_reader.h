#ifndef __INDEXLIB_NORMAL_INDEX_SEGMENT_READER_H
#define __INDEXLIB_NORMAL_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include <future_lite/Future.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index_base/segment/segment_data.h"

DECLARE_REFERENCE_CLASS(index, DictionaryReader);
DECLARE_REFERENCE_CLASS(file_system, FileReader);

IE_NAMESPACE_BEGIN(index);

class NormalIndexSegmentReader : public IndexSegmentReader
{
public:
    NormalIndexSegmentReader();
    ~NormalIndexSegmentReader();

public:
    virtual void Open(const config::IndexConfigPtr& indexConfig,
                      const index_base::SegmentData& segmentData);
    // TODO: remove arg baseDocId
    bool GetSegmentPosting(dictkey_t key, docid_t baseDocId,
                           index::SegmentPosting &segPosting,
                           autil::mem_pool::Pool* sessionPool) const;

    future_lite::Future<bool> GetSegmentPostingAsync(dictkey_t key, docid_t baseDocId,
        index::SegmentPosting& segPosting, autil::mem_pool::Pool* sessionPool) const;

    const index::DictionaryReaderPtr& GetDictionaryReader() const
    { return mDictReader; }

    const index_base::SegmentData& GetSegmentData()
    { return mSegmentData; }
protected:
    void GetSegmentPosting(dictvalue_t value,
                           index::SegmentPosting &segPosting) const;

    future_lite::Future<future_lite::Unit> GetSegmentPostingAsync(
        dictvalue_t value, index::SegmentPosting& segPosting) const;
    void Open(const config::IndexConfigPtr& indexConfig,
            const file_system::DirectoryPtr& directory);

    virtual index::DictionaryReader* CreateDictionaryReader(
        const config::IndexConfigPtr& indexConfig);

protected:
    index::DictionaryReaderPtr mDictReader;
    file_system::FileReaderPtr mPostingReader;
    IndexFormatOption mOption;
    index_base::SegmentData mSegmentData;

private:
    friend class NormalIndexSegmentReaderTest;
    friend class NormalIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalIndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NORMAL_INDEX_SEGMENT_READER_H

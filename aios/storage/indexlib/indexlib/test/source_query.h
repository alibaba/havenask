#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"

namespace indexlib { namespace test {

class SourceQuery : public Query
{
public:
    SourceQuery();
    ~SourceQuery();

public:
    bool Init(index::PostingIterator* iter, std::string indexName, index::sourcegroupid_t groupId);
    index::sourcegroupid_t GetGroupId() const;
    virtual docid_t Seek(docid_t docid) override;
    virtual pos_t SeekPosition(pos_t pos) override;
    std::string GetIndexName() const;

private:
    index::PostingIterator* mIter;
    index::sourcegroupid_t mGroupId;
    std::string mIndexName;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceQuery);
}} // namespace indexlib::test

/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_FAKE_POSTING_ITERATOR_H
#define __INDEXLIB_FAKE_POSTING_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/indexlib.h"
#include "indexlib/testlib/fake_in_doc_section_meta.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlib { namespace testlib {

class FakeInDocPositionState;
class FakePostingIterator : public index::PostingIterator
{
public:
    struct Posting {
        docid_t docid;
        std::vector<pos_t> occArray;
        std::vector<int32_t> fieldBitArray;
        uint16_t docPayload;
        std::vector<sectionid_t> sectionIdArray;
    };

    typedef std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta> DocSectionMap;
    typedef std::map<std::pair<int32_t, sectionid_t>, FakeInDocSectionMeta::SectionInfo> FieldAndSectionMap;
    typedef std::vector<Posting> Postings;
    typedef std::pair<uint16_t, Postings> RichPostings;
    typedef std::map<std::string, RichPostings> Map;
    typedef std::map<std::string, index::PostingIteratorType> IteratorTypeMap;
    typedef std::map<std::string, bool> PositionMap;

public:
    FakePostingIterator(const RichPostings& p, uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool = NULL);
    virtual ~FakePostingIterator();

    index::TermMeta* GetTermMeta() const override { return _termMeta; }
    index::TermMeta* GetTruncateTermMeta() const override { return _truncateTermMeta; }
    void setTermMeta(index::TermMeta* termMeta) { *_termMeta = *termMeta; }

    docid_t SeekDoc(docid_t id) override;
    index::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override
    {
        result = SeekDoc(docId);
        return index::ErrorCode::OK;
    }
    void Unpack(index::TermMatchData& tmd) override;

    index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextPos) override;
    bool HasPosition() const override { return _hasPostion; }
    void setPosition(bool pos) { _hasPostion = pos; }
    index::PostingIteratorType GetType() const override { return _type; }
    void setType(index::PostingIteratorType type) { _type = type; }

    PostingIterator* Clone() const override
    {
        FakePostingIterator* iter =
            POOL_COMPATIBLE_NEW_CLASS(_sessionPool, FakePostingIterator, _richPostings, _statePoolSize, _sessionPool);
        iter->setType(_type);
        iter->setPosition(_pos);
        return iter;
    }

    autil::mem_pool::Pool* GetSessionPool() const override { return _sessionPool; }

    void Reset() override { _pos = -1; }

protected:
    RichPostings _richPostings;
    util::ObjectPool<FakeInDocPositionState> _objectPool;
    uint32_t _statePoolSize;
    index::PostingIteratorType _type;
    int32_t _pos;
    int32_t _occPos;
    bool _hasPostion;
    index::TermMeta* _termMeta;
    index::TermMeta* _truncateTermMeta;
    autil::mem_pool::Pool* _sessionPool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakePostingIterator);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_POSTING_ITERATOR_H

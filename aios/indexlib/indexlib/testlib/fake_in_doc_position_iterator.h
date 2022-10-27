#ifndef __INDEXLIB_FAKE_IN_DOC_POSITION_ITERATOR_H
#define __INDEXLIB_FAKE_IN_DOC_POSITION_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/error_code.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator.h"

IE_NAMESPACE_BEGIN(testlib);

class FakeInDocPositionIterator : public index::InDocPositionIterator
{
public:
    FakeInDocPositionIterator(const std::vector<pos_t> &occArray,
                              const std::vector<int32_t> &fieldBitArray,
                              const std::vector<sectionid_t> &sectionIdArray);
    virtual ~FakeInDocPositionIterator();
public:
    common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos);
    pospayload_t GetPosPayload();

    virtual sectionid_t GetSectionId();
    virtual section_len_t GetSectionLength();
    virtual section_weight_t GetSectionWeight();
    virtual fieldid_t GetFieldId();
    virtual int32_t GetFieldPosition();
    virtual bool HasPosPayload() const;
private:
    std::vector<pos_t> _occArray;
    std::vector<int32_t> _fieldBitArray;
    std::vector<sectionid_t> _sectionIdArray;
    int32_t _offset;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeInDocPositionIterator);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_IN_DOC_POSITION_ITERATOR_H

#ifndef ISEARCH_FAKEINDOCPOSITIONITERATOR_H
#define ISEARCH_FAKEINDOCPOSITIONITERATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/index/index.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator.h>

IE_NAMESPACE_BEGIN(index);

class FakeInDocPositionIterator : public InDocPositionIterator
{
public:
    //FakeInDocPositionIterator(const std::vector<pos_t> &occArray,
     //                         const std::vector<int32_t> &fieldBitArray);
    FakeInDocPositionIterator(const std::vector<pos_t> &occArray,
        const std::vector<int32_t> &fieldBitArray,
        const std::vector<sectionid_t> &fieldBitArray2);
    virtual ~FakeInDocPositionIterator();
public:
    pos_t SeekPosition(pos_t pos);
    common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos)
    {
        nextpos = SeekPosition(pos);
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }

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
    HA3_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEINDOCPOSITINGITERATOR_H

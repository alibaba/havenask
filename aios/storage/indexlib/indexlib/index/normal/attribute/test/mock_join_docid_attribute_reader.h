#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class MockJoinDocIdAttributeReader : public JoinDocidAttributeReader
{
public:
    MockJoinDocIdAttributeReader(std::map<docid_t, docid_t> joinedBaseDocIds) : _joinedBaseDocIds(joinedBaseDocIds) {}
    ~MockJoinDocIdAttributeReader() {}

public:
    docid_t GetJoinDocId(docid_t docId) const override
    {
        docid_t joinDocId = INVALID_DOCID;
        if (std::find(_joinedBaseDocIds, docId) != _joinedBaseDocIds.end()) {
            joinDocId = _joinedBaseDocIds.at(docId);
        }
        return joinDocId;
    }

private:
    std::map<docid_t, docid_t> _joinedBaseDocIds;
};
}} // namespace indexlib::index

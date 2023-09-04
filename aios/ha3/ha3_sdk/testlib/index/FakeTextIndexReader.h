#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "ha3_sdk/testlib/index/FakeInDocSectionMeta.h"
#include "ha3_sdk/testlib/index/FakeSectionAttributeReader.h"
#include "ha3_sdk/testlib/index/FakeTopIndexReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace index {
class PostingIterator;
class SectionAttributeReader;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

class FakeTextIndexReader : public FakeTopIndexReader {
public:
    struct Posting {
        Posting()
            : docid(-1)
            , docPayload(0) {}
        docid_t docid;
        std::vector<pos_t> occArray;
        std::vector<int32_t> fieldBitArray;
        uint16_t docPayload;
        std::vector<sectionid_t> sectionIdArray;
    };

    typedef std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta> DocSectionMap;
    typedef std::map<std::pair<int32_t, sectionid_t>, FakeInDocSectionMeta::SectionInfo>
        FieldAndSectionMap;
    typedef std::vector<Posting> Postings;
    typedef std::pair<uint16_t, Postings> RichPostings;
    typedef std::map<std::string, RichPostings> Map;
    typedef std::map<std::string, PostingIteratorType> IteratorTypeMap;
    typedef std::map<std::string, bool> PositionMap;

    FakeTextIndexReader(const std::string &mpStr,
                        indexlib::index::ErrorCode seekRet = indexlib::index::ErrorCode::OK);
    FakeTextIndexReader(const std::string &mpStr, const PositionMap &posMap);

    index::Result<PostingIterator *> Lookup(const Term &term,
                                            uint32_t statePoolSize = 1000,
                                            PostingType type = pt_default,
                                            autil::mem_pool::Pool *sessionPool = NULL);

    virtual const SectionAttributeReader *GetSectionReader(const std::string &indexName) const;

    void setType(const std::string &term, PostingIteratorType type) {
        _typeMap[term] = type;
    }

    void setPostitionMap(const PositionMap &posMap) {
        _posMap = posMap;
    }

private:
    Map _map;
    DocSectionMap _docSectionMap;
    IteratorTypeMap _typeMap;
    PositionMap _posMap;
    mutable FakeSectionAttributeReaderPtr _ptr;
    indexlib::index::ErrorCode _seekRet;
};

typedef std::shared_ptr<FakeTextIndexReader> FakeTextIndexReaderPtr;

} // namespace index
} // namespace indexlib

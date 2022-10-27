#ifndef ISEARCH_INDEX_FAKETEXTINDEXREADER_H
#define ISEARCH_INDEX_FAKETEXTINDEXREADER_H

#include <ha3/index/index.h>
#include <vector>
#include <map>
#include <indexlib/common_define.h>
#include <indexlib/common/error_code.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3_sdk/testlib/index/FakeTopIndexReader.h>
#include <ha3/isearch.h>
#include <ha3_sdk/testlib/index/FakeInDocSectionMeta.h>
#include <ha3_sdk/testlib/index/FakeSectionAttributeReader.h>


IE_NAMESPACE_BEGIN(index);

class FakeTextIndexReader : public FakeTopIndexReader
{
public:
    struct Posting
    {
        Posting()
            : docid(-1)
            , docPayload(0) {
        }
        docid_t docid;
        std::vector<pos_t> occArray;
        std::vector<int32_t> fieldBitArray;
        uint16_t docPayload;
        std::vector<sectionid_t> sectionIdArray;
    };
    
    typedef std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta> DocSectionMap;
    typedef std::map<std::pair<int32_t, sectionid_t>,
        FakeInDocSectionMeta::SectionInfo> FieldAndSectionMap;
    typedef std::vector<Posting> Postings;
    typedef std::pair<uint16_t,Postings> RichPostings;
    typedef std::map<std::string, RichPostings> Map;
    typedef std::map<std::string, PostingIteratorType> IteratorTypeMap;
    typedef std::map<std::string, bool> PositionMap;

    FakeTextIndexReader(const std::string &mpStr,
                        IE_NAMESPACE(common)::ErrorCode seekRet = IE_NAMESPACE(common)::ErrorCode::OK);
    FakeTextIndexReader(const std::string &mpStr, const PositionMap &posMap);

    PostingIterator *Lookup(const common::Term& term,
                            uint32_t statePoolSize = 1000,
                            PostingType type = pt_default,
                            autil::mem_pool::Pool *sessionPool = NULL);

    virtual const SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const;

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
    IE_NAMESPACE(common)::ErrorCode _seekRet;
};

HA3_TYPEDEF_PTR(FakeTextIndexReader);

IE_NAMESPACE_END(index);
#endif

#ifndef ISEARCH_FAKEJOINDOCIDCONVERTER_H
#define ISEARCH_FAKEJOINDOCIDCONVERTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/JoinDocIdConverter.h>

BEGIN_HA3_NAMESPACE(search);

class FakeJoinDocIdConverter : public JoinDocIdConverterBase
{
public:
    FakeJoinDocIdConverter(const std::string &joinAttrName = "fake_attr")
        : JoinDocIdConverterBase(joinAttrName)
    { }
    ~FakeJoinDocIdConverter() {}
private:
    FakeJoinDocIdConverter(const FakeJoinDocIdConverter &);
    FakeJoinDocIdConverter& operator=(const FakeJoinDocIdConverter &);
public:
    docid_t convert(matchdoc::MatchDoc matchDoc) override {
        assert(matchdoc::INVALID_MATCHDOC != matchDoc);
        docid_t mainDocId = matchDoc.getDocId();
        std::map<docid_t, docid_t>::const_iterator iter =
            _docIdMap.find(mainDocId);
        if (iter == _docIdMap.end()) {
            return INVALID_DOCID;
        }
        return iter->second;
    }
public:
    void setDocIdMap(const std::map<docid_t, docid_t> &map) {
        _docIdMap = map;
    }
    void setSubDocIdRef(matchdoc::Reference<matchdoc::MatchDoc> *subDocIdRef) {
        _subDocIdRef = subDocIdRef;
    }
private:
    std::map<docid_t, docid_t> _docIdMap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeJoinDocIdConverter);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEJOINDOCIDCONVERTER_H

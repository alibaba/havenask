#ifndef ISEARCH_FAKEJOINDOCIDREADER_H
#define ISEARCH_FAKEJOINDOCIDREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/partition/join_cache/join_docid_reader_base.h>

BEGIN_HA3_NAMESPACE(search);

class FakeJoinDocidReader : public IE_NAMESPACE(partition)::JoinDocidReaderBase
{
public:
    FakeJoinDocidReader()
    { }
    ~FakeJoinDocidReader() {}
private:
    FakeJoinDocidReader(const FakeJoinDocidReader &);
    FakeJoinDocidReader& operator=(const FakeJoinDocidReader &);
public:
    /*override*/ docid_t GetAuxDocid(docid_t mainDocid) {
        std::map<docid_t, docid_t>::const_iterator iter =
            _docIdMap.find(mainDocid);
        if (iter == _docIdMap.end()) {
            return INVALID_DOCID;
        }
        return iter->second;
    }
public:
    void setDocIdMap(const std::map<docid_t, docid_t> &map) {
        _docIdMap = map;
    }

private:
    std::map<docid_t, docid_t> _docIdMap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeJoinDocidReader);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEJOINDOCIDREADER_H

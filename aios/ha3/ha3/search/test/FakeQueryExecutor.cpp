#include <ha3/search/test/FakeQueryExecutor.h>
#include <ha3/rank/MatchData.h>

IE_NAMESPACE_USE(index);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeQueryExecutor);

FakeQueryExecutor::FakeQueryExecutor() { 
}

FakeQueryExecutor::~FakeQueryExecutor() { 
}
const std::string FakeQueryExecutor::getName() const {
  return "FakeQueryExecutor";
}

docid_t FakeQueryExecutor::getDocId() {
    assert(false);
    return INVALID_DOCID;
}

docid_t FakeQueryExecutor::seek(docid_t id){
    assert(false);
    return INVALID_DOCID;
}

std::string FakeQueryExecutor::toString() const {
    return "";
}

END_HA3_NAMESPACE(search);


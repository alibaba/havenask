#include <ha3/search/SubFieldMapTermQueryExecutor.h>

using namespace std;

IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SubFieldMapTermQueryExecutor);

SubFieldMapTermQueryExecutor::SubFieldMapTermQueryExecutor(
        IE_NAMESPACE(index)::PostingIterator *iter, 
        const common::Term &term, 
        DocMapAttrIterator *mainToSubIter,
        DocMapAttrIterator *subToMainIter,
        fieldmap_t fieldMap, 
        FieldMatchOperatorType opteratorType)
    : SubTermQueryExecutor(iter, term, mainToSubIter, subToMainIter)
    , _fieldMap(fieldMap)
    , _opteratorType(opteratorType)
{
    initBufferedPostingIterator();    
}

SubFieldMapTermQueryExecutor::~SubFieldMapTermQueryExecutor() { 
}

IE_NAMESPACE(common)::ErrorCode SubFieldMapTermQueryExecutor::seekSubDoc(docid_t docId,
        docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    fieldmap_t targetFieldMap = _fieldMap;
    FieldMatchOperatorType opteratorType = _opteratorType;
    while (true) {
        auto ec = SubTermQueryExecutor::seekSubDoc(docId, 
                subDocId, subDocEnd, needSubMatchdata, subDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (subDocId == END_DOCID) {
            break;
        }
        fieldmap_t fieldMap;
        ec = _bufferedIter->GetFieldMap(fieldMap);
        IE_RETURN_CODE_IF_ERROR(ec);
        fieldmap_t andResult = fieldMap & targetFieldMap;
        if (opteratorType == FM_AND) {
            if (andResult == targetFieldMap) {
                break;
            }
        } else {
            if (andResult > 0) {
                break;
            }
        }
        ++subDocId;
    }
    result = subDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

std::string SubFieldMapTermQueryExecutor::toString() const {
    std::string ret = _term.getIndexName();
    ret += _opteratorType == FM_OR ? '[' : '(';
    ret += autil::StringUtil::toString((uint32_t)_fieldMap);
    ret += _opteratorType == FM_OR ? ']' : ')';
    ret += ':';
    ret += _term.getWord().c_str();
    return ret;
}

void SubFieldMapTermQueryExecutor::initBufferedPostingIterator() {
    _bufferedIter = dynamic_cast<BufferedPostingIterator*>(_iter);
}

END_HA3_NAMESPACE(search);


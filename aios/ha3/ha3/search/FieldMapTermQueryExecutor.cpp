#include <ha3/search/FieldMapTermQueryExecutor.h>
#include <autil/StringUtil.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FieldMapTermQueryExecutor);

FieldMapTermQueryExecutor::FieldMapTermQueryExecutor(
        IE_NAMESPACE(index)::PostingIterator *iter, 
        const common::Term &term, fieldmap_t fieldMap, 
        FieldMatchOperatorType opteratorType)
    : BufferedTermQueryExecutor(iter, term)
    , _fieldMap(fieldMap)
    , _opteratorType(opteratorType)
{
    
}

FieldMapTermQueryExecutor::~FieldMapTermQueryExecutor() { 
}

IE_NAMESPACE(common)::ErrorCode FieldMapTermQueryExecutor::doSeek(docid_t id, docid_t& result) {
    fieldmap_t targetFieldMap = _fieldMap;
    FieldMatchOperatorType opteratorType = _opteratorType;
    while (true) {
        auto ec = BufferedTermQueryExecutor::doSeek(id, id);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (id == END_DOCID) {
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
        ++id;
    }
    result = id;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

std::string FieldMapTermQueryExecutor::toString() const {
    std::string ret = _term.getIndexName();
    ret += _opteratorType == FM_OR ? '[' : '(';
    ret += autil::StringUtil::toString((uint32_t)_fieldMap);
    ret += _opteratorType == FM_OR ? ']' : ')';
    ret += ':';
    ret += _term.getWord().c_str();
    return ret;
}

END_HA3_NAMESPACE(search);


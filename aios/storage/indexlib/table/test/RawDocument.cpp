#include "indexlib/table/test/RawDocument.h"

#include <assert.h>

#include "autil/StringUtil.h"
//#include "indexlib/document/index_locator.h"
#include "indexlib/base/Constant.h"

namespace indexlibv2 { namespace table {

const std::string RawDocument::EMPTY_STRING = "";

RawDocument::RawDocument()
{
    _cmd = UNKNOWN_OP;
    _timestamp = INVALID_TIMESTAMP;
    _docid = INVALID_DOCID;
}

RawDocument::~RawDocument() {}

void RawDocument::SetField(const std::string& fieldName, const std::string& fieldValue)
{
    if (fieldName == CMD_TAG) {
        _cmd = GetCmd(fieldValue);
    } else if (fieldName == RESERVED_TIMESTAMP) {
        _timestamp = autil::StringUtil::fromString<int64_t>(fieldValue);
        // } else if (fieldName == LOCATOR) {
        //     if (fieldValue.find(":") != string::npos) {
        //         vector<int64_t> tmpVec;
        //         StringUtil::fromString(fieldValue, tmpVec, ":");
        //         assert(tmpVec.size() == 2);
        //         IndexLocator locator(tmpVec[0], tmpVec[1], /*userData=*/"");
        //         mLocator.SetLocator(locator.toString());
        //     } else {
        //         mLocator.SetLocator(fieldValue);
        //     }
    } else {
        _fields[fieldName] = fieldValue;
    }
}

const std::string& RawDocument::GetField(const std::string& fieldName) const
{
    FieldMap::const_iterator it = _fields.find(fieldName);
    if (it == _fields.end()) {
        return EMPTY_STRING;
    }
    return it->second;
}

bool RawDocument::Exist(const std::string& fieldName) const
{
    FieldMap::const_iterator it = _fields.find(fieldName);
    return (it != _fields.end());
}

DocOperateType RawDocument::GetCmd(const std::string& cmdString)
{
    if (cmdString == CMD_ADD) {
        return ADD_DOC;
    } else if (cmdString == CMD_DELETE) {
        return DELETE_DOC;
    } else if (cmdString == CMD_UPDATE_FIELD) {
        return UPDATE_FIELD;
    } else if (cmdString == CMD_DELETE_SUB) {
        return DELETE_SUB_DOC;
    }
    assert(false);
    return UNKNOWN_OP;
}

std::string RawDocument::DebugString() const
{
    std::stringstream ss;
    ss << "[docid=" << _docid << "], [pk=" << _primaryKey << "], [fields=";
    for (const auto& pair : _fields) {
        ss << " " << pair.first << ":" << pair.second << ";";
    }
    ss << "]";
    return ss.str();
}
}} // namespace indexlibv2::table

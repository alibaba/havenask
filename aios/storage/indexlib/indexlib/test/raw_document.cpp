#include "indexlib/test/raw_document.h"

#include "autil/StringUtil.h"
#include "indexlib/document/index_locator.h"

using namespace std;
using namespace autil;
using namespace indexlib::document;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, RawDocument);

const string RawDocument::EMPTY_STRING = "";

RawDocument::RawDocument()
{
    mCmd = UNKNOWN_OP;
    mTs = INVALID_TIMESTAMP;
    mDocId = INVALID_DOCID;
}

RawDocument::~RawDocument() {}

void RawDocument::SetField(const string& fieldName, const string& fieldValue)
{
    if (fieldName == CMD_TAG) {
        mCmd = GetCmd(fieldValue);
    } else if (fieldName == RESERVED_TIMESTAMP) {
        mTs = StringUtil::fromString<int64_t>(fieldValue);
    } else if (fieldName == LOCATOR) {
        if (fieldValue.find(":") != string::npos) {
            vector<int64_t> tmpVec;
            StringUtil::fromString(fieldValue, tmpVec, ":");
            assert(tmpVec.size() == 2);
            IndexLocator locator(tmpVec[0], tmpVec[1], /*userData=*/"");
            mLocator.SetLocator(locator.toString());
        } else {
            if (!fieldValue.empty()) {
                int64_t offset;
                if (StringUtil::fromString(fieldValue, offset)) {
                    IndexLocator locator(0, offset, /*userData=*/"");
                    mLocator.SetLocator(locator.toString());
                } else {
                    mLocator.SetLocator(fieldValue);
                }
            }
        }
    } else {
        mFields[fieldName] = fieldValue;
    }
}

const string& RawDocument::GetField(const string& fieldName) const
{
    FieldMap::const_iterator it = mFields.find(fieldName);
    if (it == mFields.end()) {
        return EMPTY_STRING;
    }
    return it->second;
}

bool RawDocument::Exist(const string& fieldName) const
{
    FieldMap::const_iterator it = mFields.find(fieldName);
    return (it != mFields.end());
}

DocOperateType RawDocument::GetCmd(const string& cmdString)
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
    stringstream ss;
    ss << "[docid=" << mDocId << "], [pk=" << mPrimaryKey << "], [fields=";
    for (const auto& pair : mFields) {
        ss << " " << pair.first << ":" << pair.second << ";";
    }
    ss << "]";
    return ss.str();
}
}} // namespace indexlib::test

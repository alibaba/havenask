#include "indexlib/testlib/raw_document.h"
#include "indexlib/common/index_locator.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, RawDocument);

const string RawDocument::EMPTY_STRING = "";

RawDocument::RawDocument() 
{
    mCmd = UNKNOWN_OP;
    mTs = INVALID_TIMESTAMP;
    mDocId = INVALID_DOCID;
}

RawDocument::~RawDocument() 
{
}

void RawDocument::SetField(const string &fieldName, const string &fieldValue)
{
    if (fieldName == CMD_TAG)
    {
        mCmd = GetCmd(fieldValue);
    }
    else if (fieldName == RESERVED_TIMESTAMP)
    {
        mTs = StringUtil::fromString<int64_t>(fieldValue);
    }
    else if (fieldName == LOCATOR)
    {
        if (fieldValue.find(":") != string::npos)
        {
            vector<int64_t> tmpVec;
            StringUtil::fromString(fieldValue, tmpVec, ":");
            assert(tmpVec.size() == 2);
            IndexLocator locator(tmpVec[0], tmpVec[1]);
            mLocator.SetLocator(locator.toString());
        }
        else
        {
            mLocator.SetLocator(fieldValue);
        }
    }
    else
    {
        mFields[fieldName] = fieldValue;
    }
}

const string& RawDocument::GetField(const string &fieldName) const
{
    FieldMap::const_iterator it = mFields.find(fieldName);
    if (it == mFields.end()) 
    {
        return EMPTY_STRING;
    }
    return it->second;
}

DocOperateType RawDocument::GetCmd(const string& cmdString)
{
    if (cmdString == CMD_ADD)
    {
        return ADD_DOC;
    }
    else if (cmdString == CMD_DELETE)
    {
        return DELETE_DOC;
    }
    else if (cmdString == CMD_UPDATE_FIELD)
    {
        return UPDATE_FIELD;
    }
    else if (cmdString == CMD_DELETE_SUB)
    {
        return DELETE_SUB_DOC;
    }
    assert(false);
    return UNKNOWN_OP;
}

IE_NAMESPACE_END(testlib);


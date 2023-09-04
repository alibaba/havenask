#ifndef __INDEXLIB_TEST_RAW_DOCUMENT_H
#define __INDEXLIB_TEST_RAW_DOCUMENT_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace test {

static const std::string CMD_TAG = "cmd";
static const std::string CMD_ADD = "add";
static const std::string CMD_DELETE = "delete";
static const std::string CMD_DELETE_SUB = "delete_sub";
static const std::string CMD_UPDATE_FIELD = "update_field";
static const std::string RESERVED_TIMESTAMP = "ts";
static const std::string LOCATOR = "locator";
static const std::string FIELD_DOCID = "docid";
static const std::string FIELD_SUB_DOCID = "sub_docid";
static const std::string FIELD_DELETION_MAP = "deletion_map";
static const std::string FIELD_POS = "pos";
static const std::string FIELD_POS_SEP = "#";
static const std::string RESERVED_MODIFY_FIELDS = "modify_fields";
static const std::string RESERVED_MODIFY_VALUES = "modify_values";
static const std::string MODIFY_FIELDS_SEP = "#";
static const std::string RESERVED_SUB_MODIFY_FIELDS = "sub_modify_fields";
static const std::string RESERVED_SUB_MODIFY_VALUES = "sub_modify_values";
static const std::string SUB_DOC_SEPARATOR = "^";
static const std::string RESERVED_REGION_NAME = "region_name";

class RawDocument
{
public:
    typedef std::map<std::string, std::string> FieldMap;
    typedef FieldMap::const_iterator Iterator;
    static const std::string EMPTY_STRING;

public:
    RawDocument();
    ~RawDocument();

public:
    void SetField(const std::string& fieldName, const std::string& fieldValue);
    const std::string& GetField(const std::string& fieldName) const;

    DocOperateType GetDocOperateType() const { return mCmd; }
    void SetDocOperateType(DocOperateType type) { mCmd = type; }

    int64_t GetTimestamp() const { return mTs; }
    const document::Locator& GetLocator() const { return mLocator; }

    Iterator Begin() const { return mFields.begin(); }
    Iterator End() const { return mFields.end(); }

    void Erasefield(const std::string& fieldName) { mFields.erase(fieldName); }

    void SetDocId(docid_t docId) { mDocId = docId; }
    docid_t GetDocId() const { return mDocId; }

    void SetPrimaryKey(const std::string& pk) { mPrimaryKey = pk; }
    std::string GetPrimaryKey() const { return mPrimaryKey; }

    bool Exist(const std::string& fieldName) const;
    std::string DebugString() const;

private:
    static DocOperateType GetCmd(const std::string& cmdString);

private:
    DocOperateType mCmd;
    FieldMap mFields;
    int64_t mTs;
    document::Locator mLocator;
    docid_t mDocId;
    std::string mPrimaryKey;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RawDocument);
}} // namespace indexlib::test

#endif //__INDEXLIB_TEST_RAW_DOCUMENT_H

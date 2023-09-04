#pragma once
#include <map>
#include <memory>

//#include "indexlib/document/locator.h"
#include "indexlib/base/Types.h"

namespace indexlibv2 { namespace table {

inline const std::string CMD_TAG = "cmd";
inline const std::string CMD_ADD = "add";
inline const std::string CMD_DELETE = "delete";
inline const std::string CMD_DELETE_SUB = "delete_sub";
inline const std::string CMD_UPDATE_FIELD = "update_field";
inline const std::string RESERVED_TIMESTAMP = "ts";
inline const std::string LOCATOR = "locator";
inline const std::string FIELD_DOCID = "docid";
inline const std::string FIELD_SUB_DOCID = "sub_docid";
inline const std::string FIELD_DELETION_MAP = "deletion_map";
inline const std::string FIELD_POS = "pos";
inline const std::string FIELD_POS_SEP = "#";
inline const std::string RESERVED_MODIFY_FIELDS = "modify_fields";
inline const std::string RESERVED_MODIFY_VALUES = "modify_values";
inline const std::string MODIFY_FIELDS_SEP = "#";
inline const std::string RESERVED_SUB_MODIFY_FIELDS = "sub_modify_fields";
inline const std::string RESERVED_SUB_MODIFY_VALUES = "sub_modify_values";
inline const std::string SUB_DOC_SEPARATOR = "^";
inline const std::string RESERVED_REGION_NAME = "region_name";

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

    DocOperateType GetDocOperateType() const { return _cmd; }
    void SetDocOperateType(DocOperateType type) { _cmd = type; }

    int64_t GetTimestamp() const { return _timestamp; }
    // const document::Locator& GetLocator() const { return mLocator; }

    Iterator Begin() const { return _fields.begin(); }
    Iterator End() const { return _fields.end(); }

    void Erasefield(const std::string& fieldName) { _fields.erase(fieldName); }

    void SetDocId(docid_t docId) { _docid = docId; }
    docid_t GetDocId() const { return _docid; }

    void SetPrimaryKey(const std::string& pk) { _primaryKey = pk; }
    std::string GetPrimaryKey() const { return _primaryKey; }

    bool Exist(const std::string& fieldName) const;
    std::string DebugString() const;

private:
    static DocOperateType GetCmd(const std::string& cmdString);

private:
    DocOperateType _cmd;
    FieldMap _fields;
    int64_t _timestamp;
    // document::Locator mLocator;
    docid_t _docid;
    std::string _primaryKey;
};
}} // namespace indexlibv2::table

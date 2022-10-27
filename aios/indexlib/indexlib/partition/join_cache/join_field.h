#ifndef __INDEXLIB_JOINFIELD_H
#define __INDEXLIB_JOINFIELD_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(partition);

const std::string BUILD_IN_JOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX = "_@_join_docid_";
const std::string BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX = "_@_subjoin_docid_";

struct JoinField {
    JoinField() {}
    JoinField(const std::string& _fieldName,
              const std::string& _joinTableName,
              bool _genJoinCache,
              bool _isStrongJoin)
        : genJoinCache(_genJoinCache)
        , isStrongJoin(_isStrongJoin)
        , fieldName(_fieldName)
        , joinTableName(_joinTableName)
    {}
    bool operator == (const JoinField& other) const
    {
        return genJoinCache == other.genJoinCache
            && isStrongJoin == other.isStrongJoin
            && fieldName == other.fieldName
            && joinTableName == other.joinTableName;
    }
    
    bool genJoinCache;
    bool isStrongJoin;
    std::string fieldName;
    std::string joinTableName;
};

// tableName --> JoinFields
typedef std::map<std::string, std::vector<JoinField> > JoinRelationMap;
typedef std::map<std::string, JoinField> JoinFieldMap;
// ReverseJoinRelationMap[auxTableName][mainTableName] --> JoinField
typedef std::map<std::string, JoinFieldMap> ReverseJoinRelationMap;

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_JOINFIELD_H

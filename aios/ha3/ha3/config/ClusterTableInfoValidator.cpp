#include <ha3/config/ClusterTableInfoValidator.h>

using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, ClusterTableInfoValidator);


ClusterTableInfoValidator::ClusterTableInfoValidator() { 
}

ClusterTableInfoValidator::~ClusterTableInfoValidator() { 
}

bool ClusterTableInfoValidator::validate(
        const ClusterTableInfoManagerMapPtr &clusterTableInfoManagerMapPtr)
{
    assert(clusterTableInfoManagerMapPtr);
    ClusterTableInfoManagerMap::const_iterator iter = 
        clusterTableInfoManagerMapPtr->begin();
    for (; iter != clusterTableInfoManagerMapPtr->end(); iter++) {
        if (!validateSingleClusterTableInfoManager(iter->second)) {
            HA3_LOG(ERROR, "validate ClusterTableInfoManager failed, cluster name [%s]!", 
                    iter->first.c_str());
            return false;
        }
    }

    return true;
}

bool ClusterTableInfoValidator::validateSingleClusterTableInfoManager(
        const ClusterTableInfoManagerPtr &clusterTableInfoManagerPtr) {
    TableInfoPtr mainTableInfoPtr = clusterTableInfoManagerPtr->getMainTableInfo();
    const JoinTableInfos &joinTableInfos = clusterTableInfoManagerPtr->getJoinTableInfos();

    const std::string &scanJoinClusterName =
        clusterTableInfoManagerPtr->getScanJoinClusterName();
    if (!scanJoinClusterName.empty()) {
	if (!joinTableInfos.count(scanJoinClusterName)) {
	    HA3_LOG(ERROR, "scan join cluster name [%s] invalid", scanJoinClusterName.c_str());
	    return false;
	}
    }
    

    vector<TableInfoPtr> joinTableInfoPtrs;
    for (JoinTableInfos::const_iterator it = joinTableInfos.begin();
         it != joinTableInfos.end(); it++) {
        const string& joinFieldName =  it->second.first.getJoinField();
        bool usePk = it->second.first.useJoinPK;

        const TableInfoPtr& joinTableInfoPtr = it->second.second;
        if (!validateMainTableAgainstJoinTable(mainTableInfoPtr, 
		joinTableInfoPtr, joinFieldName, usePk)) {
            HA3_LOG(ERROR, "validate join table [%s] against main table [%s] failed!",
                    joinTableInfoPtr->getTableName().c_str(),
                    mainTableInfoPtr->getTableName().c_str());
            return false;
        }
        joinTableInfoPtrs.push_back(joinTableInfoPtr);
    }
    return validateJoinTables(joinTableInfoPtrs);
}

bool ClusterTableInfoValidator::validateJoinTables(
        const vector<TableInfoPtr> &joinTableInfos)
{
    for (size_t i = 0; i < joinTableInfos.size(); i++) {
        const TableInfoPtr &joinTableInfo1 = joinTableInfos[i];
        for (size_t j = i + 1; j < joinTableInfos.size(); j++) {
            const TableInfoPtr &joinTableInfo2 = joinTableInfos[j];
            if (!validateFieldInfosBetweenJoinTables(
                            joinTableInfo1, joinTableInfo2))
            {
                HA3_LOG(ERROR, "validate between join table [%s] and join table [%s] failed!",
                        joinTableInfo1->getTableName().c_str(),
                        joinTableInfo2->getTableName().c_str());
                return false;
            }
        }
    }
    return true;
}

bool ClusterTableInfoValidator::validateMainTableAgainstJoinTable(
        const TableInfoPtr &mainTableInfoPtr,
        const TableInfoPtr &joinTableInfoPtr,
        const string &joinFieldName, bool usePK) 
{
    assert(mainTableInfoPtr && joinTableInfoPtr);
    const FieldInfos *mainTableFieldInfos = mainTableInfoPtr->getFieldInfos();
    const FieldInfos *joinTableFieldInfos = joinTableInfoPtr->getFieldInfos();
    assert(mainTableFieldInfos && joinTableFieldInfos);
    const AttributeInfos *mainTableAttributeInfos = mainTableInfoPtr->getAttributeInfos();
    
    if (!joinFieldName.empty()) {
        const FieldInfo  *mainTableFieldInfo = mainTableFieldInfos->getFieldInfo(joinFieldName.c_str());
        const FieldInfo  *joinTableFieldInfo = joinTableFieldInfos->getFieldInfo(joinFieldName.c_str());
    
        if (!mainTableFieldInfo) {
            HA3_LOG(ERROR, "join field (%s) is not defined in main table [%s].", 
                    joinFieldName.c_str(),
                    mainTableInfoPtr->getTableName().c_str());
            return false;
        }
        if (!joinTableFieldInfo) {
            HA3_LOG(ERROR, "join field (%s) is not defined in join table [%s].", 
                    joinFieldName.c_str(),
                    joinTableInfoPtr->getTableName().c_str());
            return false;
        }
        if (*mainTableFieldInfo != *joinTableFieldInfo)
        {
            HA3_LOG(ERROR, "join field has different definition in table [%s] and table [%s].", 
                    mainTableInfoPtr->getTableName().c_str(),
                    joinTableInfoPtr->getTableName().c_str());
            return false;
        }

        if (mainTableFieldInfo->isMultiValue == true) {
            HA3_LOG(ERROR, "join field (%s) is not single value type.", 
                    joinFieldName.c_str());
            return false;
        }

        if(!isSupportedFieldType(mainTableFieldInfo->fieldType)) {
            HA3_LOG(ERROR, "join field type(%d) is not supported .", 
                    mainTableFieldInfo->fieldType);
            return false;
        }

        if (usePK) {
            const IndexInfo *joinTablePrimaryKeyIndexInfo =
                    joinTableInfoPtr->getPrimaryKeyIndexInfo();
            if (!joinTablePrimaryKeyIndexInfo) {
                HA3_LOG(ERROR,
                        "primarykey index not defined in join table [%s].",
                        joinTableInfoPtr->getTableName().c_str());
                return false;
            }
            if (joinTablePrimaryKeyIndexInfo->fieldName != joinFieldName) {
                HA3_LOG(ERROR,
                        "join field [%s] doesn't has primarykey index in join "
                        "table [%s].",
                        joinFieldName.c_str(),
                        joinTableInfoPtr->getTableName().c_str());
                return false;
            }
        }

        if (!mainTableAttributeInfos) {
            HA3_LOG(ERROR, "main table [%s] doesn't has attribute definition.", 
                    mainTableInfoPtr->getTableName().c_str());
            return false;
        }
        const AttributeInfo* attrbiteInfo = 
            mainTableAttributeInfos->getAttributeInfo(joinFieldName);
        if (!attrbiteInfo) {
            HA3_LOG(ERROR, "main table [%s] doesn't has attribute(%s).", 
                    mainTableInfoPtr->getTableName().c_str(),
                    joinFieldName.c_str());
            return false;
        }
    }
    vector<string> joinTableFieldNames;
    joinTableFieldInfos->getAllFieldNames(joinTableFieldNames);
    for (size_t i = 0; i < joinTableFieldNames.size(); i++) {
        string fieldName = joinTableFieldNames[i];
        if (mainTableFieldInfos->getFieldInfo(fieldName.c_str())
            && (fieldName != joinFieldName)) {
            HA3_LOG(ERROR, "field (%s) is defined both in main table [%s] and join table [%s].",
                    fieldName.c_str(), 
                    mainTableInfoPtr->getTableName().c_str(), 
                    joinTableInfoPtr->getTableName().c_str());
            return false;
        }
    }
    return true;
}

bool ClusterTableInfoValidator::validateFieldInfosBetweenJoinTables(
        const TableInfoPtr &joinTableInfoPtr1,
        const TableInfoPtr &joinTableInfoPtr2) 
{
    assert(joinTableInfoPtr1 && joinTableInfoPtr2);
    const FieldInfos *joinTableFieldInfos1 = joinTableInfoPtr1->getFieldInfos();
    const FieldInfos *joinTableFieldInfos2 = joinTableInfoPtr2->getFieldInfos();
    vector<string> fieldNames1;
    vector<string> fieldNames2;
    joinTableFieldInfos1->getAllFieldNames(fieldNames1);
    joinTableFieldInfos2->getAllFieldNames(fieldNames2);
    
    for(size_t i = 0; i < fieldNames1.size(); i++) {
        if (find(fieldNames2.begin(), fieldNames2.end(), fieldNames1[i])
            != fieldNames2.end()) {
            HA3_LOG(ERROR, "field (%s) is denfined both in join table [%s] and join table [%s].",
                    fieldNames1[i].c_str(),
                    joinTableInfoPtr1->getTableName().c_str(),
                    joinTableInfoPtr2->getTableName().c_str());
            return false;
        }
    }
    return true;
}

bool ClusterTableInfoValidator::isSupportedFieldType(FieldType ft) {
    return (ft == ft_int8 || ft == ft_uint8
            || ft == ft_int16 || ft == ft_uint16 
            || ft == ft_int32 || ft == ft_uint32
            || ft == ft_int64 || ft == ft_uint64
            || ft == ft_string);
}

END_HA3_NAMESPACE(config);


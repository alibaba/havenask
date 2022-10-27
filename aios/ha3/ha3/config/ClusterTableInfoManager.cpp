#include <ha3/config/ClusterTableInfoManager.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, ClusterTableInfoManager);

ClusterTableInfoManager::ClusterTableInfoManager() { 
}

ClusterTableInfoManager::~ClusterTableInfoManager() { 
}

void ClusterTableInfoManager::addJoinTableInfo(const TableInfoPtr &tableInfoPtr,
        const JoinConfigInfo &joinConfigInfo) 
{
    _joinTableInfos[tableInfoPtr->getTableName()] = 
        make_pair(joinConfigInfo, tableInfoPtr);
}

TableInfoPtr ClusterTableInfoManager::getJoinTableInfoByTableName(
        const std::string &tableName) const 
{
    JoinTableInfos::const_iterator iter = _joinTableInfos.find(tableName);
    if (iter == _joinTableInfos.end()) {
        return TableInfoPtr();
    } else {
        return iter->second.second;
    }
}

std::string ClusterTableInfoManager::getJoinFieldNameByTableName(
        const std::string &tableName) const
{
    JoinTableInfos::const_iterator iter = _joinTableInfos.find(tableName);
    if (iter == _joinTableInfos.end()) {
        return std::string("");
    } else {
        return iter->second.first.getJoinField();
    }
}

const suez::turing::AttributeInfo* ClusterTableInfoManager::getAttributeInfo(
        const string &attributeName) const 
{
    assert(_mainTableInfoPtr);
    const AttributeInfos* attributeInfos = _mainTableInfoPtr->getAttributeInfos();
    if (attributeInfos) {
        const AttributeInfo* attributeInfo = attributeInfos->getAttributeInfo(attributeName);
        if (attributeInfo) {
            return attributeInfo;
        }
    }
    JoinTableInfos::const_iterator iter = _joinTableInfos.begin();
    for (; iter != _joinTableInfos.end(); iter++) {
        const AttributeInfos* attributeInfos = iter->second.second->getAttributeInfos();
        if (attributeInfos) {
            const AttributeInfo* attributeInfo = attributeInfos->getAttributeInfo(attributeName);
            if (attributeInfo) {
                return attributeInfo;
            }
        }
    }
    return NULL;
}

TableInfoPtr ClusterTableInfoManager::getClusterTableInfo() const {
    vector<TableInfoPtr> auxTableInfos;
    for (JoinTableInfos::const_iterator it = _joinTableInfos.begin();
         it != _joinTableInfos.end(); it++)
    {
        auxTableInfos.push_back(it->second.second);
    }
    return getClusterTableInfo(_mainTableInfoPtr, auxTableInfos);
}

TableInfoPtr ClusterTableInfoManager::getClusterTableInfo(
        const TableInfoPtr &mainTableInfoPtr,
        const std::vector<TableInfoPtr> &auxTableInfos)
{
    TableInfoPtr tableInfoPtr(new TableInfo(*mainTableInfoPtr));
    AttributeInfos *attributeInfos = tableInfoPtr->getAttributeInfos(); 
    FieldInfos *fieldInfos = tableInfoPtr->getFieldInfos();
    for (std::vector<TableInfoPtr>::const_iterator it = auxTableInfos.begin();
         it != auxTableInfos.end(); it++)
    {
        const TableInfoPtr &joinTableInfoPtr = *it;
        AttributeInfos *joinAttributeInfos = joinTableInfoPtr->getAttributeInfos();
        const AttributeInfos::AttrVector& attributesVect = 
            joinAttributeInfos->getAttrbuteInfoVector();
        for (size_t i = 0;  i < attributesVect.size(); i++) {
            const AttributeInfo *attributeInfo = attributesVect[i];
            AttributeInfo *attrInfo(new AttributeInfo(*attributeInfo));
            attributeInfos->addAttributeInfo(attrInfo);
            const FieldInfo &fieldInfo = attributeInfo->getFieldInfo();
            FieldInfo* newFieldInfo(new FieldInfo(fieldInfo));
            fieldInfos->addFieldInfo(newFieldInfo);
        }
    }
    return tableInfoPtr;
}

void ClusterTableInfoManager::setScanJoinClusterName(
    const std::string &scanJoinClusterName) {
    _scanJoinClusterName = scanJoinClusterName;
}

const std::string &ClusterTableInfoManager::getScanJoinClusterName() const {
    return _scanJoinClusterName;
}

////////////////////////////////////////////////////////////////////////////////

ClusterTableInfoManagerPtr ClusterTableInfoManagerMap::getClusterTableInfoManager(
        const std::string &clusterName) const 
{
    ClusterTableInfoManagerMap::const_iterator it;
    if ((it = find(clusterName)) != end()) {
        return it->second;
    }
    return ClusterTableInfoManagerPtr();
}


END_HA3_NAMESPACE(config);


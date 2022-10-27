#ifndef ISEARCH_CLUSTERTABLEINFOVALIDATOR_H
#define ISEARCH_CLUSTERTABLEINFOVALIDATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ClusterTableInfoManager.h>


BEGIN_HA3_NAMESPACE(config);

class ClusterTableInfoValidator
{
public:
    ClusterTableInfoValidator();
    ~ClusterTableInfoValidator();
private:
    ClusterTableInfoValidator(const ClusterTableInfoValidator &);
    ClusterTableInfoValidator& operator=(const ClusterTableInfoValidator &);
public:
    static bool validate(const ClusterTableInfoManagerMapPtr &clusterTableInfoManagerMapPtr);
    static bool validateMainTableAgainstJoinTable(const suez::turing::TableInfoPtr &mainTableInfoPtr,
            const suez::turing::TableInfoPtr &joinTableInfoPtr,
	const std::string &joinFieldName, bool usePK);
    static bool validateJoinTables(const std::vector<suez::turing::TableInfoPtr> &joinTables);
private:
    static bool validateFieldInfosBetweenJoinTables(const suez::turing::TableInfoPtr &joinTableInfoPtr1,
            const suez::turing::TableInfoPtr &joinTableInfoPtr2);
    static bool validateSingleClusterTableInfoManager(
            const ClusterTableInfoManagerPtr &clusterTableInfoManagerPtr);
    static bool isSupportedFieldType(FieldType ft);
private:
    typedef ClusterTableInfoManager::JoinTableInfos JoinTableInfos;
    friend class ClusterTableInfoValidatorTest;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ClusterTableInfoValidator);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_CLUSTERTABLEINFOVALIDATOR_H

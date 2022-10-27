#ifndef ISEARCH_TABLEINFOCREATOR_H
#define ISEARCH_TABLEINFOCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>

BEGIN_HA3_NAMESPACE(queryparser);

class TableInfoCreator
{
public:
    TableInfoCreator();
    ~TableInfoCreator();
public:
    static void prepareTableInfo(suez::turing::TableInfoPtr tableInfoPtr, 
                                 const std::string& attrNames, 
                                 const std::string& attrTypes);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_TABLEINFOCREATOR_H

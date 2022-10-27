#ifndef ISEARCH_TABLEREADERCONSTRUCTOR_H
#define ISEARCH_TABLEREADERCONSTRUCTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/index/TableReader.h>
#include <ha3/index/ReaderFacade.h>

BEGIN_HA3_NAMESPACE(search);

class TableReaderConstructor
{
public:
    TableReaderConstructor(const std::string &tablePath,
                           const std::string &rankConfFile,
                           const std::string &tableName,
                           const std::string &indexName);
    ~TableReaderConstructor();
public:
    index::TableReader* getTableReader();
    index::ReaderFacade* getReaderFacade();
private:
    index::TableReader *_tableReader;
    index::ReaderFacade *_readerFacade;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_TABLEREADERCONSTRUCTOR_H

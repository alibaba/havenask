#include <ha3/index/TableReader.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/test/test.h>
#include <string>
#include <memory>
#include <build_service/plugin/PlugInManager.h>

#include <ha3/search/test/TableReaderConstructor.h>

using namespace std;
USE_HA3_NAMESPACE(config);
using namespace build_service::plugin;
USE_HA3_NAMESPACE(index);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, TableReaderConstructor);

TableReaderConstructor::TableReaderConstructor(const std::string &tablePath,
        const std::string &rankConfFile,
        const std::string &tableName,
        const std::string &indexName) 
{
    TableInfo tableInfo(tablePath.c_str());
    tableInfo.setRankProfileConf(rankConfFile);
    tableInfo.tableName = tableName;
    IndexInfos *indexInfos = new IndexInfos(tableInfo.getPath().c_str());
    IndexInfo *indexInfo = new IndexInfo(indexInfos->getPath().c_str());
    indexInfo->indexName = indexName;
    indexInfos->addIndexInfo(indexInfo);
    tableInfo.setIndexInfos(indexInfos);

    _tableReader = new TableReader(tableInfo);
    bool rc = _tableReader->init();
    assert(rc);
    HA3_LOG(TRACE3, "rank profile path: [%s]", tableInfo.getRankProfileConf().c_str());
    const IndexReaderManager *readerManager = _tableReader->getIndexReaderManager();
    assert(readerManager);
    _readerFacade = new ReaderFacade(readerManager, NULL);
}

TableReaderConstructor::~TableReaderConstructor() { 
    delete _readerFacade;
    _readerFacade = NULL;
    delete _tableReader;
    _tableReader = NULL;
}

index::TableReader* TableReaderConstructor::getTableReader() {
    return _tableReader;
}

index::ReaderFacade* TableReaderConstructor::getReaderFacade() {
    return _readerFacade;
}


END_HA3_NAMESPACE(search);


#pragma once

#include "suez/turing/expression/framework/MatchDocsExpressionCreator.h"
#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/condition/Condition.h>
#include <ha3/sql/proto/SqlSearchInfo.pb.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/util/Log.h>
#include <navi/resource/MemoryPoolResource.h>
#include <ha3/sql/ops/condition/ExprUtil.h>

BEGIN_HA3_NAMESPACE(sql);

class CalcTable {
public:
    CalcTable(autil::mem_pool::Pool *pool,
              navi::MemoryPoolResource *memoryPoolResource,
              const std::vector<std::string> &outputFields,
              const std::vector<std::string> &outputFieldsType,
              suez::turing::Tracer *tracer,
              suez::turing::SuezCavaAllocator *_cavaAllocator,
              suez::turing::CavaPluginManagerPtr cavaPluginManager,
              suez::turing::FunctionInterfaceCreator *funcInterfaceCreator);
    ~CalcTable() {}
public:
    bool init(const std::string &outputExprsJson, const std::string &conditionJson);
    bool calcTable(TablePtr &table);
    bool needCalc(const TablePtr &table);
    bool filterTable(const TablePtr &table);
    bool filterTable(const TablePtr &table, size_t startIdx, size_t endIdx, bool lazyDelete);
    bool projectTable(TablePtr &table);
    void setFilterFlag(bool filterFlag) {
        _filterFlag = filterFlag;
    }

public:
    static void addAliasMap(matchdoc::MatchDocAllocator *allocator,
                            const std::map<std::string, std::string> &aliasMap);
private:
    bool projectTable(const TablePtr &inputTable, TablePtr &outputTable, bool declareOnly);
    bool cloneColumn(ColumnPtr originColumn, TablePtr table,
                     const std::string &name, bool declareOnly);
    suez::turing::AttributeExpression *createAttributeExpression(
        const std::string &outputType,
        suez::turing::MatchDocsExpressionCreator& exprCreator,
        const std::string &exprStr);
    bool outputExprColumn(const TablePtr &table, const std::string &name, const std::string &type,
                          const TablePtr &inputTable, const ExprEntity &exprStr,
                          bool declareOnly, suez::turing::MatchDocsExpressionCreator *exprCreator);
    bool needCopyTable(const TablePtr &table);

private:
    bool _filterFlag;
    autil::mem_pool::Pool *_pool;
    navi::MemoryPoolResource *_memoryPoolResource;
    std::string _conditionJson;
    ConditionPtr _condition;
    std::map<std::string, ExprEntity> _exprsMap;
    std::map<std::string, std::string> _exprsAliasMap;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsType;
    suez::turing::Tracer *_tracer;
    suez::turing::SuezCavaAllocator *_cavaAllocator;
    suez::turing::CavaPluginManagerPtr _cavaPluginManager;
    suez::turing::FunctionInterfaceCreator *_funcInterfaceCreator;
private:
    static const std::string DEFAULT_NULL_NUMBER_VALUE;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CalcTable);

END_HA3_NAMESPACE(sql);

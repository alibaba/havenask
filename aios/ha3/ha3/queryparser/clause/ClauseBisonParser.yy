%{
%}

%skeleton "lalr1.cc"
%name-prefix="isearch_bison"
%debug
%start clause_final
%defines
%define "parser_class_name" "ClauseBisonParser"

%locations

%parse-param {isearch::queryparser::ClauseScanner &scanner}
%parse-param {isearch::queryparser::ClauseParserContext &ctx}

%error-verbose
%debug

%code requires{
#include <stdio.h>
#include <string>
#include <limits.h>
#include <vector>
#include "ha3/isearch.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"
#include "suez/turing/expression/syntax/RankSyntaxExpr.h"
#include "suez/turing/expression/syntax/MultiSyntaxExpr.h"
#include "ha3/common/AggregateDescription.h"
#include "ha3/common/DistinctDescription.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "ha3/queryparser/ClauseParserContext.h"
#include "ha3/common/QueryLayerClause.h"
#include "ha3/common/RankSortClause.h"

namespace isearch {
namespace queryparser {

class ClauseScanner;

} // namespace queryparser
} // namespace isearch
}

%union {
    std::string *stringVal;
    suez::turing::SyntaxExpr *syntaxExprVal;
    suez::turing::MultiSyntaxExpr *multiSyntaxExprVal;
    suez::turing::FuncSyntaxExpr *funcExprVal;
    isearch::common::DistinctDescription *distinctDescVal;
    isearch::common::VirtualAttributeClause *virtualAttributeClauseVal;
    isearch::common::FilterClause *filterClauseVal;
    std::vector<std::string *> *gradeThresholdsVal;
    isearch::common::AggregateDescription *aggDescVal;
    isearch::common::AggFunDescription *aggFunDescVal;
    std::vector<std::string> *rangeExprVal;
    std::vector<isearch::common::AggFunDescription*> *aggFunDescsVal;
    isearch::common::QueryLayerClause *layerClause;
    isearch::common::LayerDescription *layerDesc;
    isearch::common::LayerKeyRange *layerKeyRange;
    isearch::common::LayerAttrRange *layerAttrRange;
    std::vector<isearch::common::LayerKeyRange*> *multiLayerRanges;
    isearch::common::SearcherCacheClause *searcherCacheClause;
    std::vector<suez::turing::SyntaxExpr*> *extraDistKeyExprVectVal;
    isearch::common::RankSortClause *rankSortClause;
    isearch::common::RankSortDescription *rankSortDesc;
    std::vector<isearch::common::SortDescription*> *sortDescs;
    isearch::common::SortDescription *sortDesc;
}

%token			END	     0	"end of file"
%token <stringVal> 	INTEGER  	"integer"
%token <stringVal> 	FLOAT		"float"
%token <stringVal> 	IDENTIFIER      "identifier"
%token <stringVal> 	PHRASE_STRING      "string NOT including symbols"

%token AND OR EQUAL LE GE LESS GREATER NE
%token DISTINCT_KEY DISTINCT_COUNT DISTINCT_TIMES DISTINCT_MAX_ITEM_COUNT DISTINCT_RESERVED DISTINCT_UPDATE_TOTAL_HIT MODULE_NAME DISTINCT_FILTER_CLAUSE GRADE_THRESHOLDS
%token GROUP_KEY RANGE_KEY FUNCTION_KEY MAX_FUN MIN_FUN COUNT SUM_FUN DISTINCT_COUNT_FUN AGGREGATE_FILTER AGG_SAMPLER_THRESHOLD AGG_SAMPLER_STEP MAX_GROUP ORDER_BY QUOTA_KEY UNLIMITED_KEY
%token CACHE_KEY CACHE_USE CACHE_CUR_TIME CACHE_EXPIRE_TIME CACHE_FILTER CACHE_DOC_NUM_LIMIT REFRESH_ATTRIBUTES TRUNCATE_REDUNDANT_RESULT
%token UNIQ_KEY UNIQ_COUNT UNIQ_RANK_COUNT UNIQ_MAX_GROUP DISTINCT_KEYS DISTINCT_COUNTS DIST_PAGE_NUM RESORT_KEYS RESORT_COUNTS
%token SORT_KEY RANK_EXPRESSION SORT_PERCENT

%type <syntaxExprVal> algebra_expression logical_expression logical_sub_expression relational_expression arithmetic_expression arithmetic_sub_expression distinct_key_expression group_key_expression expire_time_expression
%type <multiSyntaxExprVal> multi_algebra_expression
%type <funcExprVal> function_expr function_head
%type <aggDescVal> aggregate_description
%type <distinctDescVal> distinct_description
%type <virtualAttributeClauseVal> virtualAttribute_clause
%type <stringVal> distinct_count_expression distinct_times_expression max_item_count_expression distinct_reserved_expression update_total_hit_expression module_name_expression grade_thresholds_term general_identifier layer_range_key quota quota_and_type layer_range_value hash_key_expression cache_use_expression current_time_expression cache_doc_num_limit_expression cache_doc_num_limit_sub_expression refresh_attributes_expression refresh_attributes_sub_expression
%type <filterClauseVal> agg_filter_clause dist_filter_clause cache_filter_clause
%type <gradeThresholdsVal> grade_thresholds_expression grade_thresholds_sub_expression
%type <rangeExprVal> range_expression range_sub_expression
%type <aggFunDescsVal> aggregate_functions aggregate_functions_sub
%type <aggFunDescVal> aggregate_function
%type <stringVal> sampler_threshold sampler_step max_group order_by
%type <layerClause> layer_clause
%type <layerDesc> layer_description
%type <layerKeyRange> layer_range sub_layer_ranges
%type <layerAttrRange> sub_layer_range
%type <multiLayerRanges> multi_layer_ranges
%type <searcherCacheClause> searcher_cache_clause
%type <stringVal> sort_percent
%type <rankSortClause> rank_sort_clause
%type <rankSortDesc> rank_sort_description
%type <sortDescs> sort_expressions
%type <sortDesc> ordered_sort_expr
%type <sortDesc> sort_expr

%destructor {delete $$;} IDENTIFIER PHRASE_STRING INTEGER FLOAT
%destructor {delete $$;} algebra_expression logical_expression logical_sub_expression relational_expression arithmetic_expression arithmetic_sub_expression distinct_key_expression group_key_expression  expire_time_expression
%destructor {delete $$;} multi_algebra_expression
%destructor {delete $$;} function_expr function_head
%destructor {delete $$;} distinct_description
%destructor {delete $$;} virtualAttribute_clause
%destructor {delete $$;} distinct_count_expression distinct_times_expression max_item_count_expression distinct_reserved_expression update_total_hit_expression module_name_expression grade_thresholds_term general_identifier layer_range_key quota layer_range_value hash_key_expression cache_use_expression current_time_expression cache_doc_num_limit_expression cache_doc_num_limit_sub_expression refresh_attributes_expression refresh_attributes_sub_expression sort_percent
%destructor {delete $$;} agg_filter_clause dist_filter_clause cache_filter_clause
%destructor {
    for (std::vector<std::string *>::iterator it = $$->begin(); it != $$->end(); ++it) {
        delete *it;
    }
    delete $$;
 } grade_thresholds_expression grade_thresholds_sub_expression
%destructor {delete $$;} aggregate_description
%destructor {delete $$;} range_expression range_sub_expression
%destructor {delete $$;} aggregate_function
%destructor {
    for (std::vector<isearch::common::AggFunDescription*>::iterator it = $$->begin(); it != $$->end(); ++it) {
        delete *it;
    }
    delete $$;
 } aggregate_functions aggregate_functions_sub
%destructor {delete $$;} sampler_threshold sampler_step max_group order_by
%destructor {delete $$;} layer_clause
%destructor {delete $$;} layer_description
%destructor {delete $$;} layer_range sub_layer_ranges
%destructor {delete $$;} sub_layer_range
%destructor {
    for (std::vector<isearch::common::LayerKeyRange*>::iterator it = $$->begin(); it != $$->end(); ++it) {
        delete *it;
    }
    delete $$;
} multi_layer_ranges
%destructor {delete $$;} searcher_cache_clause

%destructor {delete $$;} rank_sort_clause
%destructor {delete $$;} rank_sort_description
%destructor {
    for (std::vector<isearch::common::SortDescription*>::iterator it = $$->begin(); it != $$->end(); ++it) {
        delete *it;
    }
    delete $$;
} sort_expressions
%destructor {delete $$;} ordered_sort_expr
%destructor {delete $$;} sort_expr

%{

#include "ha3/queryparser/ClauseScanner.h"

#undef yylex
#define yylex scanner.lex
%}

%left OR
%left AND
%left '|'
%left '^'
%left '&'
%left EQUAL NE
%left LESS GREATER LE GE
%left '+' '-'
%left '*' '/'
%%

clause_final : clause END {
    ctx.setStatus(isearch::queryparser::ClauseParserContext::OK);
}

clause : distinct_description { ctx.setDistDescription($1); }
       | virtualAttribute_clause { ctx.setVirtualAttributeClause($1); }
       | aggregate_description {  ctx.setAggDescription($1); }
       | algebra_expression { ctx.setSyntaxExpr($1); }
       | layer_clause { ctx.setLayerClause($1); }
       | searcher_cache_clause { ctx.setSearcherCacheClause($1);}
       | rank_sort_clause { ctx.setRankSortClause($1); }

searcher_cache_clause : searcher_cache_clause ',' hash_key_expression { ctx.getSearcherCacheParser().setSearcherCacheKey($1, *$3);  $$ = $1; delete $3;}
                      | hash_key_expression { $$ = ctx.getSearcherCacheParser().createSearcherCacheClause(); ctx.getSearcherCacheParser().setSearcherCacheKey($$, *$1); delete $1;}
                      | searcher_cache_clause ',' cache_use_expression { ctx.getSearcherCacheParser().setSearcherCacheUse($1, *$3); $$ = $1; delete $3;}
                      | cache_use_expression { $$ = ctx.getSearcherCacheParser().createSearcherCacheClause(); ctx.getSearcherCacheParser().setSearcherCacheUse($$, *$1); delete $1;}
                      | searcher_cache_clause ',' current_time_expression { ctx.getSearcherCacheParser().setSearcherCacheCurTime($1, *$3); $$ = $1; delete $3;}
                      | current_time_expression { $$ = ctx.getSearcherCacheParser().createSearcherCacheClause(); ctx.getSearcherCacheParser().setSearcherCacheCurTime($$, *$1); delete $1;}
                      | searcher_cache_clause ',' expire_time_expression { ctx.getSearcherCacheParser().setSearcherCacheExpireExpr($1, $3); $$ = $1; }
                      | expire_time_expression { $$ = ctx.getSearcherCacheParser().createSearcherCacheClause(); ctx.getSearcherCacheParser().setSearcherCacheExpireExpr($$, $1); }
                      | searcher_cache_clause ',' cache_filter_clause { $1->setFilterClause($3); $$ = $1; }
                      | cache_filter_clause { $$ = ctx.getSearcherCacheParser().createSearcherCacheClause(); $$->setFilterClause($1); }
                      | searcher_cache_clause ',' cache_doc_num_limit_expression { ctx.getSearcherCacheParser().setSearcherCacheDocNum($1, *$3); $$ = $1; delete $3;}
                      | cache_doc_num_limit_expression { $$ = ctx.getSearcherCacheParser().createSearcherCacheClause(); ctx.getSearcherCacheParser().setSearcherCacheDocNum($$, *$1); delete $1;}
                      | searcher_cache_clause ',' refresh_attributes_expression { ctx.getSearcherCacheParser().setRefreshAttributes($1, *$3); $$ = $1; delete $3;}
                      | refresh_attributes_expression { $$ = ctx.getSearcherCacheParser().createSearcherCacheClause(); ctx.getSearcherCacheParser().setRefreshAttributes($$, *$1); delete $1;}


hash_key_expression : CACHE_KEY ':' INTEGER { $$ = $3; }
cache_use_expression : CACHE_USE ':' IDENTIFIER { $$ = $3; }
current_time_expression : CACHE_CUR_TIME ':' INTEGER { $$ = $3; }
expire_time_expression : CACHE_EXPIRE_TIME ':' algebra_expression { $$ = $3; }
cache_filter_clause : CACHE_FILTER ':' algebra_expression { $$ = ctx.getSearcherCacheParser().createFilterClause($3); }
cache_doc_num_limit_expression : CACHE_DOC_NUM_LIMIT ':'  cache_doc_num_limit_sub_expression { $$ = $3; }

cache_doc_num_limit_sub_expression : INTEGER { $$ = $1;}
| cache_doc_num_limit_sub_expression ';' INTEGER { $$ = $1; $$->append(";"+*$3); delete $3;}
| cache_doc_num_limit_sub_expression '#' INTEGER { $$ = $1; $$->append("#"+*$3); delete $3;}

refresh_attributes_expression : REFRESH_ATTRIBUTES ':' refresh_attributes_sub_expression { $$ = $3;}
refresh_attributes_sub_expression : IDENTIFIER { $$ = $1;}
| refresh_attributes_sub_expression ';' IDENTIFIER { $$ = $1; $$->append(";" + *$3); delete $3;}

layer_clause : layer_description { $$ = ctx.getLayerParser().createLayerClause(); $$->addLayerDescription($1); }
             | layer_clause ';' layer_description { $1->addLayerDescription($3); $$ = $1;}

layer_description : quota_and_type { $$ = ctx.getLayerParser().createLayerDescription(); ctx.getLayerParser().setQuota($$, *$1); delete $1;}
                  | layer_description ',' quota_and_type { ctx.getLayerParser().setQuota($1, *$3); $$ = $1; delete $3;}
                  | RANGE_KEY ':' multi_layer_ranges { $$ = ctx.getLayerParser().createLayerDescription(); $$->setLayerRanges(*$3); delete $3; }
                  | layer_description ',' RANGE_KEY ':' multi_layer_ranges { $1->setLayerRanges(*$5); delete $5; $$ = $1;}

quota_and_type : quota { $$ = $1;}
               | quota '#' INTEGER { $$ = $1; $$->append("#" + *$3); delete $3; }

quota : QUOTA_KEY ':' INTEGER  { $$ = $3;}
      | QUOTA_KEY ':' UNLIMITED_KEY  { $$ = new std::string("UNLIMITED");}

multi_layer_ranges : layer_range { $$ = new std::vector<isearch::common::LayerKeyRange*>; $$->push_back($1);}
                   | multi_layer_ranges '*' layer_range { $1->push_back($3); $$ = $1; }

layer_range : layer_range_key '{' sub_layer_ranges '}' { $3->attrName = *$1; delete $1; $$ = $3; }
            | layer_range_key { $$ = new isearch::common::LayerKeyRange(); $$->attrName = *$1; delete $1; }

layer_range_key : general_identifier { $$ = $1; }
                | '%' general_identifier {$$ = $2; $$->insert(0, 1, '%');}

sub_layer_ranges : sub_layer_range { $$ = new isearch::common::LayerKeyRange(); $$->ranges.push_back(*$1); delete $1;}
                 | sub_layer_ranges ',' sub_layer_range { $1->ranges.push_back(*$3); $$ = $1; delete $3; }
                 | layer_range_value { $$ = new isearch::common::LayerKeyRange(); $$->values.push_back(*$1); delete $1;}
                 | sub_layer_ranges ',' layer_range_value {$1->values.push_back(*$3); $$ = $1; delete $3; }

sub_layer_range : '[' layer_range_value ',' layer_range_value ']' { $$ = new isearch::common::LayerAttrRange(); $$->from = *$2; $$->to = *$4; delete $2; delete $4;}
                | '[' layer_range_value ',' layer_range_value ')' { $$ = new isearch::common::LayerAttrRange(); $$->from = *$2; $$->to = *$4; delete $2; delete $4;}
                | '[' ',' layer_range_value ']' {
                    $$ = new isearch::common::LayerAttrRange();
                    $$->from = isearch::common::LayerDescription::INFINITE;
                    $$->to = *$3;
                    delete $3;
                  }
                | '[' ',' layer_range_value ')' {
                    $$ = new isearch::common::LayerAttrRange();
                    $$->from = isearch::common::LayerDescription::INFINITE;
                    $$->to = *$3;
                    delete $3;
                  }
                | '[' layer_range_value ',' ']' {
                    $$ = new isearch::common::LayerAttrRange();
                    $$->from = *$2;
                    $$->to = isearch::common::LayerDescription::INFINITE;
                    delete $2;
                  }
                | '[' layer_range_value ',' ')' {
                    $$ = new isearch::common::LayerAttrRange();
                    $$->from = *$2;
                    $$->to = isearch::common::LayerDescription::INFINITE;
                    delete $2;
                  }
                | '[' ',' ']' {
                    $$ = new isearch::common::LayerAttrRange();
                    $$->from = isearch::common::LayerDescription::INFINITE;
                    $$->to = isearch::common::LayerDescription::INFINITE;
                  }
                | '[' ',' ')' {
                    $$ = new isearch::common::LayerAttrRange();
                    $$->from = isearch::common::LayerDescription::INFINITE;
                    $$->to = isearch::common::LayerDescription::INFINITE;
                  }

layer_range_value : INTEGER { $$ = $1; }
                  | '-' INTEGER { $2->insert(0, "-"); $$ = $2; }
                  | FLOAT { $$ = $1; }
                  | '-' FLOAT { $2->insert(0, "-"); $$ = $2; }

distinct_description : distinct_description ',' distinct_key_expression { $$ = $1; $1->setRootSyntaxExpr($3); }
| distinct_key_expression { $$ = ctx.getDistinctParser().createDistinctDescription(); $$->setRootSyntaxExpr($1);}
| distinct_description ',' distinct_count_expression { $$ = $1; ctx.getDistinctParser().setDistinctCount($1, *$3); delete $3; }
| distinct_count_expression { $$ = ctx.getDistinctParser().createDistinctDescription(); ctx.getDistinctParser().setDistinctCount($$, *$1); delete $1;}
| distinct_description ',' distinct_times_expression { $$ = $1; ctx.getDistinctParser().setDistinctTimes($1, *$3);delete $3; }
| distinct_times_expression { $$ = ctx.getDistinctParser().createDistinctDescription(); ctx.getDistinctParser().setDistinctTimes($$, *$1); delete $1;}
| distinct_description ',' max_item_count_expression { $$ = $1; ctx.getDistinctParser().setMaxItemCount($1, *$3);delete $3; }
| max_item_count_expression { $$ = ctx.getDistinctParser().createDistinctDescription(); ctx.getDistinctParser().setMaxItemCount($$, *$1); delete $1;}
| distinct_description ',' distinct_reserved_expression { $$ = $1; ctx.getDistinctParser().setReservedFlag($1,*$3); delete $3; }
| distinct_reserved_expression { $$ = ctx.getDistinctParser().createDistinctDescription(); ctx.getDistinctParser().setReservedFlag($$, *$1); delete $1;}
| distinct_description ',' update_total_hit_expression { $$ = $1; ctx.getDistinctParser().setUpdateTotalHitFlag($1,*$3); delete $3; }
| update_total_hit_expression { $$ = ctx.getDistinctParser().createDistinctDescription(); ctx.getDistinctParser().setUpdateTotalHitFlag($$, *$1); delete $1;}
| distinct_description ',' module_name_expression { $$ = $1; $1->setModuleName(*$3); delete $3; }
| module_name_expression { $$ = ctx.getDistinctParser().createDistinctDescription(); $$->setModuleName(*$1); delete $1;}
| distinct_description ',' dist_filter_clause { $$ = $1; $1->setFilterClause($3);}
| dist_filter_clause { $$ = ctx.getDistinctParser().createDistinctDescription(); $$->setFilterClause($1);}
| distinct_description ',' grade_thresholds_expression { $$ = $1; ctx.getDistinctParser().setGradeThresholds($1, $3);}
| grade_thresholds_expression { $$ = ctx.getDistinctParser().createDistinctDescription(); ctx.getDistinctParser().setGradeThresholds($$, $1);}


distinct_key_expression : DISTINCT_KEY ':' algebra_expression { $$ = $3; }

distinct_count_expression : DISTINCT_COUNT ':' INTEGER { $$ = $3; }

distinct_times_expression : DISTINCT_TIMES ':' INTEGER { $$ = $3; }

max_item_count_expression : DISTINCT_MAX_ITEM_COUNT ':' INTEGER { $$ = $3; }

distinct_reserved_expression : DISTINCT_RESERVED ':' IDENTIFIER { $$ = $3; }

update_total_hit_expression : DISTINCT_UPDATE_TOTAL_HIT ':' IDENTIFIER { $$ = $3; }

module_name_expression : MODULE_NAME ':' general_identifier {$$ = $3;}

dist_filter_clause : DISTINCT_FILTER_CLAUSE ':' algebra_expression {$$ = ctx.getDistinctParser().createFilterClause($3);}

grade_thresholds_expression : GRADE_THRESHOLDS ':' grade_thresholds_sub_expression {$$ = $3;}

grade_thresholds_sub_expression : grade_thresholds_term { $$ = ctx.getDistinctParser().createThresholds(); $$->push_back($1); }
				| grade_thresholds_sub_expression '|' grade_thresholds_term { $$ = $1; $1->push_back($3); }

grade_thresholds_term : INTEGER { $$ = $1; }
		      | '-' INTEGER { $2->insert(0, "-"); $$ = $2; }
 		      | FLOAT { $$ = $1; }
 		      | '-' FLOAT { $2->insert(0, "-"); $$ = $2; }

aggregate_description : aggregate_description ',' group_key_expression { $1->setGroupKeyExpr($3); $$ = $1;}
| group_key_expression { $$ = ctx.getAggregateParser().createAggDesc(); $$->setGroupKeyExpr($1);}
| aggregate_description ',' range_expression { $1->setRange(*$3); delete $3; $$ = $1; }
| range_expression { $$ = ctx.getAggregateParser().createAggDesc(); $$->setRange(*$1); delete $1;}
| aggregate_description ',' aggregate_functions { $1->setAggFunDescriptions(*$3); delete $3; $$ = $1; }
| aggregate_functions { $$ = ctx.getAggregateParser().createAggDesc(); $$->setAggFunDescriptions(*$1); delete $1; }
| aggregate_description ',' agg_filter_clause { $1->setFilterClause($3); $$ = $1;}
| agg_filter_clause { $$ = ctx.getAggregateParser().createAggDesc(); $$->setFilterClause($1); }
| aggregate_description ',' sampler_threshold { $1->setAggThreshold(*$3);$$ = $1; delete $3; }
| sampler_threshold {$$ = ctx.getAggregateParser().createAggDesc(); $$->setAggThreshold(*$1); delete $1; }
| aggregate_description ',' sampler_step { $1->setSampleStep(*$3);$$ = $1; delete $3; }
| sampler_step {$$ = ctx.getAggregateParser().createAggDesc(); $$->setSampleStep(*$1); delete $1; }
| aggregate_description ',' max_group { $1->setMaxGroupCount(*$3);$$ = $1; delete $3; }
| max_group {$$ = ctx.getAggregateParser().createAggDesc(); $$->setMaxGroupCount(*$1); delete $1; }
| aggregate_description ',' order_by { $1->setSortType(*$3); $$ = $1; delete $3; }
| order_by {$$ = ctx.getAggregateParser().createAggDesc(); $$->setSortType(*$1); delete $1; }

multi_algebra_expression : algebra_expression {
    $$ = ctx.getSyntaxExprParser().createMultiSyntax();
    $$->addSyntaxExpr($1);
}
| multi_algebra_expression ',' algebra_expression { $$ = $1; $$->addSyntaxExpr($3); }

group_key_expression : GROUP_KEY ':' algebra_expression { $$ = $3;}
| GROUP_KEY ':' '[' multi_algebra_expression ']' { $$ = $4;}

range_expression : RANGE_KEY ':' range_sub_expression { $$ = $3; }

range_sub_expression : INTEGER { $$ = ctx.getAggregateParser().createRangeExpr(); $$->push_back(*$1); delete $1;}
| FLOAT { $$ = ctx.getAggregateParser().createRangeExpr(); $$->push_back(*$1); delete $1;}
| '-' INTEGER { $$ = ctx.getAggregateParser().createRangeExpr(); $2->insert(0, "-"); $$->push_back(*$2); delete $2;}
| '-' FLOAT { $$ = ctx.getAggregateParser().createRangeExpr(); $2->insert(0, "-"); $$->push_back(*$2); delete $2;}
| range_sub_expression '~' INTEGER { $1->push_back(*$3); $$ = $1; delete $3;}
| range_sub_expression '~' FLOAT { $1->push_back(*$3); $$ = $1; delete $3; }
| range_sub_expression '~' '-' INTEGER { $4->insert(0, "-"); $1->push_back(*$4); $$ = $1; delete $4;}
| range_sub_expression '~' '-' FLOAT { $4->insert(0, "-"); $1->push_back(*$4); $$ = $1; delete $4; }

aggregate_functions : FUNCTION_KEY ':' aggregate_functions_sub { $$ = $3; }

aggregate_functions_sub :  aggregate_function { $$ = ctx.getAggregateParser().createFuncDescs(); $$->push_back($1); }
| aggregate_functions_sub '#' aggregate_function { $1->push_back($3); $$ = $1;}

aggregate_function : MAX_FUN '(' algebra_expression ')' { $$ = ctx.getAggregateParser().createAggMaxFunc($3); }
| MIN_FUN '(' algebra_expression ')' { $$ = ctx.getAggregateParser().createAggMinFunc($3); }
| COUNT '(' ')' { $$ = ctx.getAggregateParser().createAggCountFunc(); }
| SUM_FUN '(' algebra_expression ')' { $$ = ctx.getAggregateParser().createAggSumFunc($3); }
| DISTINCT_COUNT_FUN '(' algebra_expression ')' { $$ = ctx.getAggregateParser().createAggDistinctCountFunc($3); }

agg_filter_clause : AGGREGATE_FILTER ':' algebra_expression { $$ = ctx.getAggregateParser().createFilterClause($3); }

sampler_threshold : AGG_SAMPLER_THRESHOLD ':' INTEGER { $$ = $3; }

sampler_step : AGG_SAMPLER_STEP ':' INTEGER { $$ = $3; }

max_group : MAX_GROUP ':' INTEGER {$$ = $3;}
| MAX_GROUP ':' '-' INTEGER { $4->insert(0, "-"); $$ = $4;}

order_by : ORDER_BY ':' general_identifier { $$ = $3;}

rank_sort_clause : rank_sort_description {
    $$ = new isearch::common::RankSortClause();
    $$->addRankSortDesc($1);
 }
| rank_sort_clause ';' {
    $$ = $1;
  }
| rank_sort_clause ';' rank_sort_description {
    $1->addRankSortDesc($3);
    $$ = $1;
  }

rank_sort_description : sort_expressions ',' sort_percent {
    $$ = new isearch::common::RankSortDescription();
    $$->setSortDescs(*$1);
    $$->setPercent(*$3);
    delete $1;
    delete $3;
 }
| sort_percent ',' sort_expressions {
    $$ = new isearch::common::RankSortDescription();
    $$->setSortDescs(*$3);
    $$->setPercent(*$1);
    delete $1;
    delete $3;
  }

sort_percent : SORT_PERCENT ':' INTEGER { $$ = $3; }
| SORT_PERCENT ':' FLOAT { $$ = $3; }

sort_expressions : SORT_KEY ':' ordered_sort_expr { $$ = new std::vector<isearch::common::SortDescription*>; $$->push_back($3); }
| sort_expressions '#' ordered_sort_expr { $$ = $1; $$->push_back($3); }

ordered_sort_expr : '+' RANK_EXPRESSION {
    $$ = new isearch::common::SortDescription();
    $$->setExpressionType(isearch::common::SortDescription::RS_RANK);
    $$->setRootSyntaxExpr(new suez::turing::RankSyntaxExpr());
    $$->setSortAscendFlag(true);
  }
| '-' RANK_EXPRESSION {
    $$ = new isearch::common::SortDescription();
    $$->setExpressionType(isearch::common::SortDescription::RS_RANK);
    $$->setRootSyntaxExpr(new suez::turing::RankSyntaxExpr());
    $$->setSortAscendFlag(false);
  }
| RANK_EXPRESSION {
    $$ = new isearch::common::SortDescription();
    $$->setRootSyntaxExpr(new suez::turing::RankSyntaxExpr());
    $$->setExpressionType(isearch::common::SortDescription::RS_RANK);
  }
| '+' sort_expr {
    $$ = $2;
    $$->setSortAscendFlag(true);
    suez::turing::SyntaxExpr *syntaxExpr = $$->getRootSyntaxExpr();
    std::string expStr = syntaxExpr->getExprString();
    expStr = "+" + expStr;
    $$->setOriginalString(expStr);
}
| '-' sort_expr {
    $$ = $2; $$->setSortAscendFlag(false);
    suez::turing::SyntaxExpr *syntaxExpr = $$->getRootSyntaxExpr();
    std::string expStr = syntaxExpr->getExprString();
    expStr = "-" + expStr;
    $$->setOriginalString(expStr);
}
| sort_expr
{
    $$ = $1;
    suez::turing::SyntaxExpr *syntaxExpr = $$->getRootSyntaxExpr();
    std::string expStr = syntaxExpr->getExprString();
    $$->setOriginalString(expStr);
}

sort_expr :
  algebra_expression {
    $$ = new isearch::common::SortDescription();
    $$->setExpressionType(isearch::common::SortDescription::RS_NORMAL);
    $$->setRootSyntaxExpr($1);
  }

algebra_expression : arithmetic_expression  { $$ = $1; }
                  | logical_expression  { $$ = $1; }
	          | function_expr { $$ = $1; }

logical_expression : relational_expression { $$ = $1; }
      | logical_sub_expression AND logical_sub_expression {
          $$ = ctx.getSyntaxExprParser().createAndExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "");
              YYERROR;
          }
      }
      | logical_sub_expression OR logical_sub_expression {
          $$ = ctx.getSyntaxExprParser().createOrExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "");
              YYERROR;
          }
      }
      | '(' logical_expression ')' { $$ = $2; }

logical_sub_expression : logical_expression { $$ = $1; }
      | function_expr { $$ = $1; $$->setExprResultType(vt_bool); }

relational_expression : arithmetic_sub_expression EQUAL arithmetic_sub_expression
      {
          $$ = ctx.getSyntaxExprParser().createEqualExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression LESS arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createLessExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression GREATER arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createGreaterExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression LE arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createLessEqualExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression GE arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createGreaterEqualExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }

      }
      | arithmetic_sub_expression NE arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createNotEqualExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }

arithmetic_expression : INTEGER { $$ = ctx.getSyntaxExprParser().createAtomicExpr($1, suez::turing::INTEGER_VALUE); }
| FLOAT { $$ = ctx.getSyntaxExprParser().createAtomicExpr($1, suez::turing::FLOAT_VALUE); }
      | general_identifier {
          $$ = ctx.getSyntaxExprParser().createAtomicExpr($1, suez::turing::ATTRIBUTE_NAME);
          if ($$ == NULL) {
	      delete $1;
	      error(yylloc, "AttributeName not exist");
	      YYERROR;
          }
      }
      | PHRASE_STRING { $$ = ctx.getSyntaxExprParser().createAtomicExpr($1, suez::turing::STRING_VALUE); }
      | arithmetic_sub_expression '+' arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createAddExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression '-' arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createMinusExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression '*' arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createMulExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression '/' arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createDivExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;

          }
      }
      | arithmetic_sub_expression '|' arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createBitOrExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression '^' arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createBitXorExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | arithmetic_sub_expression '&' arithmetic_sub_expression {
          $$ = ctx.getSyntaxExprParser().createBitAndExpr($1, $3);
          if ($$ == NULL) {
              delete $1;
              delete $3;
              error(yylloc, "the type of left expr and right expr are not the same");
              YYERROR;
          }
      }
      | '(' arithmetic_expression ')' { $$ = $2; }
      | '-' INTEGER { $$ = ctx.getSyntaxExprParser().createAtomicExpr($2, suez::turing::INTEGER_VALUE, false); }
      | '-' FLOAT { $$ = ctx.getSyntaxExprParser().createAtomicExpr($2, suez::turing::FLOAT_VALUE, false); }

arithmetic_sub_expression : arithmetic_expression { $$ = $1; }
      | function_expr { $$ = $1; }

function_expr : general_identifier '(' ')' { $$ = ctx.getSyntaxExprParser().createFuncExpr($1); }
     | function_head ')' { $$ = $1; }
     | '(' function_expr ')' { $$ = $2; }

function_head : general_identifier '(' algebra_expression {
    $$ = ctx.getSyntaxExprParser().createFuncExpr($1);
    $$->addParam($3);
}
| function_head ',' algebra_expression { $1->addParam($3); $$ = $1; }

virtualAttribute_clause : general_identifier ':' algebra_expression {$$ = ctx.getVirtualAttributeParser().createVirtualAttributeClause(); ctx.getVirtualAttributeParser().addVirtualAttribute($$, *$1, $3); delete $1; }
| virtualAttribute_clause ';' general_identifier ':' algebra_expression {$$ = $1; ctx.getVirtualAttributeParser().addVirtualAttribute($$, *$3, $5); delete $3;}
| virtualAttribute_clause ';' {$$ = $1;}
| ';' virtualAttribute_clause {$$ = $2;}

general_identifier : IDENTIFIER { $$ = $1; }
		   | DISTINCT_KEY { $$ = new std::string("dist_key"); }
		   | DISTINCT_COUNT { $$ = new std::string("dist_count"); }
		   | DISTINCT_TIMES { $$ = new std::string("dist_times"); }
                   | DISTINCT_MAX_ITEM_COUNT { $$ = new std::string("max_item_count"); }
		   | DISTINCT_RESERVED { $$ = new std::string("reserved"); }
                   | DISTINCT_UPDATE_TOTAL_HIT { $$ = new std::string("update_total_hit"); }
		   | MODULE_NAME  { $$ = new std::string("dbmodule"); }
		   | DISTINCT_FILTER_CLAUSE { $$ = new std::string("dist_filter"); }
		   | GRADE_THRESHOLDS { $$ = new std::string("grade"); }
		   | GROUP_KEY { $$ = new std::string("group_key"); }
		   | RANGE_KEY { $$ = new std::string("range"); }
		   | FUNCTION_KEY { $$ = new std::string("agg_fun"); }
		   | MAX_FUN { $$ = new std::string("max"); }
		   | MIN_FUN { $$ = new std::string("min"); }
		   | COUNT { $$ = new std::string("count"); }
		   | SUM_FUN { $$ = new std::string("sum"); }
           | DISTINCT_COUNT_FUN { $$ = new std::string("distinct_count"); }
		   | AGGREGATE_FILTER { $$ = new std::string("agg_filter"); }
		   | AGG_SAMPLER_THRESHOLD { $$ = new std::string("agg_sampler_threshold"); }
		   | AGG_SAMPLER_STEP { $$ = new std::string("agg_sampler_step"); }
		   | MAX_GROUP { $$ = new std::string("max_group"); }
		   | ORDER_BY { $$ = new std::string("order_by"); }
		   | QUOTA_KEY { $$ = new std::string("quota"); }
                   | UNLIMITED_KEY  { $$ = new std::string("UNLIMITED");}
                   | CACHE_KEY { $$ = new std::string("key"); }
                   | CACHE_USE { $$ = new std::string("use"); }
                   | CACHE_CUR_TIME { $$ = new std::string("cur_time"); }
                   | CACHE_EXPIRE_TIME { $$ = new std::string("expire_time"); }
                   | CACHE_FILTER {$$ = new std::string("cache_filter"); }
                   | CACHE_DOC_NUM_LIMIT {$$ = new std::string("cache_doc_num_limit"); }
                   | SORT_KEY { $$ = new std::string("sort");}
                   | SORT_PERCENT { $$ = new std::string("percent");}
%%
void isearch_bison::ClauseBisonParser::error(
        const isearch_bison::ClauseBisonParser::location_type& l,
        const std::string& m)
{
    ctx.setStatus(isearch::queryparser::ClauseParserContext::SYNTAX_ERROR);
    ctx.setStatusMsg(m);
}

%{
%}


%skeleton "lalr1.cc"
%name-prefix="isearch_bison"
%debug
%start query
%defines
%define "parser_class_name" "BisonParser"



%locations

%parse-param {HA3_NS(queryparser)::Scanner &scanner}
%parse-param {HA3_NS(queryparser)::ParserContext &ctx}

%error-verbose
%debug

%code requires{
#include <stdio.h>
#include <string>
#include <vector>
#include <ha3/common/Query.h>
#include <ha3/common.h>
#include <ha3/common/Term.h>
#include <ha3/isearch.h>
#include <ha3/common/Term.h>
#include <ha3/queryparser/QueryExpr.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <ha3/queryparser/NumberTermExpr.h>
#include <ha3/queryparser/PhraseTermExpr.h>
#include <ha3/queryparser/WordsTermExpr.h>
#include <ha3/queryparser/OrTermQueryExpr.h>
#include <ha3/queryparser/AndTermQueryExpr.h>
#include <ha3/queryparser/MultiTermQueryExpr.h>
#include <limits.h>
BEGIN_HA3_NAMESPACE(queryparser);
class Scanner;
class QueryParser;
class ParserContext;
END_HA3_NAMESPACE(queryparser);
}

%union {
    int intVal;
    std::string *stringVal;
    HA3_NS(common)::RequiredFields *requiredFieldsVal;
    HA3_NS(queryparser)::QueryExpr *queryExprVal;
    HA3_NS(queryparser)::AtomicQueryExpr *atomicExprVal;    
}

%token			END	     0	"end of file"
%token <stringVal> 	NUMBER		"number"
%token <stringVal> 	IDENTIFIER      "identifier"
%token <stringVal> 	CJK_STRING      "string NOT including symbols"
%token <stringVal> 	PHRASE_STRING	"double quoted string"

%token AND OR  ANDNOT RANK EQUAL LE GE LESS GREATER WEAKAND RPARENT_COLON

%type <queryExprVal> query exprs expr and_expr or_expr andnot_expr rank_expr 
%type <queryExprVal> parenthesis_expr
%type <queryExprVal> expr_with_label
%type <atomicExprVal> atomic_expr
%type <stringVal> index_identifier 
%type <requiredFieldsVal> and_field_identifier or_field_identifier fields_identifier
%type <atomicExprVal> boost_terms boost_term core_term core_term_with_secondary_chain core_term_with_boost words_term phrase_term number_term

%destructor {delete $$;} NUMBER IDENTIFIER CJK_STRING  PHRASE_STRING
%destructor {delete $$;} expr exprs and_expr or_expr andnot_expr rank_expr 
%destructor {delete $$;} parenthesis_expr
%destructor {delete $$;} expr_with_label
%destructor {delete $$;} atomic_expr
%destructor {delete $$;} index_identifier
%destructor {delete $$;} and_field_identifier or_field_identifier fields_identifier
%destructor {delete $$;} boost_terms boost_term core_term core_term_with_secondary_chain core_term_with_boost words_term phrase_term number_term

%{
#include <ha3/queryparser/QueryParser.h>
#include <ha3/queryparser/Scanner.h>
#include <ha3/queryparser/ParserContext.h>

#undef yylex
#define yylex scanner.lex
%}

%left RANK;
%left OR;
%left AND;
%left ANDNOT;
%left '|'
%left '&'
%left WEAKAND
%%

query : multi_query END { 
    ctx.setStatus(HA3_NS(queryparser)::ParserContext::OK);
 }
multi_query : exprs { ctx.addQueryExpr($1); }
            | multi_query ';' exprs  { ctx.addQueryExpr($3); }
exprs : expr 
      | exprs expr { $$ = ctx.getParser().createDefaultOpExpr($1, $2); }

expr : expr_with_label
     | atomic_expr {$$ = $1;}
     | and_expr
     | or_expr
     | andnot_expr
     | rank_expr 
     | parenthesis_expr

expr_with_label : parenthesis_expr '@' IDENTIFIER {
         std::string *label = $3;
         $1->setLabel(*label);
         delete $3;
         $$ = $1;
       }

parenthesis_expr : '(' exprs ')' {
    $$ = $2;
 }

and_expr : expr AND expr {
    $$ = ctx.getParser().createAndExpr($1, $3);
 }

or_expr : expr OR expr {
    $$ = ctx.getParser().createOrExpr($1, $3);
 }

andnot_expr : expr ANDNOT expr {
    $$ = ctx.getParser().createAndNotExpr($1, $3);
 }

rank_expr : expr RANK expr {
        $$ = ctx.getParser().createRankExpr($1, $3);
 }

atomic_expr : boost_term
              {
		  const std::string &defaultIndexName = ctx.getParser().getDefaultIndex();
	          $1->setIndexName(defaultIndexName);
		  $$ = $1;
              }
            | index_identifier boost_terms 
              {
		  std::string *indexName = $1;
		  $2->setIndexName(*indexName);
		  delete $1;
		  $$ = $2;
              }
            | fields_identifier boost_terms
              {
                  const std::string &defaultIndexName = ctx.getParser().getDefaultIndex();
                  $2->setIndexName(defaultIndexName);
                  $2->setRequiredFields(*$1);
                  delete $1;
                  $$ = $2;
              }
            | IDENTIFIER fields_identifier boost_terms
              {
                  std::string *indexName = $1;
                  $3->setIndexName(*indexName);
                  $3->setRequiredFields(*$2);
                  delete $1;
                  delete $2;
                  $$ = $3;
              }


boost_terms : boost_term
              {
                  $$ = $1;
              }
            | boost_terms '|' boost_terms
              {
                  $$ = ctx.getParser().createMultiTermQueryExpr($1, $3, OP_OR);
              }
            | boost_terms '&' boost_terms
              {
                  $$ = ctx.getParser().createMultiTermQueryExpr($1, $3, OP_AND);
              }
            | boost_terms WEAKAND boost_terms
              {
                  $$ = ctx.getParser().createMultiTermQueryExpr($1, $3, OP_WEAKAND);
              }
            | '(' boost_terms ')' '^' NUMBER
              {
                  uint32_t i = atoi($5->c_str());
                  delete $5;
                  auto expr = dynamic_cast<HA3_NS(queryparser)::MultiTermQueryExpr*>($2);
                  if (expr) {
                      expr->setMinShouldMatch(i);
                  }
                  $$ = $2;
              }
            | '(' boost_terms ')'
              {
                  $$ = $2;
              }

boost_term : core_term
           | core_term_with_secondary_chain
           | core_term_with_boost
             { $$ = $1; }
           | core_term_with_secondary_chain '^' NUMBER
             {
                 int i = atoi($3->c_str());
                 delete $3;
                 $1->setBoost(i);
                 $$ = $1;
             }
           | core_term_with_boost '#' IDENTIFIER
             {
                 std::string *secondaryChain = $3;
                 $1->setSecondaryChain(*secondaryChain);
                 delete $3;
                 $$ = $1;
             }

core_term_with_secondary_chain : core_term '#' IDENTIFIER
            {
                 std::string *secondaryChain = $3;
                 $1->setSecondaryChain(*secondaryChain);
                 delete $3;
                 $$ = $1;
             }

core_term_with_boost : core_term '^' NUMBER
            {
                int i = atoi($3->c_str());
                 delete $3;
                 $1->setBoost(i);
                 $$ = $1;
            }

core_term : words_term
          | phrase_term 
          | number_term 
            { $$ = $1; }

words_term : IDENTIFIER
             {
                 $$ = ctx.getParser().createWordsTermExpr($1); 
             }
           | CJK_STRING
             { $$ = ctx.getParser().createWordsTermExpr($1); }

phrase_term : PHRASE_STRING { $$ = ctx.getParser().createPhraseTermExpr($1); }

number_term : NUMBER 
              { 
                  $$ = ctx.getParser().createWordsTermExpr($1); 
                  if (!$$) {
                      YYERROR;
                  }
              }
            | EQUAL NUMBER 
              { 
                  $$ = ctx.getParser().createWordsTermExpr($2); 
                  if (!$$) {
                      YYERROR;
                  }
              }
            | LESS NUMBER
              { 
                  $$ = ctx.getParser().createNumberTermExpr(NULL, true, $2, false, &ctx);
                  if (!$$) {
                      YYERROR;
                  }                  
              }
            | LE NUMBER
              { 
                  $$ = ctx.getParser().createNumberTermExpr(NULL, true, $2, true, &ctx);
                  if (!$$) {
                      YYERROR;
                  }                  
              }
            | GREATER NUMBER
              { 
                  $$ = ctx.getParser().createNumberTermExpr($2, false, NULL, true, &ctx);
                  if (!$$) {
                      YYERROR;
                  }                  
              }
            | GE NUMBER
              { 
                  $$ = ctx.getParser().createNumberTermExpr($2, true, NULL, true, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }
	    | '[' NUMBER ',' ')'
              { 
                  $$ = ctx.getParser().createNumberTermExpr($2, true, NULL, true, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }
	    | '(' NUMBER ',' ')'
              { 
                  $$ = ctx.getParser().createNumberTermExpr($2, false, NULL, true, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }
	    | '(' ',' NUMBER ']'
              { 
                  $$ = ctx.getParser().createNumberTermExpr(NULL, true, $3, true, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }
	    | '(' ',' NUMBER ')'
              { 
                  $$ = ctx.getParser().createNumberTermExpr(NULL, true, $3, false, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }	    
            | '[' NUMBER ',' NUMBER ']'
              { 
                  $$ = ctx.getParser().createNumberTermExpr($2, true, $4, true, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }
            | '[' NUMBER ',' NUMBER ')'
              { 
                  $$ = ctx.getParser().createNumberTermExpr($2, true, $4, false, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }
            | '(' NUMBER ',' NUMBER ')'
              {
                  $$ = ctx.getParser().createNumberTermExpr($2, false, $4, false, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }
            | '(' NUMBER ',' NUMBER ']'
              { 
                  $$ = ctx.getParser().createNumberTermExpr($2, false, $4, true, &ctx);
                  if (!$$) {
                      YYERROR;
                  }
              }

and_field_identifier : IDENTIFIER RPARENT_COLON
                       {
                           $$ = new HA3_NS(common)::RequiredFields;
                           $$->isRequiredAnd = true;
                           std::string *field = $1;                       
                           $$->fields.push_back(*field);
                           delete $1;
                       }
                       | IDENTIFIER ',' and_field_identifier
                       {
                           $$ = $3;
                           std::string *field = $1;
                           $$->fields.push_back(*field);
                           delete $1;
                       }

or_field_identifier : IDENTIFIER ']' ':'
                      {
                          $$ = new HA3_NS(common)::RequiredFields;
                          $$->isRequiredAnd = false;
                          std::string *field = $1;                       
                          $$->fields.push_back(*field);
                          delete $1;
                      }
                      | IDENTIFIER ',' or_field_identifier
                      {
                          $$ = $3;
                          std::string *field = $1;
                          $$->fields.push_back(*field);
                          delete $1;
                      }

fields_identifier : '(' and_field_identifier
                    {
                        $$ = $2;
                    }
                    | '[' or_field_identifier
                    {
                        $$ = $2;
                    }

index_identifier : IDENTIFIER ':'  
                   { 
                       $$ = $1;
                   }

%% 
void isearch_bison::BisonParser::error(
        const isearch_bison::BisonParser::location_type& l,
        const std::string& m)
{
    ctx.setStatus(HA3_NS(queryparser)::ParserContext::SYNTAX_ERROR);
    ctx.setStatusMsg(m);
}

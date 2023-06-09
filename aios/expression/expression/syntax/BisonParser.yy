%{
%}

%skeleton "lalr1.cc"
%name-prefix="syntax_bison"
%debug
%start expression_final
%defines
%define "parser_class_name" "BisonParser"

%locations

%parse-param {expression::Scanner &scanner}
%parse-param {expression::SyntaxParser::ParseState &state}
%parse-param {bool &enableSyntaxParseOpt}

%error-verbose
%debug

%code requires {
#include "expression/syntax/SyntaxExpr.h"
#include "expression/syntax/FuncSyntaxExpr.h"
#include "expression/syntax/SyntaxFactory.h"
#include "expression/syntax/SyntaxParser.h"
    namespace expression {
    class Scanner;
    }

}
                        
%union {
    std::string *stringVal;
    expression::SyntaxExpr *syntaxExprVal;
    expression::FuncSyntaxExpr *funcExprVal;
    std::vector<expression::SyntaxExpr*> *paramVal;
}

%token			END	     0	"end of file"
%token <stringVal> 	INTEGER  	"integer"
%token <stringVal> 	FLOAT		"float"
%token <stringVal> 	IDENTIFIER      "identifier"
%token <stringVal> 	PHRASE_STRING      "string NOT including symbols"

%token AND OR EQUAL LE GE LESS GREATER NE

%type <syntaxExprVal> expression single_expression attribute_name
%type <funcExprVal> function_expr
%type <paramVal> func_param

%destructor {delete $$;} IDENTIFIER PHRASE_STRING INTEGER FLOAT
%destructor {delete $$;} expression single_expression attribute_name
%destructor {delete $$;} function_expr
%destructor {for(auto p : *$$) { delete p; } delete $$;} func_param

%{

#include "expression/syntax/Scanner.h"

#undef yylex
#define yylex scanner.lex
    using namespace expression;
%}

%left OR
%left AND
%left '|'
%left '^'
%left '&' '@'
%left EQUAL NE
%left LESS GREATER LE GE
%left '+' '-'
%left '*' '/'
%%

expression_final : expression END {
    state.setSyntax($1);
}

expression : single_expression  { $$ = $1; }
| '(' expression ')' { $$ = $2; }
| expression '+' expression { $$ = SyntaxFactory::createAddExpr($1, $3); }
| expression '-' expression { $$ = SyntaxFactory::createMinusExpr($1, $3); }
| expression '*' expression { $$ = SyntaxFactory::createMulExpr($1, $3); }
| expression '/' expression { $$ = SyntaxFactory::createDivExpr($1, $3); }
| expression AND expression { $$ = SyntaxFactory::createAndExpr($1, $3, enableSyntaxParseOpt); }
| expression OR expression { $$ = SyntaxFactory::createOrExpr($1, $3, enableSyntaxParseOpt); }
| expression EQUAL expression { $$ = SyntaxFactory::createEqualExpr($1, $3); }
| expression LESS expression { $$ = SyntaxFactory::createLessExpr($1, $3); }
| expression GREATER expression { $$ = SyntaxFactory::createGreaterExpr($1, $3); }
| expression NE expression { $$ = SyntaxFactory::createNotEqualExpr($1, $3); }
| expression LE expression { $$ = SyntaxFactory::createLessEqualExpr($1, $3); }
| expression GE expression { $$ = SyntaxFactory::createGreaterEqualExpr($1, $3); }
| expression '|' expression { $$ = SyntaxFactory::createBitOrExpr($1, $3); }
| expression '&' expression { $$ = SyntaxFactory::createBitAndExpr($1, $3); }
| expression '@' expression { $$ = SyntaxFactory::createBitAndExpr($1, $3); }
| expression '^' expression { $$ = SyntaxFactory::createBitXorExpr($1, $3); }


single_expression : INTEGER { $$ = SyntaxFactory::createConstExpr($1, INTEGER_VALUE, true); }
| '-' INTEGER { $$ = SyntaxFactory::createConstExpr($2, INTEGER_VALUE, false); }
| FLOAT { $$ = SyntaxFactory::createConstExpr($1, FLOAT_VALUE, true); }
| '-' FLOAT { $$ = SyntaxFactory::createConstExpr($2, FLOAT_VALUE, false); }
| PHRASE_STRING { $$ = SyntaxFactory::createConstExpr($1, STRING_VALUE); }
| attribute_name { $$ = $1; }
| function_expr { $$ = $1; }

function_expr : IDENTIFIER '(' ')' { $$ = SyntaxFactory::createFuncExpr($1); }
| IDENTIFIER '(' func_param ')' { $$ = SyntaxFactory::createFuncExpr($1, $3);}

attribute_name : IDENTIFIER ':' IDENTIFIER { $$ = SyntaxFactory::createAttributeExpr($3, $1); }
| IDENTIFIER { $$ = SyntaxFactory::createAttributeExpr($1, NULL); }

func_param : expression { $$ = new std::vector<SyntaxExpr*>; $$->push_back($1); }
     | func_param ',' expression { $1->push_back($3); $$ = $1; }

%%
void syntax_bison::BisonParser::error(const syntax_bison::BisonParser::location_type& l,
                                      const std::string& m)
{
    std::stringstream ss;
    std::string context = scanner.orgContext();
    ss << "'" << std::string(context.data(), context.data() + l.begin.column) << "' " << m;
    state.fail(ss.str());
}


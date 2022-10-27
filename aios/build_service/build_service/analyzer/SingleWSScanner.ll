%{
#include "build_service/analyzer/SingleWSScanner.h"
#include "build_service/util/Log.h"
#include <string>

#undef YY_DECL
#define	YY_DECL							\
    bool build_service::analyzer::SingleWSScanner::lex(         \
    	const char * &pText,					\
        int &length						\
    )
%}

%option c++
%option noyywrap
%option yyClass="SingleWSScanner"

%option debug
%x quoted double_quoted

DIGIT            [0-9]
FULL_WIDTH_DIGIT (\xef\xbc[\x90-\x99])
ALPHABET	 [a-zA-Z]
FULL_WIDTH_ALPHABET (\xef\xbc[\xa1-\xba])|(\xef\xbd[\x81-\x9a])
FLOAT_SEPERATOR [./]
SYMBOL_ALPHABET	 [&+\']
FULL_WIDTH_SYMBOL_ALPHABET   (\xef\xbc\x86)|(\xef\xbc\x87)|(\xef\xbc\x8b)|(\xe2\x80[\x98-\x99])
HTML_BEGIN (&)|(\xef\xbc\x86)
HTML_END (;)|(\xef\xbc\x9b)

UTF8_1 [\x00-\x7f]
UTF8_2 ([\xc2-\xdf][\x80-\xbf])
UTF8_3 ([\xe0-\xef][\x80-\xbf][\x80-\xbf])
UTF8_4 ([\xf0-\xf4][\x80-\xbf][\x80-\xbf][\x80-\xbf])

%%

({DIGIT}|{FULL_WIDTH_DIGIT})+{FLOAT_SEPERATOR}({DIGIT}|{FULL_WIDTH_DIGIT})+ {
    pText = YYText();
    length = YYLeng();
    _currPos += length;
    _isStopWord = false;
    return true;
}


({DIGIT}|{FULL_WIDTH_DIGIT})+ {
    pText = YYText();
    length = YYLeng();
    _currPos += length;
    _isStopWord = false;
    return true;
}


({ALPHABET}*){HTML_BEGIN}({ALPHABET}|{FULL_WIDTH_ALPHABET}){2,6}{HTML_END}({ALPHABET}*) {
    pText = YYText();
    length = YYLeng();
    _currPos += length;
    _isStopWord = false;
    return true;
}


({SYMBOL_ALPHABET}|{FULL_WIDTH_SYMBOL_ALPHABET})+ {
    BS_LOG(DEBUG, "ignore seperator char [%s]", YYText());
    pText = YYText();
    length = YYLeng();
    _currPos += length;
    _isStopWord = true;
    return true;
}


({ALPHABET}|{FULL_WIDTH_ALPHABET}|{SYMBOL_ALPHABET}|{FULL_WIDTH_SYMBOL_ALPHABET})+ {
    pText = YYText();
    length = YYLeng();
    _currPos += length;
    _isStopWord = false;
    return true;
}


{UTF8_1}|{UTF8_2}|{UTF8_3}|{UTF8_4} {
    pText = YYText();
    length = YYLeng();
    _currPos += length;
    _isStopWord = false;
    return true;
}

. {
    _invalidCharPosVec.push_back(_currPos);
    _currPos += YYLeng();
    BS_LOG(WARN, "invalid UTF8 char [%x]", *(unsigned char*)YYText());
    pText = NULL;
    length = 0;
    _isStopWord = false;
    return true;
}

<<EOF>> {
    pText = NULL;
    length = 0;
    _isStopWord = false;
    return false;
}

%%
namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, SingleWSScanner);

};
};

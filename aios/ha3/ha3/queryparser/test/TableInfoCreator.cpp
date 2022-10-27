#include <ha3/queryparser/test/TableInfoCreator.h>
#include <suez/turing/expression/util/TypeTransformer.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <autil/StringTokenizer.h>
#include <string>
#include <vector>

using namespace std;
using namespace autil;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, TableInfoCreator);

TableInfoCreator::TableInfoCreator() { 
}

TableInfoCreator::~TableInfoCreator() { 
}

void TableInfoCreator::
prepareTableInfo(TableInfoPtr tableInfoPtr, 
                 const string& attrNames, const string& attrTypes) 
{
    FieldInfos *fieldInfos = new FieldInfos;
    AttributeInfos *attrInfos = new AttributeInfos;

    StringTokenizer attrNamesTokenizer(attrNames, ",", 
            StringTokenizer::TOKEN_IGNORE_EMPTY |
            StringTokenizer::TOKEN_TRIM);

    StringTokenizer attrTypesTokenizer(attrTypes, ",", 
            StringTokenizer::TOKEN_IGNORE_EMPTY |
            StringTokenizer::TOKEN_TRIM);

    assert(attrNamesTokenizer.getNumTokens() == attrTypesTokenizer.getNumTokens());

    StringTokenizer::TokenVector::const_iterator ix = attrTypesTokenizer.begin();
    for (StringTokenizer::TokenVector::const_iterator it = attrNamesTokenizer.begin();
         it != attrNamesTokenizer.end(); ++it, ++ix) 
    {
        FieldInfo *fieldInfo = new FieldInfo;
        fieldInfo->fieldName = *it;
        fieldInfo->fieldType = TypeTransformer::transform(*ix);
        fieldInfos->addFieldInfo(fieldInfo);
        AttributeInfo *attrInfo = new AttributeInfo;
        attrInfo->setAttrName(*it);
        attrInfo->setFieldInfo(*fieldInfo);
        attrInfos->addAttributeInfo(attrInfo);
    }
    tableInfoPtr->setAttributeInfos(attrInfos);
    tableInfoPtr->setFieldInfos(fieldInfos);
}

END_HA3_NAMESPACE(queryparser);


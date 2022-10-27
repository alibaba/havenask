#include <unistd.h>
#include <errno.h>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <iomanip>
#include <tr1/memory>
#include <readline/readline.h>
#include <readline/history.h>
#include <fslib/fslib.h>
#include <autil/TimeUtility.h>
#include <autil/legacy/any.h>
#include <autil/StringTokenizer.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/index_base/schema_adapter.h>
#include "tools/document_reader_tools/DocumentPrinter.h"

using namespace std;

using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);

namespace build_service {
namespace tools {

BS_LOG_SETUP(tools, DocumentPrinter);

DocumentPrinter::DocumentPrinter()
{}

DocumentPrinter::~DocumentPrinter()
{}

bool DocumentPrinter::Init(const string& filter,
                           const string& display,
                           const string& schemaFile,
                           const string& pkStr)
{
    vector<string> attrs = StringTokenizer::tokenize(ConstString(filter), ";");
    for (auto iter = attrs.begin(); iter != attrs.end(); iter++)
    {
        vector<string> keyValue = StringTokenizer::tokenize(ConstString(*iter), "=");
        if (keyValue.size() != 2)
        {
            BS_LOG(ERROR, "format of filter is invalid, [%s]", filter.c_str());
            return false;
        }
        vector<string> values = StringTokenizer::tokenize(ConstString(keyValue[1]), ",");
        set<string> valueSet;
        valueSet.insert(values.begin(), values.end());
        mAttrFilter.insert(make_pair(keyValue[0], valueSet));
    }

    string schemaStr;
    FileSystemWrapper::AtomicLoad(schemaFile, schemaStr);
    Any any = ParseJson(schemaStr);
    IndexPartitionSchema* schema = new IndexPartitionSchema("noname");
    FromJson(schema, any);
    mSchema.reset(schema);

    vector<string> displayAttrs = StringTokenizer::tokenize(ConstString(display), ",");
    mDisplayAttrSet.insert(displayAttrs.begin(), displayAttrs.end());
    mPkStr = pkStr;
    return true;
}

bool DocumentPrinter::PrintDocument(const NormalDocumentPtr& document,
                                    stringstream& out) const
{
    string pk = document->GetPrimaryKey();
    if (!mPkStr.empty())
    {
        if (mPkStr == pk)
        {
            out << "pk= " << pk << ", docOptype = " << document->GetDocOperateType()
                <<" docOriginalOpType = " << document->GetOriginalOperateType() << endl;
            return true;
        }
        return false;
    }

    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    AttributeDocumentPtr attrDoc = document->GetAttributeDocument();
    for (auto iter = attrSchema->Begin(); iter != attrSchema->End(); iter++)
    {
        AttributeConfigPtr attrConfig = *iter;
        fieldid_t fid = attrConfig->GetFieldId();
        string attrName = attrConfig->GetAttrName();
        const ConstString attrValue = attrDoc->GetField(fid);
        FieldType ft = attrConfig->GetFieldType();
        string attrValueStr = ConvertAttrValue(ft, attrValue);
        
        auto attrIter = mAttrFilter.find(attrName);
        if (attrIter != mAttrFilter.end())            
        {
            auto valueIter = attrIter->second.find(attrValueStr);
            if (valueIter == attrIter->second.end())
            {
                // attr value not match, skip this doc
                return false;                
            }
        }

        if (mDisplayAttrSet.size() > 0 &&
            mDisplayAttrSet.find(attrName) == mDisplayAttrSet.end())
        {
            continue;
        }
        out << attrName << ":" << attrValueStr << endl;
    }
    return true;
}

string DocumentPrinter::ConvertAttrValue(FieldType ft, const ConstString& attrValue) const
{
    if (attrValue.size() == 0)
    {
        return "null";
    }
#define CONVERT_ATTR_VALUE(ft_type, type)            \
    case ft_type:                                    \
    {                                                \
        type tmp;                                    \
        memcpy(&tmp, attrValue.data(), sizeof(tmp)); \
        return StringUtil::toString(tmp);            \
    }
    switch (ft)
    {
        CONVERT_ATTR_VALUE(ft_int8, int8_t);
        CONVERT_ATTR_VALUE(ft_uint8, uint8_t);
        CONVERT_ATTR_VALUE(ft_int16, int16_t);
        CONVERT_ATTR_VALUE(ft_uint16, uint16_t);
        CONVERT_ATTR_VALUE(ft_int32, int32_t);
        CONVERT_ATTR_VALUE(ft_uint32, uint32_t);
        CONVERT_ATTR_VALUE(ft_int64, int64_t);
        CONVERT_ATTR_VALUE(ft_uint64, uint64_t);
        CONVERT_ATTR_VALUE(ft_float, float);
        CONVERT_ATTR_VALUE(ft_double, double);
        CONVERT_ATTR_VALUE(ft_line, double);
        CONVERT_ATTR_VALUE(ft_location, double);
        CONVERT_ATTR_VALUE(ft_polygon, double);
    default:
        return string(attrValue.data(), attrValue.size());
    }
}

}
}

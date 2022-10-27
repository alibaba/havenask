#ifndef ISEARCH_XMLFORMATUTIL_H
#define ISEARCH_XMLFORMATUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <sstream>
#include <autil/StringUtil.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>

BEGIN_HA3_NAMESPACE(util);

template <typename T>
struct DirectOutput{
    static const bool DIRECT_OUTPUT = false;
};

#define DECLARE_DIRECT_OUTPUT_HELPER(x,y)              \
    template <>                                         \
    struct DirectOutput<x>{                             \
        static const bool DIRECT_OUTPUT = (y);          \
    }    
 

DECLARE_DIRECT_OUTPUT_HELPER(int8_t, true);
DECLARE_DIRECT_OUTPUT_HELPER(uint8_t, true);
DECLARE_DIRECT_OUTPUT_HELPER(int16_t, true);
DECLARE_DIRECT_OUTPUT_HELPER(uint16_t, true);
DECLARE_DIRECT_OUTPUT_HELPER(int32_t, true);
DECLARE_DIRECT_OUTPUT_HELPER(uint32_t, true);
DECLARE_DIRECT_OUTPUT_HELPER(int64_t, true);
DECLARE_DIRECT_OUTPUT_HELPER(uint64_t, true);
DECLARE_DIRECT_OUTPUT_HELPER(float, true);
DECLARE_DIRECT_OUTPUT_HELPER(double, true);
DECLARE_DIRECT_OUTPUT_HELPER(bool, true);
DECLARE_DIRECT_OUTPUT_HELPER(std::string, true);

#undef DECLARE_DIRECT_OUTPUT_HELPER

class XMLFormatUtil
{
    template<bool>
    struct SupportTraits{
    };
    typedef SupportTraits<true> Support;
    typedef SupportTraits<false> Unsupport;
public:
    XMLFormatUtil();
    ~XMLFormatUtil();
private:
    XMLFormatUtil(const XMLFormatUtil &);
    XMLFormatUtil& operator=(const XMLFormatUtil &);
public:
    //0x00-0x08 0x0b-0x0c 0x0e-0x1f
    static inline bool isInvalidXMLChar(const char &c);
    static inline void escapeFiveXMLEntityReferences(const std::string &str, std::stringstream &ss);
    static inline void formatCdataValue(const std::string &str, 
            std::stringstream& ss);
    template<typename T> 
    static inline void toXMLString(const T &value, 
                            std::stringstream& oss);
    template<typename T> 
    static inline void toXMLString(const std::vector<T> &value, 
                            std::stringstream& oss);
private:
    template<typename T> 
    static inline void toXMLString(const T &value, 
                                   std::stringstream& oss, 
                                   Unsupport);
    template<typename T> 
    static inline void toXMLString(const T &value, 
                                   std::stringstream& oss, 
                                   Support);
    template<typename T> 
    static inline void toXMLString(const std::vector<T> &value, 
                                   std::stringstream& oss,
                                   Unsupport);
    template<typename T> 
    static inline void toXMLString(const std::vector<T> &value, 
                                   std::stringstream& oss,
                                   Support);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(XMLFormatUtil);

inline bool XMLFormatUtil::isInvalidXMLChar(const char &c) {
    if ( (0x00 <= c && c <= 0x08) ||
        (0x0b <= c && c <= 0x0c) ||
        (0x0e <= c && c <= 0x1f) ) 
    {
        return true;
    }
    return false;
}


inline void XMLFormatUtil::escapeFiveXMLEntityReferences(const std::string &str,
        std::stringstream &ss)
{
    size_t pos = 0;
    for (; pos < str.length(); pos ++) {
        switch (str[pos]) {
        case '&':
            ss << "&amp;";
            break;
        case '<':
            ss << "&lt;";
            break;
        case '>':
            ss << "&gt;";
            break;
        case '"':
            ss << "&quot;";
            break;
        case '\'':
            ss << "&apos;";
            break;
        default:
            ss << str[pos];
        }
    }
}

inline void XMLFormatUtil::formatCdataValue(const std::string &str, 
        std::stringstream& ss)
{
    static const std::string cdataBeginStr = "<![CDATA[";
    static const std::string cdataEndStr = "]]>";
    ss << cdataBeginStr;
        
    size_t pos = 0;
    while (pos < str.length()) {
        if (isInvalidXMLChar(str[pos])) {
            ss << '\t';
        } else if (str[pos] == ']') {
            ss << str[pos];
            if (pos + 2 < str.length() && 
                str[pos + 1] == ']' && 
                str[pos + 2] == '>') 
            {
                ss << "]&gt;";
                pos += 2;
            }
        } else {
            ss << str[pos];
        }
        pos++;
    }
    ss << cdataEndStr;
}

template<typename T> 
inline void XMLFormatUtil::toXMLString(const std::vector<T> &value, 
                               std::stringstream& oss)
{
    toXMLString(value, oss, SupportTraits< DirectOutput<T>::DIRECT_OUTPUT >());
}

template<typename T> 
inline void XMLFormatUtil::toXMLString(const std::vector<T> &value, 
                                       std::stringstream& oss,
                                       Unsupport)
{
    std::string temp = autil::StringUtil::toString(value);
    XMLFormatUtil::formatCdataValue(temp, oss);
}

template<typename T> 
inline void XMLFormatUtil::toXMLString(const std::vector<T> &value, 
                                       std::stringstream& oss,
                                       Support)
{
    for (typename std::vector<T>::const_iterator it = value.begin();
         it != value.end(); ++it)
    {
        if (it != value.begin()) oss << " ";
        oss << *it;
    }
}

template<> 
inline void XMLFormatUtil::toXMLString<int8_t>(const std::vector<int8_t> &value, 
        std::stringstream& oss,
        Support)
{
    for (std::vector<int8_t>::const_iterator it = value.begin();
         it != value.end(); ++it)
    {
        int32_t temp = *it;
        if (it != value.begin()) oss << " ";
        oss << temp;
    }
}


template<> 
inline void XMLFormatUtil::toXMLString<uint8_t>(const std::vector<uint8_t> &value, 
        std::stringstream& oss,
        Support)
{
    for (std::vector<uint8_t>::const_iterator it = value.begin();
         it != value.end(); ++it)
    {
        uint32_t temp = *it;
        if (it != value.begin()) oss << " ";
        oss << temp;
    }
}

template<> 
inline void XMLFormatUtil::toXMLString<std::string>(const std::vector<std::string> &value, 
        std::stringstream& oss,
        Support)
{
    std::string temp; 
    for (std::vector<std::string>::const_iterator it = value.begin();
         it != value.end(); ++it)
    {
        if (it != value.begin()) temp += " ";
        temp += *it;
    }
    XMLFormatUtil::formatCdataValue(temp, oss);
}



template<typename T> 
inline void XMLFormatUtil::toXMLString(const T &value, std::stringstream &oss)
{
    toXMLString(value, oss, SupportTraits< DirectOutput<T>::DIRECT_OUTPUT >());
}

template<typename T> 
inline void XMLFormatUtil::toXMLString(const T &value, 
                                       std::stringstream &oss,
                                       Unsupport)
{
    std::string temp = autil::StringUtil::toString(value);
    XMLFormatUtil::formatCdataValue(temp, oss);
}

template<typename T> 
inline void XMLFormatUtil::toXMLString(const T &value, 
                                       std::stringstream &oss,
                                       Support)
{
    oss << value;
}

template<> 
inline void XMLFormatUtil::toXMLString<int8_t>(const int8_t &value, 
        std::stringstream &oss,
        Support)
{
    toXMLString<int32_t>((int32_t)value, oss, Support() );
}

template<> 
inline void XMLFormatUtil::toXMLString<uint8_t>(const uint8_t &value, 
        std::stringstream &oss,
        Support)
{
    toXMLString<uint32_t>((uint32_t)value, oss, Support() );
}

template<> 
inline void XMLFormatUtil::toXMLString<std::string>(const std::string &value,
        std::stringstream &oss, Support)
{
    XMLFormatUtil::formatCdataValue(value, oss);
}

END_HA3_NAMESPACE(util);

#endif //ISEARCH_XMLFORMATUTIL_H

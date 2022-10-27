#include <ha3/util/IPListParser.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, IPListParser);

const string IPListParser::IP_LIST_SEPARATOR = ";";
const string IPListParser::TYPED_IP_LIST_SEPARATOR = "|";
const string IPListParser::TYPED_IP_LIST_BEGIN = "{";
const string IPListParser::TYPED_IP_LIST_END = "}";

IPListParser::IPListParser() { 
}

IPListParser::~IPListParser() { 
}

bool IPListParser::parseIPList(const string &ipListStr, set<string> &ipList,
                               map<string, set<string> > &typedIpList)
{
    StringTokenizer st(ipListStr, TYPED_IP_LIST_SEPARATOR,
                       StringTokenizer::TOKEN_TRIM |
                       StringTokenizer::TOKEN_IGNORE_EMPTY);

    set<string> ipToRemoveSet;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        string type;
        string list;
        if (!parseIPListType(st[i], type, list)) {
            return false;
        }
        
        StringTokenizer st2(list, IP_LIST_SEPARATOR,
                            StringTokenizer::TOKEN_TRIM |
                            StringTokenizer::TOKEN_IGNORE_EMPTY);
        set<string> tmpIpSet;
        for (size_t j = 0; j < st2.getNumTokens(); ++j) {
            const string &ip = st2[j];
            assert(!ip.empty());
            if (ip[0] == '-') {
                string toRemoveIp = ip.substr(1);
                if (!addIPList(toRemoveIp, ipToRemoveSet)) {
                    return false;
                }
            } else {
                if (!addIPList(ip, tmpIpSet)) {
                    return false;
                }            
            }
        }
        
        if (!type.empty()) {
            typedIpList[type] = tmpIpSet;
        }
        ipList.insert(tmpIpSet.begin(), tmpIpSet.end());
    }

    for (map<string, set<string> >::iterator it = typedIpList.begin();
         it != typedIpList.end(); ++it)
    {
        removeIPList(it->second, ipToRemoveSet);
    }
    removeIPList(ipList, ipToRemoveSet);
    
    return true;
}

bool IPListParser::addIPList(const string &ip, set<string> &ipSet)
{
    StringTokenizer st(ip, ".", StringTokenizer::TOKEN_TRIM |
                       StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 4) {
        HA3_LOG(WARN, "invalid ip address: [%s]", ip.c_str());
        return false;
    }
    for (size_t i = 0; i < 3; ++i) {
        if (!validateIPNum(st[i])) {
            HA3_LOG(WARN, "invalid ip address: [%s]", ip.c_str());
            return false;
        }
    }
    const string &ipNum4 = st[3];
    if (ipNum4[0] == '[') {
        if ((*ipNum4.rbegin()) != ']') {
            HA3_LOG(WARN, "invalid ip address: [%s]", ip.c_str());
            return false;
        }
        string ipNum = ipNum4.substr(1, ipNum4.length() - 2); // remove '[', ']'
        StringTokenizer st2(ipNum, "-", StringTokenizer::TOKEN_TRIM |
                            StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (st2.getNumTokens() != 2) {
            HA3_LOG(WARN, "invalid ip address range: [%s]", ip.c_str());
            return false;
        }
        if (!validateIPNum(st2[0]) || !validateIPNum(st2[1])) {
            HA3_LOG(WARN, "invalid ip address range: [%s]", ip.c_str());
            return false;
        }
        int32_t from;
        int32_t to;
        StringUtil::strToInt32(st2[0].c_str(), from);
        StringUtil::strToInt32(st2[1].c_str(), to);
        if (from > to) {
            HA3_LOG(WARN, "invalid ip address range, from is bigger than to: [%s]", ip.c_str());
            return false;
        }
        string prefix = st[0] + '.' + st[1] + '.' + st[2] + '.';
        for (int32_t i = from; i <= to; ++i) {
            ipSet.insert(prefix + StringUtil::toString(i));
        }
    } else {
        if (!validateIPNum(ipNum4)) {
            HA3_LOG(WARN, "invalid ip address: [%s]", ip.c_str());
            return false;
        }
        ipSet.insert(ip);
    }
    return true;
}

void IPListParser::removeIPList(set<string> &ipSet,
                                const set<string> &ipToRemoveSet)
{
    for (set<string>::const_iterator it = ipToRemoveSet.begin();
         it != ipToRemoveSet.end(); ++it)
    {
        ipSet.erase(*it);
    }
}

bool IPListParser::validateIPNum(const string &ipNumStr) {
    int32_t value;
    if (!StringUtil::strToInt32(ipNumStr.c_str(), value)) {
        return false;
    }
    if (value < 0 || value > 255) {
        return false;            
    }
    return true;
}

bool IPListParser::parseIPListType(const string &str, string &type,
                                   string &list)
{
    StringTokenizer st(str, TYPED_IP_LIST_BEGIN,
                       StringTokenizer::TOKEN_TRIM
                       | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() == 1) {
        type = "";
        list = st[0];
        return true;
    }
    
    if (st.getNumTokens() != 2) {
        HA3_LOG(WARN, "invalid ip list: [%s]", str.c_str());
        return false;
    }

    type = st[0];
    list = st[1];
    size_t pos = list.find(TYPED_IP_LIST_END);
    if (pos == string::npos
        || pos != list.length() - TYPED_IP_LIST_END.length())
    {
        HA3_LOG(WARN, "invalid ip list: [%s]", str.c_str());
        return false;        
    }
    list = list.substr(0, list.length() - TYPED_IP_LIST_END.length());
    return true;
}

END_HA3_NAMESPACE(util);


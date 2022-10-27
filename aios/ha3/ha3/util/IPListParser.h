#ifndef ISEARCH_IPLISTPARSER_H
#define ISEARCH_IPLISTPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

class IPListParser
{
private:
    static const std::string IP_LIST_SEPARATOR;
    static const std::string TYPED_IP_LIST_SEPARATOR;
    static const std::string TYPED_IP_LIST_BEGIN;
    static const std::string TYPED_IP_LIST_END;
public:
    IPListParser();
    ~IPListParser();
private:
    IPListParser(const IPListParser &);
    IPListParser& operator=(const IPListParser &);

public:
    static bool parseIPList(
            const std::string &ipListStr,
            std::set<std::string> &ipList,
            std::map<std::string, std::set<std::string> > &typedIpList);

private:
    static bool parseIPListType(const std::string &str, std::string &type,
                                std::string &list);
        
    static bool addIPList(const std::string &ip,
                          std::set<std::string> &ipSet);

    static void removeIPList(std::set<std::string> &ipSet,
                             const std::set<std::string> &ipToRemoveSet);
                             
    
    static bool validateIPNum(const std::string &ipNumStr);


private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(IPListParser);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_IPLISTPARSER_H

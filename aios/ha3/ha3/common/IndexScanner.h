#ifndef ISEARCH_INDEXSCANNER_H
#define ISEARCH_INDEXSCANNER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <ha3/proto/ProtoClassComparator.h>
#include <ha3/common/MultiLevelRangeSet.h>

BEGIN_HA3_NAMESPACE(common);

class IndexScanner
{
public:
    static const std::string INDEX_INVALID_FLAG;
public:
    IndexScanner();
    ~IndexScanner();
private:
    IndexScanner(const IndexScanner &);
    IndexScanner& operator=(const IndexScanner &);
public:
    typedef std::map<proto::Range, /* range */
                     int32_t /* incrementalVersion */ > GenerationInfo;
    typedef std::map<int32_t, /* generation id */
                     GenerationInfo> ClusterIndexInfo;
    typedef std::map<std::string, /* clustername */
                     ClusterIndexInfo> IndexInfo;

public:
    static bool scanIndex(const std::string &indexPath,
                          IndexInfo &indexInfo, 
                          bool isOffline = false);
    static bool scanCluster(const std::string &clusterIndexPath,
                            ClusterIndexInfo &clusterIndexInfo,
                            bool isOffline = false,
                            uint32_t offlineLevel = 1);
    static bool scanClusterNames(const std::string &indexPath, 
                                 std::vector<std::string> &clusterNames);
    static bool scanGeneration(const std::string &generationIndexPath,
                               GenerationInfo &generationInfo,
                               MultiLevelRangeSet& rangeSets,
                               bool isOffline = false);
    static bool getMaxIncrementalVersion(const std::string &partitionPath,
            uint32_t &version, bool &hasError);

    static bool isGenerationValid(const std::string& generationPath);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(IndexScanner);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_INDEXSCANNER_H

#ifndef ISEARCH_BS_INDEXPATHCONSTRUCTOR_H
#define ISEARCH_BS_INDEXPATHCONSTRUCTOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
namespace build_service {
namespace util {

class IndexPathConstructor
{
private:
    IndexPathConstructor();
    ~IndexPathConstructor();
    IndexPathConstructor(const IndexPathConstructor &);
    IndexPathConstructor& operator=(const IndexPathConstructor &);
public:
    static std::string getGenerationId(
            const std::string &indexRoot,
            const std::string &clusterName,
            const std::string &buildMode,
            const std::string &rangeFrom,
            const std::string &rangeTo);

    static bool parsePartitionRange(const std::string& partitionPath,
                                    uint32_t& rangeFrom, uint32_t& rangeTo);

    static std::string constructIndexPath(
            const std::string &indexRoot,
            const std::string &clusterName,
            const std::string &generationId,
            const std::string &rangeFrom,
            const std::string &rangeTo);
    static std::string constructIndexPath(
            const std::string &indexRoot,
            const proto::PartitionId &partitionId);

    static std::string constructDocReclaimDirPath(
            const std::string &indexRoot,
            const proto::PartitionId &partitionId);

    static std::string constructDocReclaimDirPath(
            const std::string &indexRoot,
            const std::string &clusterName,
            generationid_t generationId);

    static generationid_t getNextGenerationId(
            const std::string &indexRoot, const std::string &clusterName);
    static generationid_t getNextGenerationId(const std::string &indexRoot, 
            const std::vector<std::string>& clusterNames);
    // note: may return incomplete getGenerationList when read the remote path failed.
    static std::vector<generationid_t> getGenerationList(
            const std::string &indexRoot, const std::string &clusterName);
    static std::string getGenerationIndexPath(const std::string &indexRoot, 
            const std::string &clusterName, generationid_t generationId);
    static std::string getMergerCheckpointPath(const std::string &indexRoot,
                                               const proto::PartitionId &partitionId);
    static std::string constructGenerationPath(
        const std::string &indexRoot, const proto::PartitionId &partitionId);
    static std::string constructGenerationPath(
        const std::string &indexRoot, const std::string &clusterName,
        generationid_t generationId);
        
private:
    static bool isGenerationContainRange(
            const std::string &indexRoot,
            const std::string &clusterName,
            generationid_t generationId,
            const std::string &rangeFrom,
            const std::string &rangeTo);
public:
    static const std::string GENERATION_PREFIX;
    static const std::string PARTITION_PREFIX;
    static const std::string DOC_RECLAIM_DATA_DIR;
    
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_INDEXPATHCONSTRUCTOR_H

#ifndef __AITHETA_TEST_TEST_H
#define __AITHETA_TEST_TEST_H



#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/plugin/Module.h>
#include <indexlib/test/partition_state_machine.h>

#include <aitheta_indexer/test/unittest.h>

#define private public
#include <indexlib/plugin/plugin_manager.h>
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reduce_item.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_segment_retriever.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_indexer.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/segment.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaTestBase : public PLUGIN_TESTBASE {
 public:
    AithetaTestBase() = default;
    ~AithetaTestBase();

 public:
    void CaseSetUp();
    void CaseTearDown();

 protected:
    bool Build(const util::KeyValueMap &params, const std::string &dataPath, std::string &outputDir, size_t &successCnt,
               const std::vector<docid_t> &docIds = {}, bool isFullBuildPhase = true);

    bool Build(const util::KeyValueMap &params, const std::vector<std::string> &data, std::string &outputDir,
               size_t &successCnt, const std::vector<docid_t> &docIds = {}, bool isFullBuildPhase = true);

    bool Reduce(const util::KeyValueMap &params, const std::vector<std::string> &segmentPaths, bool isEntireSet,
                const std::vector<docid_t> &baseDocIds, const std::vector<docid_t> &docIdMap, std::string &reduceDir,
                uint32_t parallelCount = 1u);

    // build index from multi segment
    bool CreateSearcher(const util::KeyValueMap &params, const std::vector<std::string> &dataPaths,
                        std::string &outputPath, IE_NAMESPACE(index)::IndexRetrieverPtr &searcher,
                        uint32_t parallelCount = 1u);

    bool CreateSearcher(const util::KeyValueMap &params, const std::vector<std::vector<std::string>> &data,
                        std::string &outputPath, IE_NAMESPACE(index)::IndexRetrieverPtr &searcher,
                        uint32_t parallelCount = 1u);

    bool CreateSearcher(const util::KeyValueMap &params, const std::vector<std::string> &segmentPaths,
                        const std::vector<docid_t> &baseDocIds, IE_NAMESPACE(index)::IndexRetrieverPtr &searcher);

    bool Search(IE_NAMESPACE(index)::IndexRetrieverPtr &searcher, const std::string &query,
                std::vector<std::pair<docid_t, float>> &result);

    DocIdMap CreateDocIdMap(docid_t base, const std::vector<docid_t> &globalDocIds);

    bool LoadSegment(const util::KeyValueMap &params, const std::string &path, const DocIdMap &docIdMap,
                     SegmentPtr &segment);

    size_t InnerGetIndexSegmentLoad4ReduceMemoryUse(const std::vector<std::string> &pathVector, size_t parallelCount);
    size_t InnerGetIndexSegmentLoad4RetrieveMemoryUse(const std::string &dirPath);

 protected:
    std::string mRootDir;
    std::string mPluginRoot;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr mSchema;
    IE_NAMESPACE(config)::IndexPartitionOptions mOptions;
    IE_NAMESPACE(index)::IndexModuleFactoryPtr mFactory;
    IE_NAMESPACE(plugin)::Module *mModule;
    IE_NAMESPACE(plugin)::PluginManagerPtr mPluginManager;
    IE_NAMESPACE(index_base)::MergeTaskResourceManagerPtr mResMgr;

 protected:
    IndexerResourcePtr CreateIndexerResource(bool isFullBuildPhase);
    std::vector<IE_NAMESPACE(index)::ReclaimMapPtr *> mReclaimMaps;

 private:
    bool ParallelReduce(const util::KeyValueMap &params, const std::vector<std::string> &segmentPaths, bool isEntireSet,
                        const std::vector<docid_t> &baseDocIds, const std::vector<docid_t> &docIdMap,
                        const std::string &outputDir, uint32_t parallelCount);

    bool SequentialReduce(const util::KeyValueMap &params,
                          const std::vector<IE_NAMESPACE(index)::IndexReduceItemPtr> &reduceItems, bool isEntireSet,
                          const std::string &outputDir);

    bool Load(const util::KeyValueMap &params, const std::string &dir, docid_t base,
              const std::vector<docid_t> &newDocIds, IE_NAMESPACE(index)::IndexReduceItemPtr &segment);

    std::string CreateOutputPath(const util::KeyValueMap &params, const std::vector<std::string> &inputPaths,
                                 const std::string &suffix = "");


    int32_t randomNum() const {
        srand((int)time(0));
        return rand();
    }

 protected:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __AITHETA_TEST_TEST_H

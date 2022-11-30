#ifndef __INDEXLIB_AITHETA_INDEX_SEGMENT_H
#define __INDEXLIB_AITHETA_INDEX_SEGMENT_H

#include <unordered_map>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/segment.h"
#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class IndexSegment : public Segment {
 public:
    IndexSegment(const SegmentMeta &meta = SegmentMeta(SegmentType::kIndex, 0U, 0U));
    ~IndexSegment() {}

 public:
    bool PrepareReduceTaskInput(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, ReduceTaskInput &input) override;
    bool Load(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) override;
    bool UpdateDocId(const DocIdMap &docIdMap) override;
    bool EstimateLoad4ReduceMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, size_t &size) const override;
    bool EstimateLoad4RetrieveMemoryUse(const IE_NAMESPACE(file_system)::DirectoryPtr &dir,
                                        size_t &size) const override;
    bool Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) override;

 public:
    bool MergeRawSegments(docid_t baseDocId, const std::vector<RawSegmentPtr> &rawSegmentVec,
                          const std::vector<docid_t> &segmentBaseDocIds);

    void addCatIndexInfo(const IndexInfo &info) { mIndexInfos.push_back(info); }
    const std::vector<IndexInfo> &getCatIndexInfos() const { return mIndexInfos; }
    void addJsonizableIndexInfo(const JsonizableIndexInfo &info) { mJIndexInfos.push_back(info); }
    const std::vector<JsonizableIndexInfo> &getJsonizableIndexInfos() const { return mJIndexInfos; }
    void AddPkey(size_t index, int64_t pkey) {
        if (unlikely(mPkeyArray->size() < index + 1)) {
            (*mPkeyArray).resize(index + 1, 0);
        }
        (*mPkeyArray)[index] = pkey;
    }

    void AddDocId(size_t index, docid_t docId) {
        if (unlikely(mDocIdArray->size() < index + 1)) {
            mDocIdArray->resize(index + 1, INVALID_DOCID);
        }
        (*mDocIdArray)[index] = docId;
    }

    void setValidDocNum(size_t n) { mMeta.setValidDocNum(n); }
    void setTotalDocNum(size_t n) { mMeta.setTotalDocNum(n); }
    void setIndexBuiltSize(size_t n) { mIndexBuiltSize = n; }

    const std::vector<docid_t> &getDocIdArray() const { return *mDocIdArray; }
    const std::vector<int64_t> &getPkeyArray() const { return *mPkeyArray; }

    void SetDocIdArray(const std::shared_ptr<std::vector<docid_t>> &docIdArray) { mDocIdArray = docIdArray; }
    void SetPkeyArray(const std::shared_ptr<std::vector<int64_t>> &pkeyArray) { mPkeyArray = pkeyArray; }
    void SetPkey2DocIdIdx(const std::shared_ptr<std::unordered_map<int64_t, docid_t>> &pkey2DocIdIdx) {
        mPkey2DocIdIdx = pkey2DocIdIdx;
    }

    bool GetIndexBaseAddress(int8_t *&address);

 public:
    void DisableConstructPkeyMap() { mEnableConstructPkeyMap = false; }
    void DisableDumpDocIdRemap() { mEnableDumpDocIdRemap = false; }
    void DisableLoadDocIdRemap() { mEnabelLoadDocIdRemap = false; }

 public:
    static bool LoadIndexInfo(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                              IE_NAMESPACE(file_system)::FSOpenType type, std::vector<IndexInfo> &indexInfos,
                              std::vector<JsonizableIndexInfo> &jIndexInfos);
    static bool DumpIndexInfo(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                              const std::vector<IndexInfo> &indexInfos,
                              const std::vector<JsonizableIndexInfo> &jIndexInfos, size_t &fileSize);
    static bool LoadDocIdRemap(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                               IE_NAMESPACE(file_system)::FSOpenType type, std::vector<docid_t> &docIdArray,
                               std::vector<int64_t> &pkeyArray);
    static bool DumpDocIdRemap(const IE_NAMESPACE(file_system)::DirectoryPtr &directory,
                               const std::vector<docid_t> &docIdArray, const std::vector<int64_t> &pkeyArray,
                               size_t &fileSize);
    static bool DumpSegmentMeta(const IE_NAMESPACE(file_system)::DirectoryPtr &directory, size_t fileSize,
                                const std::vector<IndexInfo> &indexInfos, SegmentMeta &meta);
    static bool CopyIndex(const IE_NAMESPACE(file_system)::DirectoryPtr &loadDir,
                          const IE_NAMESPACE(file_system)::DirectoryPtr &dumpDir, size_t &fileSize);
    static void BuildPkMap4RawSegments(const std::vector<RawSegmentPtr> &rawSegments,
                                       const std::vector<docid_t> &baseSegmentIds,
                                       std::unordered_map<int64_t, std::pair<docid_t, docid_t>> &pkeyMap);

 private:
    void BuildPkMap4IndexSegment();
    bool LoadDocIdRemapInOldVersion();

 private:
    IE_NAMESPACE(file_system)::DirectoryPtr mLoadDir;
    std::shared_ptr<std::vector<docid_t>> mDocIdArray;
    std::shared_ptr<std::vector<int64_t>> mPkeyArray;
    std::shared_ptr<std::unordered_map<int64_t, docid_t>> mPkey2DocIdIdx;
    std::vector<IndexInfo> mIndexInfos;
    std::vector<JsonizableIndexInfo> mJIndexInfos;
    file_system::FileReaderPtr mIndexProvider;
    bool mEnableConstructPkeyMap;
    bool mEnableDumpDocIdRemap;
    bool mEnabelLoadDocIdRemap;
    size_t mIndexBuiltSize;
    IE_LOG_DECLARE();

    friend class AithetaIndexSegmentRetriever;
    friend class AithetaIndexerTest;
    friend class AithetaIncTest;
};

DEFINE_SHARED_PTR(IndexSegment);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEX_SEGMENT_H

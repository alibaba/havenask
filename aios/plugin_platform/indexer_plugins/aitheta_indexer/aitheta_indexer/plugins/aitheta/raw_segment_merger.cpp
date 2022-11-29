#include "aitheta_indexer/plugins/aitheta/raw_segment_merger.h"

#include <assert.h>
#include <unordered_set>
#include <vector>

#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;

IE_NAMESPACE_USE(file_system);

IE_LOG_SETUP(aitheta_plugin, RawSegmentMerger);

bool RawSegmentMerger::Merge(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) {
    RawSegmentPtr rawSegment(new RawSegment());
    EmbeddingPtr embedding;
    unordered_set<int64_t> existPkeys;
    for (auto it = mSegmentVec.rbegin(); it != mSegmentVec.rend(); ++it) {
        const auto &catId2records = (*it)->GetCatId2Records();
        for (const auto &kv : catId2records) {
            int64_t catId = kv.first;
            const auto &pkeys = get<0>(kv.second);
            const auto &docIds = get<1>(kv.second);
            assert(pkeys.size() == docIds.size());
            size_t docNum = docIds.size();
            for (size_t i = 0; i < docNum; ++i) {
                if (existPkeys.find(pkeys[i]) == existPkeys.end()) {
                    rawSegment->Add(catId, pkeys[i], docIds[i], embedding);
                    existPkeys.insert(pkeys[i]);
                }
            }
        }
    }
    return rawSegment->Dump(directory);
}

IE_NAMESPACE_END(aitheta_plugin);

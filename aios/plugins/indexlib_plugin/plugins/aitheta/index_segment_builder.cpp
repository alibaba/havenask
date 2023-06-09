/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib_plugin/plugins/aitheta/index_segment_builder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "autil/StringUtil.h"
#include <aitheta/index_framework.h>
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"
#include "indexlib_plugin/plugins/aitheta/util/output_storage.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_params_selector_factory.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/knn_builder_factory.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace aitheta;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, IndexSegmentBuilder);

bool IndexSegmentBuilder::Init() {
    METRIC_SETUP(mTrainIndexLatency, "TrainIndexLatency", kmonitor::GAUGE);
    METRIC_SETUP(mBuildIndexLatency, "BuildIndexLatency", kmonitor::GAUGE);
    METRIC_SETUP(mDumpIndexLatency, "DumpIndexLatency", kmonitor::GAUGE);
    return true;
}

bool IndexSegmentBuilder::Build(catid_t catid, docid_t docid, const embedding_t embedding) {
    return mRawSegment->Add(catid, docid, embedding);
}

bool IndexSegmentBuilder::Dump(const indexlib::file_system::DirectoryPtr &dir) {
    return BatchBuild({mRawSegment}, dir, mRawSegment->GetCatids());
}

bool IndexSegmentBuilder::BatchBuild(const std::vector<SegmentPtr> &srcSgts,
                                     const indexlib::file_system::DirectoryPtr &dir,
                                     const std::set<catid_t> &buildingCatids) {
    SegmentMeta sgtMeta(SegmentType::kIndex, false, mSchemaParam.dimension);
    IndexSegment dstIndexSgt(sgtMeta);
    dstIndexSgt.SetOfflineKnnConfig(mKnnCfg);
    dstIndexSgt.CreateIndexWriter(dir, true);

    EmbedHolderIterator iterator;
    for (size_t i = 0u; i < srcSgts.size(); i++) {
        srcSgts[i]->SetOfflineKnnConfig(mKnnCfg);
        if (i == 0u) {
            iterator = srcSgts[i]->CreateEmbedHolderIterator(mSchemaParam.keyValMap, buildingCatids);
            continue;
        }
        iterator.Merge(srcSgts[i]->CreateEmbedHolderIterator(mSchemaParam.keyValMap, buildingCatids));
    }

    while (iterator.IsValid()) {
        EmbedHolderPtr embedHolder = iterator.Value();
        if (embedHolder->count() != 0u) {
            if (!BuildIndex(embedHolder, dstIndexSgt)) {
                return false;
            }
        } else {
            IE_LOG(WARN, "no document with catid[%ld]", embedHolder->GetCatid());
        }

        iterator.Next();
    }
    if (!dstIndexSgt.Dump(dir)) {
        IE_LOG(ERROR, "index sgt dump failed, dir[%s]", dir->DebugString().c_str());
        return false;
    }

    IE_LOG(INFO, "finish building index segment");
    return true;
}

bool IndexSegmentBuilder::BuildIndex(EmbedHolderPtr &embedHolder, IndexSegment &dstIndexSgt) {
    catid_t catid = embedHolder->GetCatid();
    size_t docNum = embedHolder->count();
    IE_LOG(INFO, "start train-build-dump index, catid[%ld], docNum[%lu]", catid, docNum);

    OfflineIndexAttr indexAttr;
    indexAttr.catid = catid;
    indexAttr.docNum = docNum;

    // TODO(richard.sy): opt it
    util::KeyValueMap keyVal = mSchemaParam.keyValMap;
    keyVal[DOC_NUM] = StringUtil::toString(docNum);
    keyVal[TMP_DUMP_DIR] = BuildTmpDumpDir();

    MipsReformer reformer(0u, 0.0f, 0.0f);
    IndexBuilderPtr builder = KnnBuilderFactory::Create(mSchemaParam, keyVal, mKnnCfg, docNum, reformer);
    if (reformer.m() > 0u) {
        if (!embedHolder->DoMips(reformer, reformer)) {
            return false;
        }
    }
    indexAttr.reformer = reformer;
    {
        ScopedLatencyReporter reporter(mTrainIndexLatency);
        int32_t err = builder->trainIndex(embedHolder);
        if (err < 0) {
            IE_LOG(ERROR, "failed to train index, err[%s]", IndexError::What(err));
            return false;
        }
        IE_LOG(INFO, "finish training index");
    }
    {
        ScopedLatencyReporter reporter(mBuildIndexLatency);
        int32_t err = builder->buildIndex(embedHolder);
        embedHolder.reset();
        if (err < 0) {
            IE_LOG(ERROR, "failed to build index, err[%s]", IndexError::What(err));
            return false;
        }
        IE_LOG(INFO, "finish building index");
    }
    {
        ScopedLatencyReporter reporter(mDumpIndexLatency);
        auto writer = dstIndexSgt.GetIndexWriter();
        assert(writer);
        indexAttr.offset = writer->GetLength();
        IndexStoragePtr storage(new OutputStorage(writer));
        int32_t err = builder->dumpIndex(".", storage);
        if (err < 0) {
            IE_LOG(ERROR, "failed to dump index, err[%s]", IndexError::What(err));
            return false;
        }
        indexAttr.size = writer->GetLength() - indexAttr.offset;
        IE_LOG(INFO, "finish dumping index");
    }

    dstIndexSgt.AddOfflineIndexAttr(indexAttr);
    FslibWrapper::DeleteDirE(keyVal[TMP_DUMP_DIR], DeleteOption::NoFence(false));
    IE_LOG(INFO, "finish train-build-dump index");
    return true;
}

const std::string IndexSegmentBuilder::BuildTmpDumpDir() {
    string tmpDumpDir = "./tmp_dump_" + StringUtil::toString(pthread_self()) + "_" +
                        StringUtil::toString(TimeUtility::currentTimeInMicroSeconds());
    FslibWrapper::MkDirE(tmpDumpDir, true, true);
    return tmpDumpDir;
}

size_t IndexSegmentBuilder::EstimateTempMemoryUseInDump() {
    auto maxDocCount = mRawSegment->GetMaxDocCount();
    auto rawSize = maxDocCount * (sizeof(docid_t) + mSchemaParam.dimension * sizeof(float));
    return rawSize * REDUCE_MEM_USE_MAGIC_NUM;
}

}
}

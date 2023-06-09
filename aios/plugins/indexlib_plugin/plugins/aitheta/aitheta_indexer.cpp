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
#include "indexlib_plugin/plugins/aitheta/aitheta_indexer.h"
#include <string>
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib_plugin/plugins/aitheta/raw_segment_builder.h"
#include "indexlib_plugin/plugins/aitheta/index_segment_builder.h"
#include "indexlib_plugin/plugins/aitheta/rt_segment_builder.h"
#include "indexlib_plugin/plugins/aitheta/util/custom_logger.h"
#include "indexlib_plugin/plugins/aitheta/aitheta_rt_segment_retriever.h"
#include "indexlib_plugin/plugins/aitheta/util/string_parser.h"

using namespace std;
using namespace autil;

using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, AithetaIndexer);

bool AithetaIndexer::DoInit(const IndexerResourcePtr &resource) {
    RegisterGlobalIndexLogger();
    KnnConfig knnCfg;
    if (!knnCfg.Load(resource->pluginPath)) {
        IE_LOG(WARN, "failed to load knn config[%s]", resource->pluginPath.c_str());
    }

    mIndexerResource = resource;
    auto buildPhase = mIndexerResource->options.GetBuildConfig().buildPhase;
    const string &indexName = mIndexerResource->indexName;
    switch (buildPhase) {
        case BuildConfigBase::BuildPhase::BP_FULL: {
            if (mSchemaParam.forceBuildIndex) {
                mBuilder.reset(new IndexSegmentBuilder(mSchemaParam, knnCfg));
            } else {
                mBuilder.reset(new RawSegmentBuilder(mSchemaParam));
            }
            break;
        }
        case BuildConfigBase::BuildPhase::BP_INC: {
            mBuilder.reset(new IndexSegmentBuilder(mSchemaParam, knnCfg));
            break;
        }
        case BuildConfigBase::BuildPhase::BP_RT: {
            if (!mSchemaParam.rtOption.enable) {
                IE_LOG(INFO, "disable realtime build for embedding index");
                return true;
            }
            mBuilder.reset(new RtSegmentBuilder(mSchemaParam));
            break;
        }
        default: {
            IE_LOG(ERROR, "unknown build phase[%d] for index[%s]", buildPhase, indexName.c_str());
            return false;
        }
    }

    auto schema = mIndexerResource->schema;
    string tableName = schema != nullptr ? schema->GetSchemaName() : "DEFAULT";
    _metricReporter.reset(new MetricReporter(mIndexerResource->metricProvider, tableName, indexName));
    mBuilder->SetIndexName(indexName);
    mBuilder->SetMetricReporter(_metricReporter);
    return mBuilder->Init();
}

bool AithetaIndexer::Build(const std::vector<const Field *> &fields, docid_t docid) {
    if (docid == INVALID_DOCID|| !mBuilder) {
        return true;
    }

    if (mIndexerResource->options.GetBuildConfig().buildPhase == BuildConfigBase::BuildPhase::BP_RT &&
        !mSchemaParam.rtOption.enable) {
        IE_LOG(DEBUG, "build real time segment failed, as it is disabled");
        return true;
    }

    const auto &indexName = mIndexerResource->indexName;
    if (fields.size() < 2u || fields.size() > 3u) {
        IE_LOG(ERROR, "unexpected fields size[%lu] for index[%s]", fields.size(), indexName.c_str());
        for (auto &field : fields) {
            fieldid_t fieldId = field->GetFieldId();
            string fieldName = mFieldSchema->GetFieldConfig(fieldId)->GetFieldName();
            IE_LOG(INFO, "field debug info: fieldId[%d], fieldName[%s]", fieldId, fieldName.c_str());
        }
        return false;
    }

    bool hasCatid = (fields.size() == 3u ? true : false);
    auto &embeddingField = (hasCatid ? fields[2] : fields[1]);
    EmbeddingPtr embedding;
    if (!ParseEmbedding(embeddingField, embedding)) {
        return false;
    }
    std::vector<catid_t> catids;
    if (hasCatid) {
        if (!ParseCatid(fields[1], catids)) {
            return false;
        }
    } else {
        catids.push_back(DEFAULT_CATEGORY_ID);
    }

    for_each(catids.begin(), catids.end(), [&](catid_t id) { mBuilder->Build(id, docid, embedding); });
    return true;
}

bool AithetaIndexer::Dump(const DirectoryPtr &dir) {
    if (!mBuilder) {
        return true;
    }
    return mBuilder->Dump(dir);
}
bool AithetaIndexer::ParseCatid(const Field *field, std::vector<catid_t> &catids) {
    auto rawField = dynamic_cast<const IndexRawField *>(field);
    if (!rawField) {
        IE_LOG(ERROR, "failed to dynamic_cast Field to IndexRawField for index[%s]",
               mIndexerResource->indexName.c_str());
        return false;
    }

    string catidStr = string(rawField->GetData().data(), rawField->GetData().size());
    IE_LOG(DEBUG, "catid value: [%s]", catidStr.c_str());
    if (catidStr.empty()) {
        return false;
    }
    char catidSep = mSchemaParam.catidSeparator[0];
    if (!StringParser::Parse(catidStr, 0u, catidStr.size(), catidSep, catids)) {
        IE_LOG(ERROR, "failed to parse catid [%s] for index name[%s]", catidStr.c_str(),
               mIndexerResource->indexName.c_str());
        return false;
    }
    return true;
}

bool AithetaIndexer::ParseEmbedding(const Field *field, EmbeddingPtr &embedding) {
    auto rawField = dynamic_cast<const IndexRawField *>(field);
    if (!rawField) {
        IE_LOG(ERROR, "failed to dynamic_cast Field to IndexRawField for index[%s]",
               mIndexerResource->indexName.c_str());
        return false;
    }

    string embStr = string(rawField->GetData().data(), rawField->GetData().size());
    IE_LOG(DEBUG, "embedding value: [%s]", embStr.c_str());
    if (embStr.empty()) {
        return false;
    }
    char embSep = mSchemaParam.embedSeparator[0];
    uint32_t dim = mSchemaParam.dimension;
    if (!StringParser::Parse(embStr, 0u, embStr.size(), embSep, dim, embedding)) {
        IE_LOG(ERROR, "failed to parse embedding [%s] for index name[%s]", embStr.c_str(),
               mIndexerResource->indexName.c_str());
        return false;
    }
    return true;
}

bool AithetaIndexer::ParseKey(const document::Field *field, int64_t &key) {
    if (field->GetFieldTag() == Field::FieldTag::TOKEN_FIELD) {
        auto tokenizeField = dynamic_cast<const IndexTokenizeField *>(field);
        if (!tokenizeField) {
            IE_LOG(WARN, "failed to parse key");
            return false;
        }
        auto section = *(tokenizeField->Begin());
        if (section->GetLength() >= 1) {
            key = section->GetToken(0)->GetHashKey();
        } else {
            IE_LOG(WARN, "failed to parse key");
            return false;
        }
    } else {
        auto indexRawField = dynamic_cast<const IndexRawField *>(field);
        if (!indexRawField) {
            IE_LOG(WARN, "failed to parse key");
            return false;
        }
        auto data = indexRawField->GetData();
        string keyStr(data.data(), data.size());
        if (!StringUtil::strToInt64(keyStr.data(), key)) {
            IE_LOG(WARN, "failed to parse [%s] to int64", keyStr.c_str());
            return false;
        }
    }
    IE_LOG(DEBUG, "key value: [%ld]", key);
    return true;
}

InMemSegmentRetrieverPtr AithetaIndexer::CreateInMemSegmentRetriever() const {
    if (!mSchemaParam.rtOption.enable || !mBuilder) {
        IE_LOG(INFO, "failed to create InMemSegmentRetriever as rt build is disabled for index[%s]",
               mIndexerResource->indexName.c_str());
        return InMemSegmentRetrieverPtr();
    }
    auto rtSgtBuilder = DYNAMIC_POINTER_CAST(RtSegmentBuilder, mBuilder);
    if (!rtSgtBuilder) {
        IE_LOG(ERROR, "failed to dynamically cast to RtSegmentBuilder for index[%s]",
               mIndexerResource->indexName.c_str());
        return InMemSegmentRetrieverPtr();
    }
    auto segment = rtSgtBuilder->GetSegment();
    return InMemSegmentRetrieverPtr(new AithetaRtSegmentRetriever(mSchemaParam.keyValMap, segment));
}

}
}

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
#include "indexlib_plugin/plugins/aitheta/util/embed_holder_iterator/embed_holder_iterator.h"

using namespace std;
namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, EmbedHolderIterator);

EmbedHolderIterator::~EmbedHolderIterator() {
    while (!mHeap.empty()) {
        auto node = mHeap.top();
        if (!node) {
            delete node;
            node = nullptr;
        }
        mHeap.pop();
    }
}

EmbedHolderIterator::EmbedHolderIterator(size_t dim, Node* node, const std::set<int64_t>& catids)
    : mDimension(dim), mBuildingCatids(catids) {
    if (nullptr != node) {
        if (node->IsValid()) {
            mHeap.push(node);
        } else {
            delete node;
            node = nullptr;
        }
    }
}

bool EmbedHolderIterator::Merge(const EmbedHolderIterator& other) {
    if (other.mValue || mValue) {
        IE_LOG(ERROR, "failed to merge as output of mCurs may be disordered");
        return false;
    }
    auto heap = other.mHeap;
    while (!heap.empty()) {
        if (heap.top() && heap.top()->IsValid()) {
            mHeap.push(heap.top());
        }
        heap.pop();
    }
    auto& st = other.mBuildingCatids;
    mBuildingCatids.insert(st.begin(), st.end());
    return true;
}

EmbedHolderPtr EmbedHolderIterator::Value() {
    if (!mValue) {
        if (!BuildValue()) {
            mValue.reset();
        }
    }
    return mValue;
}

bool EmbedHolderIterator::IsValid() const {
    if (mValue) {
        return true;
    }

    if (!mBuildingCatids.empty() && mHeap.empty()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "mHeap is empty but mBuildingCatids is not");
    }

    if (mBuildingCatids.empty()) {
        return false;
    }

    return true;
}

void EmbedHolderIterator::Next() {
    if (mValue) {
        mValue.reset();
    }
}

bool EmbedHolderIterator::BuildValue() {
    if (mHeap.empty()) {
        IE_LOG(ERROR, "no node, failed to build holder");
        return false;
    }
    if (mBuildingCatids.empty()) {
        IE_LOG(ERROR, "no valid catid, failed to build");
        return false;
    }

    catid_t buildingCatid = *mBuildingCatids.begin();
    while (!mHeap.empty()) {
        // erase invalid node
        while (!mHeap.empty()) {
            Node* node = mHeap.top();
            if (node && node->IsValid()) {
                break;
            }
            mHeap.pop();
        }
        if (mHeap.empty()) {
            IE_LOG(ERROR, "no node, failed to build holder");
            return false;
        }
        Node* node = mHeap.top();
        auto holder = node->Value();
        int64_t id = holder->GetCatid();
        if (id == buildingCatid) {
            mBuildingCatids.erase(mBuildingCatids.begin());
            break;
        } else if (id < buildingCatid) {
            node->Next();
            mHeap.pop();
            if (node->IsValid()) {
                mHeap.push(node);
            }
        } else {
            IE_LOG(INFO, "autual catid[%ld], expect[%ld]", id, buildingCatid);
            return false;
        }
    }

    if (mHeap.empty()) {
        IE_LOG(ERROR, "no valid node, should check valid catid set");
        return false;
    }

    vector<EmbedHolderPtr> holderVec;
    while (!mHeap.empty()) {
        Node* node = mHeap.top();
        auto holder = node->Value();
        int64_t id = holder->GetCatid();
        if (id == buildingCatid) {
            mHeap.pop();
            if (holder->dimension() != mDimension) {
                IE_LOG(ERROR, "actual dim[%lu], expect[%lu]", holder->dimension(), mDimension);
                return false;
            }
            holderVec.push_back(holder);
            node->Next();
            if (node->IsValid()) {
                mHeap.push(node);
            } else {
                delete node;
                node = nullptr;
            }
        } else if (id > buildingCatid) {
            break;
        } else {
            IE_LOG(ERROR, "actual catid[%ld], expect[%ld]", id, buildingCatid);
            return false;
        }
    }
    if (holderVec.empty()) {
        IE_LOG(ERROR, "no holder, failed to build for catid[%ld]", buildingCatid);
        mValue.reset();
        return false;
    }

    mValue.reset(new EmbedHolder(mDimension, buildingCatid));
    for (const auto& holder : holderVec) {
        auto& vec = holder->GetEmbedVec();
        mValue->BatchAdd(vec);
    }
    return true;
}

}
}

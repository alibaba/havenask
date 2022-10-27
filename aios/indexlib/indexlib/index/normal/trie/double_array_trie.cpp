#include <algorithm>
#include "indexlib/index/normal/trie/double_array_trie.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DoubleArrayTrie);

DoubleArrayTrie::DoubleArrayTrie(PoolBase* pool)
    : mStateArray(StateArray::allocator_type(pool))
    , mSize(0)
    , mError(false)
{
}

DoubleArrayTrie::~DoubleArrayTrie() 
{
}

void DoubleArrayTrie::FindChildNodes(const KVPairVector& sortedKVPairVector,
                                     const Node& parentNode, NodeVector& childNodes)
{
    if (mError)
    {
        return;
    }
    uint32_t prevCode = 0;
    for (size_t i = parentNode.left; i < parentNode.right; ++i)
    {
        const ConstString& key = sortedKVPairVector[i].first;
        if (key.size() < parentNode.depth)
        {
            continue;
        }
        uint32_t curCode = 0;
        if (key.size() != parentNode.depth)
        {
            uint8_t* data = (uint8_t*)key.data();
            curCode = (uint32_t)data[parentNode.depth] + 1;
        }

        if (prevCode > curCode)
        {
            IE_LOG(ERROR, "need input sorted KVPairVector!");
            mError = true;
            return;
        }

        if (curCode != prevCode || childNodes.empty())
        {
            Node node;
            node.depth = parentNode.depth + 1;
            node.code = curCode;
            node.left = i;
            if (!childNodes.empty())
            {
                childNodes[childNodes.size() - 1].right = i;
            }
            childNodes.push_back(node);
        }
        prevCode = curCode;
    }

    if (!childNodes.empty())
    {
        childNodes[childNodes.size() - 1].right = parentNode.right;
    }
}

void DoubleArrayTrie::Resize(size_t newSize)
{
    mStateArray.resize(newSize);
    mUsed.resize(newSize, false);
}

bool DoubleArrayTrie::CheckOtherChildNodes(const NodeVector& childNodes, size_t begin)
{
    for (size_t i = 1; i < childNodes.size(); ++i)
    {
        if (mStateArray[begin + childNodes[i].code].check != 0)
        {
            return false;
        }
    }
    return true;
}

size_t DoubleArrayTrie::FindBegin(const KVPairVector& sortedKVPairVector,
                                  const NodeVector& childNodes)
{
    size_t begin = 0;
    size_t nonZeroNum = 0;
    bool first = false;
    const Node& firstNode = childNodes[0];
    size_t pos = max((size_t)firstNode.code + 1, mNextCheckPos) - 1;
    while (true)
    {
        ++pos;
        if (mStateArray.size() <= pos)
        {
            Resize(pos + 1);
        }
        if (mStateArray[pos].check) // begin +C for first node
        {
            ++nonZeroNum;
            continue;
        }
        else if(!first)
        {
            mNextCheckPos = pos; // first valid pos for first node
            first = true;
        }

        begin = pos - firstNode.code;
        if (mStateArray.size() <= (begin + childNodes[childNodes.size() - 1].code))
        {
            float rate = max(1.05, 1.0 * sortedKVPairVector.size() / mProgress);
            Resize((size_t)(mStateArray.size() * rate));
        }
        if (mUsed[begin])
        {
            continue; // begin +0
        }
        if (CheckOtherChildNodes(childNodes, begin)) // begin +C
        {
            break;
        }
    }

    if (1.0 * nonZeroNum / (pos - mNextCheckPos + 1) >= 0.95)
    {
        mNextCheckPos = pos;
    }
    mUsed[begin] = true;
    return begin;
}

size_t DoubleArrayTrie::Insert(const KVPairVector& sortedKVPairVector,
                               const NodeVector& childNodes)
{
    if (mError)
    {
        return 0;
    }

    size_t begin = FindBegin(sortedKVPairVector, childNodes);
    mSize = max(mSize, begin + (size_t)childNodes[childNodes.size() - 1].code + 1);

    for (size_t i = 0; i < childNodes.size(); ++i)
    {
        mStateArray[begin + childNodes[i].code].check = begin;
    }

    for (size_t i = 0; i < childNodes.size(); ++i)
    {
        const Node& child = childNodes[i];
        NodeVector newChildNodes;
        FindChildNodes(sortedKVPairVector, child, newChildNodes);
        if (newChildNodes.empty())
        {
            int32_t rawValue = sortedKVPairVector[child.left].second;
            int32_t value = -rawValue - 1;
            mStateArray[begin + child.code].base = value;
            if (value >= 0)
            {
                IE_LOG(ERROR, "invalid value[%d]", rawValue);
                mError = true;
                return 0;
            }
            ++mProgress;
        }
        else
        {
            size_t h = Insert(sortedKVPairVector, newChildNodes);
            mStateArray[begin + child.code].base = h;

            // if use below code instead of above, will assert fail...why?
            //mStateArray[begin + child.code].base = Insert(sortedKVPairVector, newChildNodes);
            assert(mStateArray[begin + child.code].base != 0);
        }
    }
    return begin;
}

bool DoubleArrayTrie::Build(const KVPairVector& sortedKVPairVector)
{
    if (sortedKVPairVector.empty())
    {
        return true;
    }

    Resize(INIT_SIZE);

    mStateArray[0].base = 1;
    mNextCheckPos = 0;

    Node rootNode;
    rootNode.left = 0;
    rootNode.right = sortedKVPairVector.size();
    rootNode.depth = 0;

    NodeVector childNodes;
    FindChildNodes(sortedKVPairVector, rootNode, childNodes);
    Insert(sortedKVPairVector, childNodes);

    // for search end
    mSize += (1 << 8 * sizeof(char)) + 1;
    if (mStateArray.size() < mSize)
    {
        Resize(mSize);
    }

    return !mError;
}

autil::ConstString DoubleArrayTrie::Data() const
{
    return autil::ConstString((const char*)mStateArray.data(),
                              mSize * sizeof(State));
}

float DoubleArrayTrie::CompressionRatio() const
{
    size_t nonZeroNum = 0;
    for (size_t i = 0; i < mSize; ++i)
    {
        if (mStateArray[i].check)
        {
            ++nonZeroNum;
        }
    }
    return nonZeroNum * 1.0 / mSize;
}

size_t DoubleArrayTrie::EstimateMemoryUseFactor()
{
    // TODO
    return 5 * sizeof(State);
}

size_t DoubleArrayTrie::EstimateMemoryUse(size_t keyCount)
{
    // TODO
    int64_t stateCount = max((size_t)INIT_SIZE, keyCount * 5);
    return stateCount * sizeof(State);
}

IE_NAMESPACE_END(index);


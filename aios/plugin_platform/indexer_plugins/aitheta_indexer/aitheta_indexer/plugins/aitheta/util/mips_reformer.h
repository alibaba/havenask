/**
 *   Copyright (C) The Software Authors. All rights reserved.

 *   \file     mips_reformer.h
 *   \author   Hechong.xyf
 *   \date     Jul 2018
 *   \version  1.0.0
 *   \brief    Interface of Aitheta MIPS Reformer
 */

#ifndef __INDEXLIB_AITHETA_UTIL_MIPS_REFORMER_H_
#define __INDEXLIB_AITHETA_UTIL_MIPS_REFORMER_H_

#include <vector>
#include <cmath>

namespace aitheta {

/*! MIPS Reformer
 */
class MipsReformer
{
public:
    //! Constructor
    MipsReformer(void) : _m(4), _u(0.38196601f), _norm(0.0) {}

    //! Constructor
    MipsReformer(float l2) : _m(4), _u(0.38196601f), _norm(l2) {}

    //! Constructor
    MipsReformer(uint32_t mval, float uval, float l2)
        : _m(mval), _u(uval), _norm(l2)
    {
    }

    //! Retrieve the L2-norm
    float norm(void) const
    {
        return _norm;
    }

    //! Retrieve parameter `U`
    float u(void) const
    {
        return _u;
    }

    //! Retrieve parameter `m`
    uint32_t m(void) const
    {
        return _m;
    }

    //! Update parameters
    void update(uint32_t mval, float uval, float l2)
    {
        _m = mval;
        _u = uval;
        if (l2 > _norm) {
            _norm = l2;
            if (_norm < 1.0 && _norm > _u) {
                _u = _norm;
            }
        }
    }

    //! Update L2-norm parameter
    void update(float l2)
    {
        if (l2 > _norm) {
            _norm = l2;
            if (_norm < 1.0 && _norm > _u) {
                _u = _norm;
            }
        }
    }

    //! Update L2-norm parameter
    void update(const float *vec, size_t dim)
    {
        float l2 = 0.0f;
        for (size_t i = 0; i < dim; ++i) {
            float val = vec[i];
            l2 += val * val;
        }
        l2 = std::sqrt(l2);
        if (l2 > _norm) {
            _norm = l2;
            if (_norm < 1.0 && _norm > _u) {
                _u = _norm;
            }
        }
    }

    //! Transform the feature vectors
    void transFeature(const float *vec, size_t dim, std::vector<float> *out) const
    {
        out->resize(dim + _m);
        float *result = out->data();
        float offset = 0.0f;

        for (size_t i = 0; i < dim; ++i) {
            float val = vec[i];
            result[i] = val * _u / _norm;
            offset += val * val;
        }
        offset = offset * (_u * _u) / (_norm * _norm);
        for (size_t i = 0; i < _m; ++i) {
            result[i + dim] = 0.5f - offset;
            offset *= offset;
        }
    }

    //! Transform the query vectors
    void transQuery(const float *vec, size_t dim, std::vector<float> *out) const
    {
        out->assign(vec, vec + dim);
        out->resize(dim + _m);
    }

    //! Normalize squared euclidean score to inner product
    float normalize(float sqnorm, float score) const
    {
        return ((sqnorm + _m / 4 - score) / 2.0f) * (_norm / _u);
    }

private:
    uint32_t _m;
    float _u;
    float _norm;
};

} // namespace aitheta

#endif // __INDEXLIB_AITHETA_UTIL_MIPS_REFORMER_H_
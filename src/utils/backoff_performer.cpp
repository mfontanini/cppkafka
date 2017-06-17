/*
 * Copyright (c) 2017, Matias Fontanini
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <algorithm>
#include "utils/backoff_performer.h"

using std::min;

namespace cppkafka {

BackoffPerformer::BackoffPerformer()
: initial_backoff_(DEFAULT_INITIAL_BACKOFF),
  backoff_step_(DEFAULT_BACKOFF_STEP), maximum_backoff_(DEFAULT_MAXIMUM_BACKOFF),
  policy_(BackoffPolicy::LINEAR) {

}

void BackoffPerformer::set_backoff_policy(BackoffPolicy policy) {
    policy_ = policy;
}

void BackoffPerformer::set_initial_backoff(TimeUnit value) {
    initial_backoff_ = value;
}

void BackoffPerformer::set_backoff_step(TimeUnit value) {
    backoff_step_ = value;
}

void BackoffPerformer::set_maximum_backoff(TimeUnit value) {
    maximum_backoff_ = value;
}

BackoffPerformer::TimeUnit BackoffPerformer::increase_backoff(TimeUnit backoff) {
    if (policy_ == BackoffPolicy::LINEAR) {
        backoff = backoff + backoff_step_;
    }
    else {
        backoff = backoff * 2;
    }
    return min(backoff, maximum_backoff_);
}

} // cppkafka

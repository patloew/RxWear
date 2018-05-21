package com.patloew.rxwear;

import android.support.annotation.NonNull;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;

import io.reactivex.ObservableEmitter;

/* Copyright 2016 Patrick Löwenstein
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * FILE MODIFIED by Marek Wałach, 2018
 *
 *
 */
class StatusErrorResultCallBack<T> implements OnCompleteListener<T> {

    private final ObservableEmitter emitter;

    StatusErrorResultCallBack(@NonNull ObservableEmitter emitter) {
        this.emitter = emitter;
    }

    @Override
    public void onComplete(@NonNull Task<T> task) {
        if (!task.isSuccessful()) {
            emitter.onError(new StatusException(task.getException()));
        }
    }
}

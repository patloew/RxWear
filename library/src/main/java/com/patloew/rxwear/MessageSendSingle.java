package com.patloew.rxwear;

import android.content.Context;
import android.support.annotation.NonNull;

import com.google.android.gms.wearable.Wearable;

import io.reactivex.SingleEmitter;

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
class MessageSendSingle extends BaseSingle<Integer> {

    final String nodeId;
    final String path;
    final byte[] data;

    MessageSendSingle(@NonNull Context context, String nodeId, String path, byte[] data) {
        super(context);
        this.nodeId = nodeId;
        this.path = path;
        this.data = data;
    }

    @Override
    void onSubscribe(SingleEmitter<Integer> integerSingleEmitter) {
        setupWearTask(
                Wearable.getMessageClient(context).sendMessage(nodeId, path, data),
                SingleResultCallBack.get(integerSingleEmitter)
        );
    }
}

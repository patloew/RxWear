package com.patloew.rxwear;

import android.content.Context;
import android.net.Uri;
import android.support.annotation.NonNull;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.wearable.MessageClient;
import com.google.android.gms.wearable.MessageEvent;
import com.google.android.gms.wearable.Wearable;

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
class MessageListenerObservable extends BaseObservable<MessageEvent> {

    final Uri uri;
    final Integer filterType;

    private MessageClient.OnMessageReceivedListener listener;

    MessageListenerObservable(@NonNull Context context, Uri uri, Integer filterType) {
        super(context);
        this.uri = uri;
        this.filterType = filterType;
    }

    @Override
    void onSubscribe(ObservableEmitter<MessageEvent> messageEventObservableEmitter) {
        listener = messageEventObservableEmitter::onNext;

        OnCompleteListener<Void> resultCallback = new StatusErrorResultCallBack<>(messageEventObservableEmitter);

        if (uri != null) {
            setupWearTask(Wearable.getMessageClient(context).addListener(listener, uri, filterType), resultCallback);
        } else {
            setupWearTask(Wearable.getMessageClient(context).addListener(listener), resultCallback);
        }
    }

    @Override
    void unSubscribe() {
        Wearable.getMessageClient(context).removeListener(listener);
    }
}

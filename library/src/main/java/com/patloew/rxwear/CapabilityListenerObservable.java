package com.patloew.rxwear;

import android.content.Context;
import android.net.Uri;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.wearable.CapabilityClient;
import com.google.android.gms.wearable.CapabilityInfo;
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
class CapabilityListenerObservable extends BaseObservable<CapabilityInfo> {

    final String capability;
    final Uri uri;
    final Integer filterType;

    private CapabilityClient.OnCapabilityChangedListener listener;

    CapabilityListenerObservable(Context context, String capability, Uri uri, Integer filterType) {
        super(context);
        this.capability = capability;
        this.uri = uri;
        this.filterType = filterType;
    }

    @Override
    void onSubscribe(ObservableEmitter<CapabilityInfo> capabilityInfoObservableEmitter) {
        listener = capabilityInfoObservableEmitter::onNext;

        OnCompleteListener<Void> resultCallback = new StatusErrorResultCallBack<>(capabilityInfoObservableEmitter);

        if (capability != null) {
            setupWearTask(Wearable.getCapabilityClient(context).addListener(listener, capability), resultCallback);
        } else {
            setupWearTask(Wearable.getCapabilityClient(context).addListener(listener, uri, filterType), resultCallback);
        }
    }

    @Override
    void unSubscribe() {
        if (capability != null) {
            Wearable.getCapabilityClient(context).removeListener(listener, capability);
        } else {
            Wearable.getCapabilityClient(context).removeListener(listener);
        }
    }
}

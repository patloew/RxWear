package com.patloew.rxwear;

import android.content.Context;
import android.net.Uri;
import android.support.annotation.NonNull;

import com.google.android.gms.wearable.CapabilityInfo;

import io.reactivex.Observable;
import io.reactivex.Single;

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
public class Capability {

    private final Context context;

    Capability(Context context) {
        this.context = context;
    }

    // listen

    public Observable<CapabilityInfo> listen(@NonNull String capability) {
        return listenInternal(capability, null, null);
    }

    public Observable<CapabilityInfo> listen(@NonNull Uri uri, int filterType) {
        return listenInternal(null, uri, filterType);
    }

    private Observable<CapabilityInfo> listenInternal(String capability, Uri uri, Integer filterType) {
        return Observable.create(new CapabilityListenerObservable(context, capability, uri, filterType));
    }

    // getAll

    public Observable<CapabilityInfo> getAll(int nodeFilter) {
        return getAllInternal(nodeFilter);
    }

    private Observable<CapabilityInfo> getAllInternal(int nodeFilter) {
        return Single.create(new CapabilityGetAllSingle(context, nodeFilter))
                .flatMapObservable(capabilityInfoMap -> Observable.fromIterable(capabilityInfoMap.values()));
    }

    // get

    public Single<CapabilityInfo> get(@NonNull String capability, int nodeFilter) {
        return getInternal(capability, nodeFilter);
    }

    private Single<CapabilityInfo> getInternal(String capability, int nodeFilter) {
        return Single.create(new CapabilityGetSingle(context, capability, nodeFilter));
    }

    // addLocal

    public Single<Void> addLocal(@NonNull String capability) {
        return addLocalInternal(capability);
    }

    private Single<Void> addLocalInternal(String capability) {
        return Single.create(new CapabilityAddLocalSingle(context, capability));
    }

    // removeLocal

    public Single<Void> removeLocal(@NonNull String capability) {
        return removeLocalInternal(capability);
    }

    private Single<Void> removeLocalInternal(String capability) {
        return Single.create(new CapabilityRemoveLocalSingle(context, capability));
    }
}

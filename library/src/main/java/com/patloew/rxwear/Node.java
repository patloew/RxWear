package com.patloew.rxwear;

import android.content.Context;

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
public class Node {

    private final Context context;

    Node(Context context) {
        this.context = context;
    }

    // getConnectedNodes

    public Observable<com.google.android.gms.wearable.Node> getConnectedNodes() {
        return getConnectedNodesInternal();
    }

    Observable<com.google.android.gms.wearable.Node> getConnectedNodesInternal() {
        return Single.create(new NodeGetConnectedSingle(context))
                .flatMapObservable(Observable::fromIterable);
    }

    // getLocalNode

    public Single<com.google.android.gms.wearable.Node> getLocalNode() {
        return getLocalNodeInternal();
    }

    private Single<com.google.android.gms.wearable.Node> getLocalNodeInternal() {
        return Single.create(new NodeGetLocalSingle(context));
    }

}

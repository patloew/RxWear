package com.patloew.rxwear;

import android.content.Context;

import com.google.android.gms.wearable.CapabilityInfo;
import com.google.android.gms.wearable.Wearable;

import java.util.Map;

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
class CapabilityGetAllSingle extends BaseSingle<Map<String, CapabilityInfo>> {

    final int nodeFilter;

    CapabilityGetAllSingle(Context context, int nodeFilter) {
        super(context);
        this.nodeFilter = nodeFilter;
    }

    @Override
    void onSubscribe(SingleEmitter<Map<String, CapabilityInfo>> mapSingleEmitter) {
        setupWearTask(
                Wearable.getCapabilityClient(context).getAllCapabilities(nodeFilter),
                SingleResultCallBack.get(mapSingleEmitter)
        );
    }
}
